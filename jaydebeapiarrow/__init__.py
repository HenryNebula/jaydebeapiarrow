#-*- coding: utf-8 -*-

# Copyright 2010-2015 Bastian Bowe
#
# This file is part of JayDeBeApi.
# JayDeBeApi is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
# 
# JayDeBeApi is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public
# License along with JayDeBeApi.  If not, see
# <http://www.gnu.org/licenses/>.
#
# Modified by HenryNebula (2023):
# 1. Remove py2 & Jython support
# 2. Enforce typing for Decimal and temporal types

import importlib.metadata
import re

__version__ = importlib.metadata.version("JayDeBeApiArrow")
_ver_match = re.match(r"^(\d+)\.(\d+)\.(\d+)", __version__)
__version_info__ = tuple(int(x) for x in _ver_match.groups()) if _ver_match else (0, 0, 0)

import datetime
from decimal import Decimal
import glob
import os
import threading
import time
import sys
import warnings

from jaydebeapiarrow.lib.arrow_utils import \
    convert_jdbc_rs_to_arrow_iterator, \
    read_rows_from_arrow_iterator, \
    create_pyarrow_batches_from_list, \
    add_pyarrow_batches_to_statement, \
    fetch_next_batch


def _is_jvm_started():
    """Check if the JPype JVM is started.

    Defensive wrapper that prefers ``jpype.isJVMStarted()`` when available,
    falling back to the internal ``_core._JVM_started`` flag if the public
    attribute is missing (e.g. due to a broken/incomplete JPype install).

    Note: ``jpype.isJVMStarted()`` has NOT been removed from any released
    JPype version (confirmed present through v1.7.0).  The original upstream
    report (baztian/jaydebeapi#253) was likely caused by a faulty
    installation rather than an API removal.
    """
    import jpype
    if hasattr(jpype, 'isJVMStarted'):
        return jpype.isJVMStarted()
    # Fallback for broken/incomplete JPype installs where the attribute is missing
    return bool(getattr(getattr(jpype, '_core', None), '_JVM_started', False))


def set_debug(enabled=True):
    """Enable or disable debug logging from the Java bridge (JUL FINE level).

    This controls java.util.logging for the org.jaydebeapiarrow.extension
    package. By default only INFO and above is printed; calling
    ``set_debug(True)`` enables column mapping, parameter binding, and
    other diagnostic messages.

    Must be called *after* the JVM has been started (i.e. after
    ``connect()``). Calling before the JVM starts is a no-op.

    Args:
        enabled: True to enable debug logging, False to disable.
    """
    if not _is_jvm_started():
        return
    Level = jpype.JClass("java.util.logging.Level")
    target_level = Level.FINE if enabled else Level.INFO
    logger = jpype.JClass("java.util.logging.Logger").getLogger(
        "org.jaydebeapiarrow.extension"
    )
    logger.setLevel(target_level)
    for handler in jpype.JClass("java.util.logging.Logger").getLogger("").getHandlers():
        handler.setLevel(target_level)


def reraise(tp, value, tb=None):
    if value is None:
        value = tp()
    else:
        value = tp(value)
    if tb:
        raise value.with_traceback(tb)
    raise value


# Mapping from java.sql.Types attribute name to attribute value
_jdbc_name_to_const = None

# Mapping from java.sql.Types attribute constant value to it's attribute name
_jdbc_const_to_name = None

_jdbc_connect = None

_handle_sql_exception = None

old_jpype = False

# Flag and lock to prevent race condition when multiple threads call
# connect() simultaneously — without this, two threads can both see
# isJVMStarted() return False and both attempt to start the JVM,
# causing a crash.  The lock is only held briefly to check/set the
# flag; startJVM() runs _outside_ the lock to avoid potential
# deadlocks if JPype spawns threads during initialisation.
_jvm_startup_lock = threading.Lock()
_jvm_starting = False

def _build_java_exception_message(exc):
    """Build a clean error message from a Java exception, walking the cause
    chain to surface wrapped exceptions. Avoids the duplicated class-name
    artefact that JPype 1.7.0+ produces via ``str(exc)``."""
    parts = []
    current = exc
    depth = 0
    while current is not None and depth < 20:
        cls_name = current.getClass().getName()
        msg = str(current.getMessage()) if current.getMessage() else ""
        parts.append(f"{cls_name}: {msg}" if msg else cls_name)
        current = current.getCause()
        depth += 1
    return "\n  Caused by: ".join(parts)
_jvm_started_pid = None

def _handle_sql_exception_jpype():
    import jpype
    SQLException = jpype.java.sql.SQLException
    exc_info = sys.exc_info()
    exc_val = exc_info[1]

    # Check the exception and its cause chain for SQLException.
    # The Arrow JDBC library wraps driver SQLExceptions in
    # JdbcConsumerException, so we must walk the chain to find
    # the original SQL error (e.g., divide-by-zero during fetch).
    db_err = False
    current = exc_val
    while current is not None:
        if old_jpype:
            clazz = current.__javaclass__
            if issubclass(clazz, SQLException):
                db_err = True
                break
        else:
            if isinstance(current, SQLException):
                db_err = True
                break
        try:
            current = current.getCause()
        except AttributeError:
            break

    if db_err:
        exc_type = DatabaseError
    else:
        exc_type = InterfaceError

    message = _build_java_exception_message(exc_val)
    reraise(exc_type, message, exc_info[2])

def _dynamic_load_driver(jclassname, jars):
    """Load a JDBC driver from JARs after JVM start using the DriverShim pattern.

    Java's DriverManager refuses to use drivers not loaded by the system
    classloader.  This function works around that restriction by creating a
    URLClassLoader for the new JARs, instantiating the driver through it, and
    registering a ``DriverShim`` proxy (loaded on the system classloader) with
    DriverManager.

    Args:
        jclassname: Fully-qualified Java class name of the JDBC driver.
        jars: List of JAR file paths to load the driver from.

    Returns:
        The URLClassLoader used to load the driver.
    """
    import jpype

    # Build URLClassLoader with the new JARs, parented to system classloader
    urls = [jpype.java.io.File(j).toURI().toURL() for j in jars]
    url_cl = jpype.java.net.URLClassLoader(
        urls, jpype.java.lang.ClassLoader.getSystemClassLoader()
    )

    # Load driver class from custom classloader and instantiate
    driver_cls = url_cl.loadClass(jclassname)
    driver = driver_cls.getDeclaredConstructor().newInstance()

    # Create a DriverShim proxy and register it with DriverManager.
    # The shim is a Python-implemented java.sql.Driver that delegates
    # every call to the real driver loaded via URLClassLoader.
    # DriverManager accepts the shim because it is loaded by the system CL.

    @jpype.JImplements("java.sql.Driver")
    class DriverShim:
        def __init__(self, _driver):
            self._driver = _driver
        @jpype.JOverride
        def connect(self, u, info):
            return self._driver.connect(u, info)
        @jpype.JOverride
        def acceptsURL(self, u):
            return self._driver.acceptsURL(u)
        @jpype.JOverride
        def getPropertyInfo(self, u, info):
            return self._driver.getPropertyInfo(u, info)
        @jpype.JOverride
        def getMajorVersion(self):
            return self._driver.getMajorVersion()
        @jpype.JOverride
        def getMinorVersion(self):
            return self._driver.getMinorVersion()
        @jpype.JOverride
        def jdbcCompliant(self):
            return self._driver.jdbcCompliant()
        @jpype.JOverride
        def getParentLogger(self):
            return self._driver.getParentLogger()

    jpype.java.sql.DriverManager.registerDriver(DriverShim(driver))

    # Update thread context classloader so the driver can find its own resources
    jpype.java.lang.Thread.currentThread().setContextClassLoader(url_cl)

    return url_cl


def _jdbc_connect_jpype(jclassname, url, driver_args, jars, libs, jvm_args=None, experimental=None):
    import jpype
    global _jvm_starting, _jvm_started_pid

    _experimental = experimental or {}

    if _jvm_started_pid is not None and _jvm_started_pid != os.getpid():
        if not _experimental.get('dynamic_classpath'):
            raise InterfaceError(
                "Cannot use jaydebeapiarrow in a forked process. "
                "The JVM was started in the parent process (PID %d) but this is "
                "PID %d. JPype does not support fork after JVM start. "
                "Move the connect() call after the fork, or use a "
                "post-fork-spawn worker model (e.g. gunicorn --preload with "
                "lazy connections)." % (_jvm_started_pid, os.getpid())
            )

    # Brief lock: decide who starts the JVM (if needed).
    with _jvm_startup_lock:
        if _is_jvm_started():
            should_start = False
        elif _jvm_starting:
            # Another thread is already starting the JVM; wait for it.
            should_start = False
        else:
            _jvm_starting = True
            should_start = True

    if should_start:
        try:
            class_path = []
            if jars:
                class_path.extend(jars)
            class_path.extend(_get_classpath())
            class_path.extend(_get_arrow_jar_paths())
            class_path = list(set(class_path))

            args = []

            if libs:
                # path to shared libraries
                libs_path = os.path.pathsep.join(libs)
                args.append('-Djava.library.path=%s' % libs_path)

            # Known issue: some JDBC drivers (notably IBM Db2) use the JVM's
            # default charset for string conversion.  When the default is not
            # UTF-8, non-ASCII characters (German umlauts, CJK, emoji) cause
            # CharConversionException during result-set traversal.  Users who
            # encounter this should pass jvm_args=['-Dfile.encoding=UTF-8']
            # when calling connect().
            # TODO: document this encoding requirement in user-facing docs
            # and consider exposing a dedicated encoding parameter in connect().

            # Add-opens for Apache Arrow on Java 9+
            args.append('--add-opens=java.base/java.nio=ALL-UNNAMED')
            # Drill's javassist needs reflective access to ClassLoader.defineClass
            args.append('--add-opens=java.base/java.lang=ALL-UNNAMED')
            # User-supplied extra JVM arguments (e.g. logging suppression)
            args.extend(jvm_args or [])

            # jvm_path = ('/usr/lib/jvm/java-6-openjdk'
            #             '/jre/lib/i386/client/libjvm.so')
            jvm_path = jpype.getDefaultJVMPath()
            global old_jpype
            if hasattr(jpype, '__version__'):
                try:
                    ver_match = re.match(r'\d+\.\d+', jpype.__version__)
                    if ver_match:
                        jpype_ver = float(ver_match.group(0))
                        if jpype_ver < 0.7:
                            old_jpype = True
                except ValueError:
                    pass
            if old_jpype:
                jpype.startJVM(jvm_path, *args,
                               classpath=class_path)
            else:
                jpype.startJVM(jvm_path, *args, ignoreUnrecognized=True,
                               convertStrings=True,
                               classpath=class_path)
            _jvm_started_pid = os.getpid()
        finally:
            with _jvm_startup_lock:
                _jvm_starting = False
    elif not _is_jvm_started():
        # Another thread is starting the JVM; spin-wait until ready.
        waited = 0
        while not _is_jvm_started():
            if not _jvm_starting:
                # Startup thread failed; bail out so the caller sees the
                # original exception (or retries on the next connect()).
                break
            time.sleep(0.05)
            waited += 0.05
            if waited > 120:
                raise RuntimeError("Timed out waiting for JVM to start")
    if not jpype.java.lang.Thread.isAttached():
        jpype.java.lang.Thread.attach()
        jpype.java.lang.Thread.currentThread().setContextClassLoader(jpype.java.lang.ClassLoader.getSystemClassLoader())
    try:
        import pyarrow.jvm
    except ImportError as e:
        raise RuntimeError(f"Failed to import pyarrow.jvm ({e}). Looks like JVM is not started. Thisis required for jaydebeapiarrow to work.")

    # register driver for DriverManager
    if _experimental.get('dynamic_classpath') and jars and _is_jvm_started():
        _dynamic_load_driver(jclassname, jars)
    else:
        jpype.JClass(jclassname)

    if isinstance(driver_args, dict):
        Properties = jpype.java.util.Properties
        info = Properties()
        for k, v in driver_args.items():
            info.setProperty(k, v)
        dargs = [ info ]
    else:
        dargs = driver_args
    return jpype.java.sql.DriverManager.getConnection(url, *dargs)

def _get_classpath():
    """Extract CLASSPATH from system environment as JPype doesn't seem
    to respect that variable.
    """
    try:
        orig_cp = os.environ['CLASSPATH']
    except KeyError:
        return []
    expanded_cp = []
    for i in orig_cp.split(os.path.pathsep):
        expanded_cp.extend(_jar_glob(i))
    return _deduplicate_jars(expanded_cp)

def _deduplicate_jars(jars):
    """Keep only the newest JAR when duplicates (same basename) exist."""
    import logging
    # First pass: deduplicate by real path (same file found by different glob
    # patterns, e.g. dir/*.jar and dir/**/*.jar).
    by_realpath = {}
    for jar in jars:
        rp = os.path.realpath(jar)
        if rp not in by_realpath:
            by_realpath[rp] = jar
    # Second pass: deduplicate by basename, keeping newest, warn on conflicts.
    seen = {}
    for jar in by_realpath.values():
        name = os.path.basename(jar)
        if name not in seen:
            seen[name] = jar
        else:
            try:
                if os.path.getmtime(jar) > os.path.getmtime(seen[name]):
                    logging.getLogger(__name__).warning(
                        "Duplicate JAR %s: keeping %s (newer) over %s",
                        name, jar, seen[name])
                    seen[name] = jar
                else:
                    logging.getLogger(__name__).warning(
                        "Duplicate JAR %s: keeping %s (newer) over %s",
                        name, seen[name], jar)
            except OSError:
                pass
    return list(seen.values())

def _jar_glob(item):
    if item.endswith('*'):
        jars = []
        for p in ['', '/**/']:
            jars.extend(glob.glob('%s' % str(item).rstrip("*") + p + "*.[jJ][aA][rR]", recursive=True))
        return jars
    else:
        return [item]

def _prepare_jpype():
    global _jdbc_connect
    _jdbc_connect = _jdbc_connect_jpype
    global _handle_sql_exception
    _handle_sql_exception = _handle_sql_exception_jpype
    from jaydebeapiarrow.lib import arrow_utils
    arrow_utils._handle_sql_exception = _handle_sql_exception_jpype

_prepare_jpype()


def _get_arrow_jar_paths():
    search_path = os.path.join(os.path.dirname(__file__), "./lib/arrow-jdbc-extension*")
    arrow_jars = list(_jar_glob(search_path))
    assert len(arrow_jars) > 0, f"Can not find arrow-jdbc JAR file at {search_path}"
    return arrow_jars


apilevel = '2.0'
threadsafety = 1
paramstyle = 'qmark'

class DBAPITypeObject(object):
    _mappings = {}
    def __init__(self, group_name, *values):
        """Construct new DB-API 2.0 type object.
        values: Attribute names of java.sql.Types constants"""
        self.values = values
        self.group_name = group_name
        for type_name in values:
            if type_name in DBAPITypeObject._mappings:
                raise ValueError("Non unique mapping for type '%s'" % type_name)
            DBAPITypeObject._mappings[type_name] = self
    def __eq__(self, other):
        if isinstance(other, DBAPITypeObject):
            return self.group_name == other.group_name
        if _jdbc_const_to_name is None:
            return False
        try:
            name = _jdbc_const_to_name.get(other)
        except (KeyError, TypeError):
            return False
        return name in self.values
    def __hash__(self):
        return hash(self.group_name)
    def __ne__(self, other):
        return not self.__eq__(other)
    def __repr__(self):
        return 'DBAPITypeObject(%s)' % ", ".join([repr(i) for i in self.values])
    @classmethod
    def _map_jdbc_type_to_dbapi(cls, jdbc_type_const):
        global _jdbc_const_to_name
        if _jdbc_const_to_name is None:
            import jpype
            if not _is_jvm_started():
                return None
            try:
                Types = jpype.java.sql.Types
                _jdbc_const_to_name = {}
                for field in Types.class_.getFields():
                    modifiers = field.getModifiers()
                    if jpype.java.lang.reflect.Modifier.isStatic(modifiers) and \
                       jpype.java.lang.reflect.Modifier.isPublic(modifiers):
                        try:
                            value = int(field.get(None))
                            _jdbc_const_to_name[value] = field.getName()
                        except (TypeError, ValueError):
                            continue
            except Exception:
                _jdbc_const_to_name = {}

        try:
            type_name = _jdbc_const_to_name[jdbc_type_const]
        except (KeyError, TypeError):
            warnings.warn("Unknown JDBC type with constant value %s. "
                          "Using None as a default type_code." % str(jdbc_type_const))
            return None
        try:
            return cls._mappings[type_name]
        except KeyError:
            warnings.warn("No type mapping for JDBC type '%s' (constant value %d). "
                          "Using None as a default type_code." % (type_name, jdbc_type_const))
            return None


STRING = DBAPITypeObject('STRING', 'CHAR', 'NCHAR', 'NVARCHAR', 'VARCHAR', 'OTHER')

TEXT = DBAPITypeObject('TEXT', 'CLOB', 'LONGVARCHAR', 'LONGNVARCHAR', 'NCLOB', 'SQLXML')

BINARY = DBAPITypeObject('BINARY', 'BINARY', 'BLOB', 'LONGVARBINARY', 'VARBINARY')

NUMBER = DBAPITypeObject('NUMBER','BOOLEAN', 'BIGINT', 'BIT', 'INTEGER', 'SMALLINT',
                         'TINYINT')

FLOAT = DBAPITypeObject('FLOAT', 'FLOAT', 'REAL', 'DOUBLE')

DECIMAL = DBAPITypeObject('DECIMAL', 'DECIMAL', 'NUMERIC')

DATE = DBAPITypeObject('DATE', 'DATE')

TIME = DBAPITypeObject('TIME', 'TIME')

DATETIME = DBAPITypeObject('TIMESTAMP', 'TIMESTAMP', 'TIMESTAMP_WITH_TIMEZONE', 'TIME_WITH_TIMEZONE')

ROWID = DBAPITypeObject('ROWID', 'ROWID')

ARRAY = DBAPITypeObject('ARRAY', 'ARRAY')

# DB-API 2.0 Module Interface Exceptions
class Error(Exception):
    pass

class Warning(Exception):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

class InternalError(DatabaseError):
    pass

class OperationalError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    pass

class DataError(DatabaseError):
    pass

class NotSupportedError(DatabaseError):
    pass

# DB-API 2.0 Type Objects and Constructors

def Binary(x):
    """Construct an object capable of holding a binary (long) string value."""
    if isinstance(x, str):
        return x.encode('utf-8')
    return bytes(x)

Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime

# Date = datetime.date

def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])

def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])

# DB-API 2.0 Module Interface connect constructor
def connect(jclassname, url, driver_args=None, jars=None, libs=None, jvm_args=None, experimental=None):
    """Open a connection to a database using a JDBC driver and return
    a Connection instance.

    jclassname: Full qualified Java class name of the JDBC driver.
    url: Database url as required by the JDBC driver.
    driver_args: Dictionary or sequence of arguments to be passed to
           the Java DriverManager.getConnection method. Usually
           sequence of username and password for the db. Alternatively
           a dictionary of connection arguments (where `user` and
           `password` would probably be included). See
           http://docs.oracle.com/javase/7/docs/api/java/sql/DriverManager.html
           for more details
    jars: Jar filename or sequence of filenames for the JDBC driver
    libs: Dll/so filenames or sequence of dlls/sos used as shared
          library by the JDBC driver
    jvm_args: Optional list of extra JVM arguments passed to startJVM().
          Only takes effect on the first connect() call (when the JVM
          is started). Ignored on subsequent calls.
    experimental: Optional dict of experimental feature flags.
          Supported keys:
            dynamic_classpath (bool): If True, allow loading JDBC drivers
              from JARs after the JVM has already been started, using a
              DriverShim proxy.  This also bypasses the fork-after-JVM-start
              guard, making it suitable for gunicorn --preload workers.
    """
    if not isinstance(url, str):
        raise ProgrammingError(
            "The 'url' parameter must be a JDBC connection string, "
            "not %s. If you meant to pass connection credentials, "
            "use the 'driver_args' parameter. "
            "Usage: connect(jclassname, url, driver_args=None, jars=None, libs=None)"
            % type(url).__name__
        )
    if isinstance(driver_args, str):
        driver_args = [ driver_args ]
    if not driver_args:
       driver_args = []
    if jars:
        if isinstance(jars, str):
            jars = [ jars ]
    else:
        jars = []
    if libs:
        if isinstance(libs, str):
            libs = [ libs ]
    else:
        libs = []
    if experimental is None:
        experimental = {}
    jconn = _jdbc_connect(jclassname, url, driver_args, jars, libs, jvm_args=jvm_args, experimental=experimental)
    return Connection(jconn, jclassname)

# DB-API 2.0 Connection Object
class Connection(object):

    Error = Error
    Warning = Warning
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    InternalError = InternalError
    OperationalError = OperationalError
    ProgrammingError = ProgrammingError
    IntegrityError = IntegrityError
    DataError = DataError
    NotSupportedError = NotSupportedError

    def __init__(self, jconn, jclassname=None):
        self.jconn = jconn
        self._jclassname = jclassname
        self._closed = False
        self._stringify_dates = False
        if self._jclassname and ("sqlite" in self._jclassname.lower()):
             self._stringify_dates = True

    def close(self):
        if self._closed:
            return
        self.jconn.close()
        self._closed = True

    def commit(self):
        if self.jconn.getAutoCommit():
            return
        try:
            self.jconn.commit()
        except:
            _handle_sql_exception()

    def rollback(self):
        if self.jconn.getAutoCommit():
            return
        try:
            self.jconn.rollback()
        except:
            _handle_sql_exception()

    def cursor(self):
        return Cursor(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

# DB-API 2.0 Cursor Object
def _decimal_string_column_indexes(meta):
    """Return 0-based indexes of DECIMAL/NUMERIC columns in a JDBC
    ResultSetMetaData. The Java layer transfers such columns as UTF-8 when
    their values cannot fit an Arrow decimal vector (> 76 digits) or when the
    driver reports no precision (unbounded numerics, e.g. Postgres NUMERIC);
    the cursor rebuilds exact Decimals from those strings."""
    import jpype
    types = jpype.java.sql.Types
    decimal_consts = (types.DECIMAL, types.NUMERIC)
    indexes = []
    for col in range(1, meta.getColumnCount() + 1):
        if int(meta.getColumnType(col)) in decimal_consts:
            indexes.append(col - 1)
    return tuple(indexes)


def _decimal_field_metadata(jdbc_type_name, precision, scale):
    """Arrow field metadata marking a utf8 column as a DECIMAL/NUMERIC that
    exceeded Arrow's decimal256 capacity (or had no declared precision), so
    downstream consumers can recover its intended type."""
    return {
        "jdbc_type": str(jdbc_type_name),
        "jdbc_precision": str(int(precision)),
        "jdbc_scale": str(int(scale)),
    }


def _decimal_fallback_indexes(schema, decimal_meta):
    """Map schema field index -> (jdbc_type_name, precision, scale) for
    fields that arrived as utf8 but were DECIMAL/NUMERIC in JDBC metadata."""
    import pyarrow as pa
    fallback = {}
    for index, jdbc_type_name, precision, scale in decimal_meta:
        if index < len(schema):
            dtype = schema.field(index).type
            if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
                fallback[index] = (jdbc_type_name, precision, scale)
    return fallback


def _infer_decimal_widths(strings):
    """Scan decimal strings and return (max_total_digits, max_scale) — the
    smallest decimal shape that holds every value."""
    max_digits = 0
    max_scale = 0
    for value in strings:
        if value is None:
            continue
        if "E" in value or "e" in value:
            # java.math.BigDecimal.toString() uses scientific notation for
            # extreme scales (e.g. "1E-7")
            t = Decimal(value).as_tuple()
            digits = len(t.digits)
            scale = max(0, -t.exponent)
        else:
            int_part, _, frac = value.partition(".")
            digits = len(int_part.lstrip("-")) + len(frac)
            scale = len(frac)
        if digits > max_digits:
            max_digits = digits
        if scale > max_scale:
            max_scale = scale
    return max_digits, max_scale


class Cursor(object):

    _rs = None
    _description = None
    _iter = None
    _buffer = None
    _decimal_cols = None
    _decimal_info = None

    def __init__(self, connection):
        self._connection = connection
        self._buffer = []
        self._prep = None
        self.rowcount = -1
        self.lastrowid = None

    @property
    def connection(self):
        return self._connection

    @property
    def description(self):
        if self._description:
            return self._description
        m = self._meta
        if m:
            count = m.getColumnCount()
            self._description = []
            for col in range(1, count + 1):
                size = m.getColumnDisplaySize(col)
                jdbc_type = m.getColumnType(col)
                if jdbc_type == 0:
                    # PEP-0249: SQL NULL values are represented by the
                    # Python None singleton
                    dbapi_type = None
                else:
                    dbapi_type = DBAPITypeObject._map_jdbc_type_to_dbapi(jdbc_type)
                col_desc = ( m.getColumnLabel(col),
                             dbapi_type,
                             size,
                             size,
                             m.getPrecision(col),
                             m.getScale(col),
                             m.isNullable(col),
                             )
                self._description.append(col_desc)
            return self._description

#   optional callproc(self, procname, *parameters) unsupported

    def close(self):
        self._close_last()
        self._connection = None

    def _close_last(self):
        """Close the resultset and reset collected meta data.
        """
        if self._iter:
            try:
                self._iter.close()
            except:
                pass
        self._iter = None
        self._buffer = []
        if self._rs:
            self._rs.close()
        self._rs = None
        if self._prep:
            self._prep.close()
        self._prep = None
        self._meta = None
        self._description = None
        self._decimal_cols = None
        self._decimal_info = None

    # def _set_stmt_parms(self, prep_stmt, parameters):
    #     for i in range(len(parameters)):
    #         # print (i, parameters[i], type(parameters[i]))
    #         prep_stmt.setObject(i + 1, parameters[i])

    def _stringify_params(self, params, is_batch):
        if not params:
            return params
        
        def _to_str(x):
            if isinstance(x, (datetime.date, datetime.time, datetime.datetime)):
                return str(x)
            return x
            
        if is_batch:
            # params is a sequence of sequences
            return [[_to_str(p) for p in row] for row in params]
        else:
            # params is a sequence
            return [_to_str(p) for p in params]

    def _set_stmt_parms(self, statement, parameters, is_batch=False):
        if self._connection._stringify_dates:
             parameters = self._stringify_params(parameters, is_batch)
        try:
            batches = create_pyarrow_batches_from_list(parameters)
            add_pyarrow_batches_to_statement(batches, statement, is_batch=is_batch)
        except Exception:
            self._set_stmt_parms_fallback(statement, parameters, is_batch)

    def _set_stmt_parms_fallback(self, statement, parameters, is_batch=False):
        """Fallback using standard JDBC setObject() for drivers that don't
        support Arrow-stream parameter binding (e.g. Trino)."""
        import jpype

        Types_NULL = jpype.java.sql.Types.NULL

        def _to_java(p):
            """Convert Python types to Java SQL types for setObject()."""
            if p is None:
                return p  # JPype passes Python None as Java null; setObject(i, null) is valid JDBC
            if isinstance(p, bool):
                return p
            if isinstance(p, (bytes, bytearray)):
                return jpype.JArray(jpype.JByte)(p)
            if isinstance(p, datetime.datetime):
                return jpype.JClass("java.sql.Timestamp").valueOf(
                    p.strftime("%Y-%m-%d %H:%M:%S.%f"))
            if isinstance(p, datetime.date):
                return jpype.JClass("java.sql.Date").valueOf(p.isoformat())
            if isinstance(p, datetime.time):
                return jpype.JClass("java.sql.Time").valueOf(
                    p.strftime("%H:%M:%S"))
            if isinstance(p, Decimal):
                return jpype.JClass("java.math.BigDecimal")(str(p))
            if isinstance(p, list):
                return _list_to_java_array(p)
            return p

        def _list_to_java_array(lst):
            """Convert a Python list to a Java array for setArray() binding."""
            if not lst:
                return jpype.JArray(jpype.JString)(0)

            # Infer element type from first non-None element
            sample = None
            for item in lst:
                if item is not None:
                    sample = item
                    break

            if sample is None:
                return jpype.JArray(jpype.JString)([None] * len(lst))
            if isinstance(sample, bool):
                return jpype.JArray(jpype.JBoolean)(lst)
            if isinstance(sample, int):
                return jpype.JArray(jpype.JInt)(lst)
            if isinstance(sample, float):
                return jpype.JArray(jpype.JDouble)(lst)
            if isinstance(sample, str):
                return jpype.JArray(jpype.JString)(lst)
            if isinstance(sample, Decimal):
                return jpype.JArray(jpype.JString)([str(x) for x in lst])

            return jpype.JArray(jpype.JString)([str(x) for x in lst])

        def _bind_param(stmt, idx, p):
            """Bind a parameter, using setArray for list values."""
            if isinstance(p, list):
                java_arr = _list_to_java_array(p)
                try:
                    conn = stmt.getConnection()
                    sql_type = _infer_sql_type_name(java_arr)
                    sql_array = conn.createArrayOf(sql_type, java_arr)
                    stmt.setArray(idx, sql_array)
                except Exception:
                    stmt.setObject(idx, java_arr)
            else:
                stmt.setObject(idx, _to_java(p))

        def _infer_sql_type_name(java_arr):
            """Map a Java array class to its SQL type name for createArrayOf()."""
            import jpype
            cls = java_arr.getClass().getComponentType()
            if cls == jpype.JBoolean:
                return "BOOLEAN"
            if cls == jpype.JInt:
                return "INTEGER"
            if cls == jpype.JDouble:
                return "DOUBLE"
            if cls == jpype.JString:
                return "VARCHAR"
            return "VARCHAR"

        if is_batch:
            for row in parameters:
                for i, p in enumerate(row):
                    if p is None:
                        statement.setNull(i + 1, Types_NULL)
                    elif isinstance(p, list):
                        _bind_param(statement, i + 1, p)
                    else:
                        statement.setObject(i + 1, _to_java(p))
                statement.addBatch()
        else:
            for i, p in enumerate(parameters):
                if p is None:
                    statement.setNull(i + 1, Types_NULL)
                elif isinstance(p, list):
                    _bind_param(statement, i + 1, p)
                else:
                    statement.setObject(i + 1, _to_java(p))

    def execute(self, operation, parameters=None):
        if self._connection._closed:
            raise Error()
        if not parameters:
            parameters = ()
        self._close_last()
        self.lastrowid = None
        try:
            self._prep = self._connection.jconn.prepareStatement(operation, 1)
        except Exception:
            try:
                self._prep = self._connection.jconn.prepareStatement(operation)
            except:
                _handle_sql_exception()
        self._set_stmt_parms(self._prep, parameters, is_batch=False)
        try:
            is_rs = self._prep.execute()
        except:
            _handle_sql_exception()
        if is_rs:
            self._rs = self._prep.getResultSet()
            self._meta = self._rs.getMetaData()
            self.rowcount = -1
        else:
            self.rowcount = self._prep.getUpdateCount()
            try:
                gk_rs = self._prep.getGeneratedKeys()
                if gk_rs.next():
                    self.lastrowid = int(gk_rs.getLong(1))
                    if gk_rs.wasNull():
                        self.lastrowid = None
            except Exception:
                pass
        # self._prep.getWarnings() ???

    def executemany(self, operation, seq_of_parameters):
        self._close_last()
        self.lastrowid = None
        try:
            self._prep = self._connection.jconn.prepareStatement(operation, 1)
        except Exception:
            try:
                self._prep = self._connection.jconn.prepareStatement(operation)
            except:
                _handle_sql_exception()
        self._set_stmt_parms(self._prep, seq_of_parameters, is_batch=True)
        try:
            update_counts = self._prep.executeBatch()
        except:
            _handle_sql_exception()
        # self._prep.getWarnings() ???
        self.rowcount = sum(update_counts)
        self._close_last()

    def _get_iter(self):
        if self._iter:
            return self._iter
        if not self._rs:
            raise Error()
        # Use a reasonable batch size.
        # For small reads (fetchone), this might be overhead, but it's safe.
        # For large reads (fetchall), this is efficient.
        # Using arraysize or a default.
        batch_size = max(self.arraysize, 1024)
        self._iter = convert_jdbc_rs_to_arrow_iterator(self._rs, batch_size=batch_size)
        return self._iter

    def _jdbc_decimal_info(self):
        """Cached per-column info for DECIMAL/NUMERIC columns of the current
        result set: tuple of (0-based index, jdbc type name, precision,
        scale), used both for restoring Decimals on row fetches and for
        typing/provenance in the Arrow-native fetch paths."""
        if self._decimal_info is None:
            rows = []
            if self._meta is not None:
                import jpype
                types = jpype.java.sql.Types
                decimal_consts = (int(types.DECIMAL), int(types.NUMERIC))
                m = self._meta
                for col in range(1, m.getColumnCount() + 1):
                    if int(m.getColumnType(col)) in decimal_consts:
                        rows.append((col - 1,
                                     str(m.getColumnTypeName(col)),
                                     int(m.getPrecision(col)),
                                     int(m.getScale(col))))
            self._decimal_info = tuple(rows)
        return self._decimal_info

    def _restore_decimal_columns(self, rows):
        """Rebuild Decimal objects for DECIMAL/NUMERIC columns transferred as
        UTF-8 strings (values beyond decimal256's 76-digit capacity, or
        unbounded numeric columns where JDBC reports no precision). Columns
        that stayed Arrow decimals already hold Decimal objects and are
        skipped by the isinstance check."""
        if not rows:
            return rows
        if self._decimal_cols is None:
            self._decimal_cols = (_decimal_string_column_indexes(self._meta)
                                   if self._meta is not None else ())
        if not self._decimal_cols:
            return rows
        first = rows[0]
        # Arrow column types are fixed for the whole result set, so the first
        # row decides whether this batch needs restoring at all — queries
        # whose decimal columns stayed Arrow decimals skip the rebuild
        # entirely. A None value is ambiguous (nullable column of either
        # kind) and conservatively takes the restoring path below.
        needs_restore = False
        for i in self._decimal_cols:
            if i >= len(first):
                continue
            if isinstance(first[i], str) or first[i] is None:
                needs_restore = True
                break
        if not needs_restore:
            return rows
        # Transpose to columns, convert the decimal strings in bulk, and
        # transpose back — measurably faster than a per-row rebuild loop.
        lists = list(zip(*rows))
        for i in self._decimal_cols:
            if i < len(lists):
                lists[i] = [Decimal(v) if isinstance(v, str) else v
                            for v in lists[i]]
        return [tuple(t) for t in zip(*lists)]

    def fetchone(self):
        if not self._rs:
            return None

        if self._buffer:
            return self._buffer.pop(0)

        it = self._get_iter()
        rows = self._restore_decimal_columns(fetch_next_batch(it))
        if rows:
            self._buffer.extend(rows)
            return self._buffer.pop(0)

        # Iterator exhausted and closed by fetch_next_batch
        self._iter = None
        return None

    def fetchmany(self, size=None):
        if not self._rs:
            return []

        if size is None:
            size = self.arraysize

        assert size > 0, f"Fetchmany expects positive size other than size={size}."

        result = []
        while len(result) < size:
            if self._buffer:
                needed = size - len(result)
                take = self._buffer[:needed]
                self._buffer = self._buffer[needed:]
                result.extend(take)
            else:
                it = self._get_iter()
                rows = self._restore_decimal_columns(fetch_next_batch(it))
                if not rows:
                    # Iterator exhausted and closed by fetch_next_batch
                    self._iter = None
                    break
                self._buffer.extend(rows)

        return result

    def fetchall(self):
        if not self._rs:
            return []

        result = []
        if self._buffer:
            result.extend(self._buffer)
            self._buffer = []

        it = self._get_iter()

        # We can implement a more efficient fetchall if we want to avoid python loops for buffering,
        # but reusing fetch_next_batch is simpler.
        while True:
            rows = self._restore_decimal_columns(fetch_next_batch(it))
            if not rows:
                break
            result.extend(rows)

        # Iterator exhausted and closed by fetch_next_batch
        self._iter = None
        return result

    # optional nextset() unsupported

    arraysize = 1

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def fetch_arrow_batches(self):
        """
        Fetch results as Arrow RecordBatches (zero-copy, native Arrow format).

        This is the most efficient way to retrieve data for Arrow-native workflows.
        Returns a generator that yields pyarrow.RecordBatch objects.

        Example:
            for batch in cursor.fetch_arrow_batches():
                # Process Arrow batch directly (zero-copy)
                df = batch.to_pandas()  # If you need pandas
                # OR: process with any Arrow-compatible library

        Returns:
            Generator[pyarrow.RecordBatch]: Arrow RecordBatches from the query result

        Note:
            This is significantly faster (3-4x) than fetchall() for Arrow-native workflows
            because it avoids converting to Python tuples.

            DECIMAL/NUMERIC columns whose values cannot fit Arrow's
            decimal256 type (declared precision > 76, or unbounded numerics
            where JDBC reports no precision) are transferred as utf8
            strings to preserve exact values. Those fields carry Arrow
            metadata (jdbc_type, jdbc_precision, jdbc_scale) so consumers
            can recover the intended type and cast deliberately, e.g.
            ``batch.column(i).cast(pa.decimal256(76, scale))``.
        """
        if not self._rs:
            raise Error("No result set")

        import pyarrow as pa
        it = self._get_iter()

        try:
            while it.hasNext():
                root = it.next()
                try:
                    batch = pa.jvm.record_batch(root)
                    fallback = _decimal_fallback_indexes(
                        batch.schema, self._jdbc_decimal_info())
                    if fallback:
                        fields = []
                        for i, field in enumerate(batch.schema):
                            if i in fallback:
                                jdbc_type, precision, scale = fallback[i]
                                field = field.with_metadata(_decimal_field_metadata(
                                    jdbc_type, precision, scale))
                            fields.append(field)
                        batch = pa.RecordBatch.from_arrays(
                            batch.columns, schema=pa.schema(fields))
                    yield batch
                finally:
                    root.clear()
        finally:
            # Close iterator to release Arrow allocator and JDBC resources
            self._iter = None
            try:
                it.close()
            except Exception:
                pass

    def fetch_arrow_table(self):
        """
        Fetch all results as a single pyarrow.Table.

        This is a convenience method that collects all RecordBatches into one Table.

        Example:
            table = cursor.fetch_arrow_table()
            df = table.to_pandas()  # Efficient conversion to pandas

        Returns:
            pyarrow.Table: Complete result set as an Arrow Table
        """
        import pyarrow as pa
        batches = list(self.fetch_arrow_batches())
        if not batches:
            # Return empty table with inferred schema
            return pa.Table.from_arrays([])
        table = pa.Table.from_batches(batches)
        # Fallback decimal columns that fit decimal256 become real decimal
        # columns again; the shape is inferred from the data so the type
        # matches the actual values.
        fallback = _decimal_fallback_indexes(
            table.schema, self._jdbc_decimal_info())
        for index in fallback:
            column = table.column(index)
            max_digits, max_scale = _infer_decimal_widths(
                value for chunk in column.chunks for value in chunk.to_pylist())
            if max_digits > 76:
                # No Arrow decimal type holds it; keep the exact utf8 form.
                continue
            target = pa.decimal256(max(max_digits, 1), max_scale)
            try:
                casted = column.cast(target)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                continue
            jdbc_type, precision, scale = fallback[index]
            old_field = table.schema.field(index)
            field = pa.field(
                old_field.name, target, nullable=old_field.nullable,
                metadata=_decimal_field_metadata(jdbc_type, precision, scale))
            table = table.set_column(index, field, casted)
        return table

    def fetch_df(self):
        """
        Fetch all results as a pandas DataFrame (optimized Arrow path).

        This is more efficient than fetchall() + manual pandas conversion
        because it uses Arrow's optimized pandas conversion.

        Example:
            df = cursor.fetch_df()
            # Work with DataFrame directly

        Returns:
            pandas.DataFrame: Query result as a pandas DataFrame
        """
        try:
            import pandas  # noqa: F401
        except ImportError:
            raise ImportError(
                "fetch_df() requires pandas. "
                "Install it with: pip install jaydebeapiarrow[pandas]"
            )
        table = self.fetch_arrow_table()
        df = table.to_pandas()
        # Columns still utf8 (values beyond decimal256) are converted
        # value-by-value so every decimal column has the same object dtype
        # holding decimal.Decimal, whatever its precision.
        fallback = _decimal_fallback_indexes(
            table.schema, self._jdbc_decimal_info())
        for index in fallback:
            name = df.columns[index]
            df[name] = df[name].map(
                lambda v: Decimal(v) if isinstance(v, str) else v)
        return df

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

