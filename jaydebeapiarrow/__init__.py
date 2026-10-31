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

import glob
import importlib.metadata
import os
import re
import sys
import threading
import time

import jaydebeapiarrow._bridge as _bridge
from jaydebeapiarrow._dbapi import (
    ARRAY,
    BINARY,
    DATE,
    DATETIME,
    DECIMAL,
    FLOAT,
    NUMBER,
    ROWID,
    STRING,
    TEXT,
    TIME,
    Binary,
    DatabaseError,
    DataError,
    Date,
    DateFromTicks,
    DBAPITypeObject,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
    Warning,
    apilevel,
    paramstyle,
    threadsafety,
)
from jaydebeapiarrow.cursor import (
    Cursor,
)
from jaydebeapiarrow.cursor import (
    _decimal_fallback_indexes as _decimal_fallback_indexes,
)
from jaydebeapiarrow.cursor import (
    _decimal_field_metadata as _decimal_field_metadata,
)
from jaydebeapiarrow.cursor import (
    _infer_decimal_widths as _infer_decimal_widths,
)
from jaydebeapiarrow.cursor import (
    _is_decimal_array_type_name as _is_decimal_array_type_name,
)

__version__ = importlib.metadata.version("JayDeBeApiArrow")
_ver_match = re.match(r"^(\d+)\.(\d+)\.(\d+)", __version__)
__version_info__ = tuple(int(x) for x in _ver_match.groups()) if _ver_match else (0, 0, 0)

# The DB-API 2.0 module interface plus this fork's public extensions.
# Implementation lives in _dbapi.py, cursor.py and here; this package's
# __init__ is the stable import surface.
__all__ = [
    'ARRAY', 'BINARY', 'DATE', 'DATETIME', 'DECIMAL', 'FLOAT', 'NUMBER',
    'ROWID', 'STRING', 'TEXT', 'TIME', 'Binary', 'Connection', 'Cursor',
    'DBAPITypeObject', 'DataError', 'DatabaseError', 'Date', 'DateFromTicks',
    'Error', 'IntegrityError', 'InterfaceError', 'InternalError',
    'NotSupportedError', 'OperationalError', 'ProgrammingError', 'Time',
    'TimeFromTicks', 'Timestamp', 'TimestampFromTicks', 'Warning',
    'apilevel', 'connect', 'paramstyle', 'set_debug', 'threadsafety',
]


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
    package. By default only WARNING and above is printed (see the packaged
    ``logging.properties``); calling ``set_debug(True)`` enables column
    mapping, parameter binding, and other diagnostic messages. Because the
    bundled SLF4J binding routes Arrow and JDBC-driver logs into JUL as
    well, this also surfaces their FINER detail.

    Must be called *after* the JVM has been started (i.e. after
    ``connect()``). Calling before the JVM starts is a no-op.

    Args:
        enabled: True to enable debug logging, False to disable.
    """
    import jpype
    if not _is_jvm_started():
        return
    Level = jpype.JClass("java.util.logging.Level")
    target_level = Level.FINE if enabled else Level.WARNING
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


_jdbc_connect = None


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
    if exc_val is not None and not hasattr(exc_val, 'getClass'):
        # Not a Java exception (a Python AttributeError/TypeError, or
        # Ctrl-C as KeyboardInterrupt, caught by the bare except around
        # the JDBC calls): re-raise unchanged with its original traceback
        # instead of letting the Java message builder mask it behind its
        # own AttributeError on getClass().
        raise

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

# Cache for drivers loaded through a URLClassLoader, keyed by
# (jclassname, jars).  The driver class lives in a child classloader, so
# jpype.JClass(jclassname) keeps raising TypeError on every connect() —
# without this cache each reconnect would build another URLClassLoader and
# register another DriverShim with DriverManager, leaking both for the life
# of the JVM (#134).  Guarded by _dynamic_driver_lock so concurrent
# connect() calls produce exactly one loader/shim per key.
_dynamic_driver_lock = threading.Lock()
_dynamic_driver_loaders = {}


def _dynamic_load_driver(jclassname, jars):
    """Load a JDBC driver from JARs after JVM start using the DriverShim pattern.

    Java's DriverManager refuses to use drivers not loaded by the system
    classloader.  This function works around that restriction by creating a
    URLClassLoader for the new JARs, instantiating the driver through it, and
    registering a ``DriverShim`` proxy (loaded on the system classloader) with
    DriverManager.

    Loaders are cached per (jclassname, jars): a repeat connect() with the
    same driver reuses the existing loader and registered shim instead of
    accumulating a new pair each call.

    Args:
        jclassname: Fully-qualified Java class name of the JDBC driver.
        jars: List of JAR file paths to load the driver from.

    Returns:
        The URLClassLoader used to load the driver.
    """
    import jpype

    key = (jclassname, tuple(sorted(jars)))
    with _dynamic_driver_lock:
        url_cl = _dynamic_driver_loaders.get(key)
        if url_cl is None:
            # Build URLClassLoader with the new JARs, parented to system
            # classloader
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
            # DriverManager accepts the shim because it is loaded by the
            # system CL.

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
            _dynamic_driver_loaders[key] = url_cl

    # Update thread context classloader so the driver can find its own resources
    jpype.java.lang.Thread.currentThread().setContextClassLoader(url_cl)

    return url_cl


# Cache for the extension's JDBCUtils class (and the loader that produced
# it) when it had to be loaded through a URLClassLoader (externally started
# JVM without the extension jar on the classpath).  Guarded by
# _jdbc_utils_lock so concurrent connect() calls produce exactly one class
# identity — mixing classes from two loaders would raise ClassCastException
# on the fetch path.  The loader is kept so the java.nio module-open check
# keeps probing the class's own module on every connect(), not just the
# first.
_jdbc_utils_lock = threading.Lock()
_jdbc_utils_dynamic_class = None
_jdbc_utils_dynamic_loader = None


def _java_base_opens_nio(loader=None):
    """True if java.base opens java.nio to the module holding JDBCUtils.

    Arrow's MemoryUtil must reflect on java.nio.Buffer.address, which is only
    permitted when the JVM was started with
    ``--add-opens=java.base/java.nio=ALL-UNNAMED``.  Best-effort: on JVMs
    without module introspection (or any unexpected error) assume open and
    let the original error surface at fetch time.
    """
    import jpype
    try:
        for_name = jpype.JClass("java.lang.Class").forName
        java_base = for_name("java.nio.Buffer").getModule()
        if loader is not None:
            target = for_name(
                "org.jaydebeapiarrow.extension.JDBCUtils", True, loader)
        else:
            target = for_name("org.jaydebeapiarrow.extension.JDBCUtils")
        return bool(java_base.isOpen("java.nio", target.getModule()))
    except Exception:
        return True


def _resolve_jdbc_utils():
    """Make the extension's JDBCUtils class resolvable for the fetch path.

    When connect() starts the JVM itself the extension jar is on the
    classpath and the plain lookup below succeeds.  When the JVM was
    started externally without it, load the packaged extension jar through
    a URLClassLoader and inject the class into lib.arrow_utils so fetches
    work with zero classpath configuration.  Unlike drivers — which Java's
    DriverManager must discover via a DriverShim registered on the system
    classloader — JDBCUtils needs no Java-side shim: the Python/Java
    boundary is static method calls plus raw C Data pointers, so handing
    arrow_utils the class object is sufficient.
    """
    global _jdbc_utils_dynamic_class, _jdbc_utils_dynamic_loader
    import jpype

    loader = None
    try:
        jpype.JClass("org.jaydebeapiarrow.extension.JDBCUtils")
    except TypeError:
        with _jdbc_utils_lock:
            if _jdbc_utils_dynamic_class is None:
                jars = _get_arrow_jar_paths()
                urls = [jpype.java.io.File(j).toURI().toURL() for j in jars]
                loader = jpype.java.net.URLClassLoader(
                    urls,
                    jpype.java.lang.ClassLoader.getSystemClassLoader())
                _jdbc_utils_dynamic_class = jpype.JClass(
                    loader.loadClass(
                        "org.jaydebeapiarrow.extension.JDBCUtils"))
                _jdbc_utils_dynamic_loader = loader
            else:
                loader = _jdbc_utils_dynamic_loader
            _bridge._jdbc_utils_class = _jdbc_utils_dynamic_class

    # Arrow's MemoryUtil cannot reflect on java.nio.Buffer.address unless
    # java.base was opened at JVM start; that permission cannot be granted
    # afterwards, so fail fast with actionable guidance instead of letting
    # the first fetch die with a confusing InaccessibleObjectException.
    if not _java_base_opens_nio(loader):
        raise InterfaceError(
            "The JVM was started without opening java.nio to Arrow, which "
            "cannot be granted after JVM start. Restart it with "
            "--add-opens=java.base/java.nio=ALL-UNNAMED (also recommended: "
            "--add-opens=java.base/java.lang=ALL-UNNAMED).")


_DEFAULT_LOGGING_PROPERTIES = os.path.join(
    os.path.dirname(__file__), 'logging.properties')

_JUL_CONFIG_PROPERTIES = (
    'java.util.logging.config.file',
    'java.util.logging.config.class',
)


def _default_jul_config_arg(jvm_args):
    """JVM args pointing java.util.logging at the packaged
    logging.properties, or [] when the user already configured JUL
    (config.file or config.class) via jvm_args."""
    for arg in jvm_args or []:
        for prop in _JUL_CONFIG_PROPERTIES:
            if arg.startswith(f'-D{prop}='):
                return []
    return [f'-Djava.util.logging.config.file={_DEFAULT_LOGGING_PROPERTIES}']


def _jdbc_connect_jpype(jclassname, url, driver_args, jars, libs, jvm_args=None, experimental=None):
    import jpype
    global _jvm_starting, _jvm_started_pid

    _experimental = experimental or {}

    if _jvm_started_pid is not None and _jvm_started_pid != os.getpid():
        if not _experimental.get('dynamic_classpath'):
            raise InterfaceError(
                "Cannot use jaydebeapiarrow in a forked process. "
                f"The JVM was started in the parent process (PID {_jvm_started_pid}) "
                f"but this is PID {os.getpid()}. JPype does not support fork after "
                "JVM start. Move the connect() call after the fork, or use a "
                "post-fork-spawn worker model (e.g. gunicorn --preload with "
                "lazy connections)."
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
            # Order-preserving dedup: jars, then CLASSPATH env, then the
            # packaged extension jar.  SLF4J discovers providers on the
            # system classloader in classpath order, so a user-supplied
            # binding on CLASSPATH deterministically precedes ours.
            class_path = list(dict.fromkeys(class_path))

            args = []

            if libs:
                # path to shared libraries
                libs_path = os.path.pathsep.join(libs)
                args.append(f'-Djava.library.path={libs_path}')

            # Known issue: some JDBC drivers (notably IBM Db2) use the JVM's
            # default charset for string conversion.  When the default is not
            # UTF-8, non-ASCII characters cause CharConversionException during
            # result-set traversal — see the "Non-ASCII characters garbled"
            # section of the usage docs for the -Dfile.encoding=UTF-8
            # workaround.

            # Add-opens for Apache Arrow on Java 9+
            args.append('--add-opens=java.base/java.nio=ALL-UNNAMED')
            # Drill's javassist needs reflective access to ClassLoader.defineClass
            args.append('--add-opens=java.base/java.lang=ALL-UNNAMED')
            # Default JUL config (WARNING console) unless the user brings
            # their own java.util.logging configuration.  Injected before
            # user args: the JVM lets a later duplicate -D win, so an
            # explicit user value overrides even without the skip check.
            args.extend(_default_jul_config_arg(jvm_args))
            # User-supplied extra JVM arguments
            args.extend(jvm_args or [])

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
        import pyarrow.jvm  # noqa: F401 -- probe: verifies the JVM bridge is usable
    except ImportError as e:
        raise RuntimeError(
            f"Failed to import pyarrow.jvm ({e}). Looks like JVM is not "
            "started. This is required for jaydebeapiarrow to work.") from e

    # Resolve the extension's JDBCUtils (dynamically loading the packaged
    # extension jar when the JVM was started without it) and fail fast when
    # Arrow's required --add-opens flags are missing.
    _resolve_jdbc_utils()

    # register driver for DriverManager
    if _experimental.get('dynamic_classpath') and jars and _is_jvm_started():
        _dynamic_load_driver(jclassname, jars)
    else:
        try:
            jpype.JClass(jclassname)
        except TypeError as exc:
            if jars:
                # The JVM was started externally without this driver on its
                # classpath; load it from `jars` through a URLClassLoader so
                # jars= works plug-and-play on a pre-started JVM (#128).
                _dynamic_load_driver(jclassname, jars)
            else:
                raise TypeError(
                    f"Class {jclassname} is not found. It is not on the JVM classpath "
                    "and no jars were given to load it from. If you start "
                    "the JVM yourself, pass the driver JAR via jars= or put "
                    "it on the classpath before startJVM()."
                ) from exc

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
            pattern = f"{str(item).rstrip('*')}{p}*.[jJ][aA][rR]"
            jars.extend(glob.glob(pattern, recursive=True))
        return jars
    else:
        return [item]

def _prepare_jpype():
    global _jdbc_connect
    _jdbc_connect = _jdbc_connect_jpype
    _bridge._handle_sql_exception = _handle_sql_exception_jpype

_prepare_jpype()


def _get_arrow_jar_paths():
    search_path = os.path.join(os.path.dirname(__file__), "./lib/arrow-jdbc-extension*")
    arrow_jars = list(_jar_glob(search_path))
    assert len(arrow_jars) > 0, f"Can not find arrow-jdbc JAR file at {search_path}"
    return arrow_jars


# DB-API 2.0 Module Interface connect constructor
def connect(jclassname, url, driver_args=None, jars=None, libs=None,
            jvm_args=None, experimental=None):
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
          is started). Ignored on subsequent calls. When connect() starts
          the JVM, a default ``java.util.logging.config.file`` pointing at
          the packaged logging.properties (WARNING console level) is
          injected unless jvm_args sets ``java.util.logging.config.file``
          or ``.class`` (see the Logging section of the docs).
    experimental: Optional dict of experimental feature flags.
          Supported keys:
            dynamic_classpath (bool): If True, always load JDBC drivers
              from JARs through a URLClassLoader DriverShim, even when the
              class is already on the classpath.  This also bypasses the
              fork-after-JVM-start guard, making it suitable for
              gunicorn --preload workers.

    Note: when the JVM is already running (e.g. started manually via
    jpype.startJVM()), jars are loaded dynamically through a URLClassLoader,
    and the packaged Arrow extension is loaded the same way when missing
    from the classpath.  An externally started JVM must still pass
    --add-opens=java.base/java.nio=ALL-UNNAMED (plus
    --add-opens=java.base/java.lang=ALL-UNNAMED for some drivers), since
    module opens cannot be granted after JVM start; connect() fails fast
    with that guidance when the flag is missing.
    """
    if not isinstance(url, str):
        raise ProgrammingError(
            "The 'url' parameter must be a JDBC connection string, "
            f"not {type(url).__name__}. If you meant to pass connection credentials, "
            "use the 'driver_args' parameter. "
            "Usage: connect(jclassname, url, driver_args=None, jars=None, libs=None)"
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
    jconn = _jdbc_connect(jclassname, url, driver_args, jars, libs,
                          jvm_args=jvm_args, experimental=experimental)
    return Connection(jconn, jclassname)

# DB-API 2.0 Connection Object
class Connection:

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
        except Exception:
            _bridge._handle_sql_exception()

    def rollback(self):
        if self.jconn.getAutoCommit():
            return
        try:
            self.jconn.rollback()
        except Exception:
            _bridge._handle_sql_exception()

    def cursor(self):
        return Cursor(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

