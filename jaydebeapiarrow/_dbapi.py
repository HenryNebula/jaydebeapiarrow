# DB-API 2.0 module surface: level/threading/paramstyle globals, exception
# hierarchy, type objects and type constructors.  Re-exported by
# jaydebeapiarrow/__init__.py; importable as a leaf module (no dependencies
# on the JVM plumbing) so Connection/Cursor modules can use it freely.

import datetime
import time
import warnings

apilevel = '2.0'
threadsafety = 1
paramstyle = 'qmark'

# Mapping from java.sql.Types attribute constant value to it's attribute name
_jdbc_const_to_name = None


class DBAPITypeObject:
    _mappings = {}
    def __init__(self, group_name, *values):
        """Construct new DB-API 2.0 type object.
        values: Attribute names of java.sql.Types constants"""
        self.values = values
        self.group_name = group_name
        for type_name in values:
            if type_name in DBAPITypeObject._mappings:
                raise ValueError(f"Non unique mapping for type '{type_name}'")
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
        return 'DBAPITypeObject({})'.format(", ".join([repr(i) for i in self.values]))
    @classmethod
    def _map_jdbc_type_to_dbapi(cls, jdbc_type_const):
        global _jdbc_const_to_name
        if _jdbc_const_to_name is None:
            import jpype

            from jaydebeapiarrow import _is_jvm_started
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
            warnings.warn(
                f"Unknown JDBC type with constant value {jdbc_type_const}. "
                "Using None as a default type_code.", stacklevel=3)
            return None
        try:
            return cls._mappings[type_name]
        except KeyError:
            warnings.warn(
                f"No type mapping for JDBC type '{type_name}' "
                f"(constant value {jdbc_type_const}). "
                "Using None as a default type_code.", stacklevel=3)
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

DATETIME = DBAPITypeObject('TIMESTAMP', 'TIMESTAMP',
                           'TIMESTAMP_WITH_TIMEZONE', 'TIME_WITH_TIMEZONE')

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

def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])

def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])
