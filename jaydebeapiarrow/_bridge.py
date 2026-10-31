# Late-bound hooks shared across package modules.
#
# jaydebeapiarrow/__init__.py wires these after JPype initialization; other
# modules (cursor, lib.arrow_utils) consume them.  Keeping the state here,
# rather than injecting a copy into each consumer's namespace, means there
# is exactly one source of truth for every reader.

# Java SQLException -> Python DatabaseError translator, set by
# _prepare_jpype() at import time.  None means JPype is not wired up yet.
_handle_sql_exception = None

# The extension's JDBCUtils class when it had to be loaded through a
# URLClassLoader (externally started JVM without the extension jar on the
# classpath); None means "resolve from the JVM classpath as usual".
# Set by _resolve_jdbc_utils() during connect().
_jdbc_utils_class = None
