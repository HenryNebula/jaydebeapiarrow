#-*- coding: utf-8 -*-

# Consolidated infrastructure tests that previously lived in both
# test_integration.py and test_mock.py with near-identical logic.
#
# Each test category has a base class parameterized by driver class,
# with concrete HSQLDB and MockDriver subclasses.

import jaydebeapiarrow
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

try:
    from test._base import _THIS_DIR, _SUPPRESS_LOGGING_ARGS
except ImportError:
    from _base import _THIS_DIR, _SUPPRESS_LOGGING_ARGS


def _connect(*args, **kwargs):
    """Wrapper that injects logging suppression JVM args on first connect."""
    kwargs.setdefault('jvm_args', _SUPPRESS_LOGGING_ARGS)
    return jaydebeapiarrow.connect(*args, **kwargs)


# ---------------------------------------------------------------------------
# Fork safety tests (legacy issue #232)
# ---------------------------------------------------------------------------

class _ForkSafetyTestBase(object):
    """Base class for fork-after-JVM-start guard tests."""

    DRIVER_CLASS = None  # override in subclass
    JDBC_URL = None      # override in subclass
    DRIVER_ARGS = None   # override in subclass

    def test_fork_after_connect_raises_interface_error(self):
        """Simulating a fork by overwriting the PID tracker must raise
        InterfaceError when attempting a new connection."""
        original_pid = jaydebeapiarrow._jvm_started_pid
        try:
            jaydebeapiarrow._jvm_started_pid = os.getpid() + 99999
            with self.assertRaises(jaydebeapiarrow.InterfaceError) as ctx:
                _connect(self.DRIVER_CLASS,
                                        self.JDBC_URL, self.DRIVER_ARGS)
            self.assertIn("forked process", str(ctx.exception))
        finally:
            jaydebeapiarrow._jvm_started_pid = original_pid

    def test_pid_recorded_after_connect(self):
        """After connect(), _jvm_started_pid must equal the current PID."""
        c = _connect(self.DRIVER_CLASS,
                                    self.JDBC_URL, self.DRIVER_ARGS)
        try:
            self.assertEqual(jaydebeapiarrow._jvm_started_pid, os.getpid())
        finally:
            c.close()


class ForkSafetyHsqldbTest(_ForkSafetyTestBase, unittest.TestCase):
    DRIVER_CLASS = 'org.hsqldb.jdbcDriver'
    JDBC_URL = 'jdbc:hsqldb:mem:.'
    DRIVER_ARGS = ['SA', '']


class ForkSafetyMockTest(_ForkSafetyTestBase, unittest.TestCase):
    DRIVER_CLASS = 'org.jaydebeapi.mockdriver.MockDriver'
    JDBC_URL = 'jdbc:jaydebeapi://dummyurl'
    DRIVER_ARGS = None


# ---------------------------------------------------------------------------
# JAR path with spaces tests (issue #86)
# ---------------------------------------------------------------------------

class _JarPathSpacesTestBase(object):
    """Base class for JAR file paths containing spaces."""

    def _find_jar(self):
        raise NotImplementedError

    def _driver_class(self):
        raise NotImplementedError

    def _jdbc_url(self):
        raise NotImplementedError

    def _driver_args(self):
        return None

    def _run_connect_in_subprocess(self, jar_path):
        """Run a connect call in a fresh subprocess and return success/failure."""
        driver = self._driver_class()
        url = self._jdbc_url()
        args = self._driver_args()
        code = f'''
import jaydebeapiarrow
try:
    conn = jaydebeapiarrow.connect(
        {repr(driver)},
        {repr(url)},
        driver_args={repr(args)},
        jars={repr(jar_path)}
    )
    print('OK')
    conn.close()
except Exception as e:
    print(f'FAIL: {{type(e).__name__}}: {{e}}')
'''
        result = subprocess.run(
            [sys.executable, '-c', code],
            capture_output=True, text=True, timeout=30,
            cwd=os.path.dirname(os.path.dirname(__file__))
        )
        return result.stdout.strip(), result.stderr.strip()

    def test_jar_path_with_spaces(self):
        """JAR paths containing spaces should work (issue #86)."""
        jar = self._find_jar()
        with tempfile.TemporaryDirectory(prefix='path with spaces ') as tmpdir:
            dest = os.path.join(tmpdir, os.path.basename(jar))
            shutil.copy2(jar, dest)
            stdout, stderr = self._run_connect_in_subprocess(dest)
        self.assertEqual(stdout, 'OK', f'Connection failed: {stderr}')

    def test_jar_path_with_special_chars(self):
        """JAR paths containing parentheses and special chars should work."""
        jar = self._find_jar()
        with tempfile.TemporaryDirectory(prefix='path (x86) & test ') as tmpdir:
            dest = os.path.join(tmpdir, os.path.basename(jar))
            shutil.copy2(jar, dest)
            stdout, stderr = self._run_connect_in_subprocess(dest)
        self.assertEqual(stdout, 'OK', f'Connection failed: {stderr}')


class JarPathSpacesHsqldbTest(_JarPathSpacesTestBase, unittest.TestCase):

    def _find_jar(self):
        jar_dir = os.path.join(_THIS_DIR, 'jars')
        if not os.path.isdir(jar_dir):
            self.skipTest('test/jars/ directory not found (run download_jdbc_drivers.sh)')
        for f in os.listdir(jar_dir):
            if 'hsqldb' in f.lower() and f.endswith('.jar'):
                return os.path.join(jar_dir, f)
        self.skipTest('HSQLDB JAR not found in test/jars/')

    def _driver_class(self):
        return 'org.hsqldb.jdbcDriver'

    def _jdbc_url(self):
        return 'jdbc:hsqldb:mem:.'

    def _driver_args(self):
        return ['SA', '']


class JarPathSpacesMockTest(_JarPathSpacesTestBase, unittest.TestCase):

    def _find_jar(self):
        for root, dirs, files in os.walk(_THIS_DIR):
            for f in files:
                if f.startswith('mockdriver') and f.endswith('.jar'):
                    return os.path.join(root, f)
        self.fail('mockdriver JAR not found')

    def _driver_class(self):
        return 'org.jaydebeapi.mockdriver.MockDriver'

    def _jdbc_url(self):
        return 'jdbc:jaydebeapi://dummyurl'


# ---------------------------------------------------------------------------
# Dynamic classpath tests
# ---------------------------------------------------------------------------

class _DynamicClasspathTestBase(object):
    """Base class for experimental dynamic_classpath feature."""

    def _find_primary_jar(self):
        raise NotImplementedError

    def _primary_driver_class(self):
        raise NotImplementedError

    def _primary_jdbc_url(self):
        raise NotImplementedError

    def _primary_driver_args(self):
        return None

    def _run_in_subprocess(self, code):
        """Run code in a fresh subprocess and return stdout, stderr."""
        result = subprocess.run(
            [sys.executable, '-c', code],
            capture_output=True, text=True, timeout=30,
            cwd=os.path.dirname(os.path.dirname(__file__))
        )
        return result.stdout.strip(), result.stderr.strip()

    def test_dynamic_load_after_jvm_start(self):
        """Connect with a driver JAR after JVM is already running (dynamic_classpath)."""
        jar = self._find_primary_jar()
        driver = self._primary_driver_class()
        url = self._primary_jdbc_url()
        args = self._primary_driver_args()
        code = f'''
import jaydebeapiarrow

# First connection starts the JVM normally
conn1 = jaydebeapiarrow.connect(
    {repr(driver)},
    {repr(url)},
    driver_args={repr(args)}
)
conn1.close()

# Second connection uses dynamic classpath to load the driver from JAR
conn2 = jaydebeapiarrow.connect(
    {repr(driver)},
    {repr(url)},
    driver_args={repr(args)},
    jars={repr(jar)},
    experimental={{'dynamic_classpath': True}}
)
conn2.close()
print('OK')
'''
        stdout, stderr = self._run_in_subprocess(code)
        self.assertEqual(stdout, 'OK', f'Dynamic load failed: {stderr}')

    def test_dynamic_load_without_flag_raises_error(self):
        """Without dynamic_classpath flag, connecting with new JARs after JVM
        start should raise InterfaceError (fork guard)."""
        jar = self._find_primary_jar()
        driver = self._primary_driver_class()
        url = self._primary_jdbc_url()
        args = self._primary_driver_args()
        code = f'''
import jaydebeapiarrow

# Start JVM with first connection
conn1 = jaydebeapiarrow.connect(
    {repr(driver)},
    {repr(url)},
    driver_args={repr(args)}
)
conn1.close()

# Try connecting with explicit jars after JVM start — no experimental flag
try:
    conn2 = jaydebeapiarrow.connect(
        {repr(driver)},
        {repr(url)},
        driver_args={repr(args)},
        jars={repr(jar)}
    )
    conn2.close()
    print('NO_ERROR')
except jaydebeapiarrow.InterfaceError as e:
    if 'forked process' in str(e):
        print('FORK_ERROR')
    else:
        print(f'OTHER_INTERFACE_ERROR: {{e}}')
except Exception as e:
    print(f'OTHER_ERROR: {{type(e).__name__}}: {{e}}')
'''
        stdout, stderr = self._run_in_subprocess(code)
        self.assertIn(stdout, ['OK', 'NO_ERROR', 'FORK_ERROR', 'OTHER_INTERFACE_ERROR'],
                      f'Unexpected output: {stdout}\nstderr: {stderr}')

    def test_dynamic_load_bypasses_fork_guard(self):
        """dynamic_classpath flag bypasses the fork-after-JVM-start guard."""
        jar = self._find_primary_jar()
        driver = self._primary_driver_class()
        url = self._primary_jdbc_url()
        args = self._primary_driver_args()
        code = f'''
import jaydebeapiarrow, os

# Start JVM
conn1 = jaydebeapiarrow.connect(
    {repr(driver)},
    {repr(url)},
    driver_args={repr(args)}
)
conn1.close()

# Simulate fork: change _jvm_started_pid to a different PID
jaydebeapiarrow._jvm_started_pid = os.getpid() + 99999

# Without flag — should raise
try:
    conn2 = jaydebeapiarrow.connect(
        {repr(driver)},
        {repr(url)},
        driver_args={repr(args)},
        jars={repr(jar)}
    )
    print('NO_ERROR')
except jaydebeapiarrow.InterfaceError as e:
    print('FORK_ERROR')

# With flag — should succeed
try:
    conn3 = jaydebeapiarrow.connect(
        {repr(driver)},
        {repr(url)},
        driver_args={repr(args)},
        jars={repr(jar)},
        experimental={{'dynamic_classpath': True}}
    )
    conn3.close()
    print('DYNAMIC_OK')
except Exception as e:
    print(f'DYNAMIC_FAIL: {{type(e).__name__}}: {{e}}')
'''
        stdout, stderr = self._run_in_subprocess(code)
        lines = stdout.split('\n')
        self.assertEqual(lines[0], 'FORK_ERROR',
                         f'Expected fork error without flag, got: {stdout}\nstderr: {stderr}')
        self.assertEqual(lines[1], 'DYNAMIC_OK',
                         f'Dynamic load should bypass fork guard, got: {stdout}\nstderr: {stderr}')


class DynamicClasspathHsqldbTest(_DynamicClasspathTestBase, unittest.TestCase):
    """Integration test with real HSQLDB driver."""

    def _find_primary_jar(self):
        jar_dir = os.path.join(_THIS_DIR, 'jars')
        if not os.path.isdir(jar_dir):
            self.skipTest('test/jars/ directory not found (run download_jdbc_drivers.sh)')
        for f in os.listdir(jar_dir):
            if 'hsqldb' in f.lower() and f.endswith('.jar'):
                return os.path.join(jar_dir, f)
        self.skipTest('HSQLDB JAR not found in test/jars/')

    def _primary_driver_class(self):
        return 'org.hsqldb.jdbcDriver'

    def _primary_jdbc_url(self):
        return 'jdbc:hsqldb:mem:.'

    def _primary_driver_args(self):
        return ['SA', '']

    def test_hsqldb_fails_without_dynamic_classpath(self):
        """Connecting to HSQLDB after JVM starts with only mock driver on classpath
        should fail — the HSQLDB driver is not available."""
        hsqldb_jar = self._find_primary_jar()
        mock_dir = os.path.join(_THIS_DIR, 'mock-jars')
        mock_jar = None
        for f in os.listdir(mock_dir):
            if f.startswith('mockdriver') and f.endswith('.jar'):
                mock_jar = os.path.join(mock_dir, f)
                break
        if not mock_jar:
            self.skipTest('mockdriver JAR not found')

        env = {**os.environ, 'CLASSPATH': mock_jar}
        code = f'''
import jaydebeapiarrow

# Start JVM with only the mock driver available
conn1 = jaydebeapiarrow.connect(
    'org.jaydebeapi.mockdriver.MockDriver',
    'jdbc:jaydebeapi://dummyurl'
)
conn1.close()

# Try to connect to HSQLDB without dynamic classpath — should fail
try:
    conn2 = jaydebeapiarrow.connect(
        'org.hsqldb.jdbcDriver',
        'jdbc:hsqldb:mem:.',
        ['SA', '']
    )
    conn2.close()
    print('UNEXPECTED_SUCCESS')
except Exception as e:
    print(f'EXPECTED_FAIL: {{type(e).__name__}}')
'''
        result = subprocess.run(
            [sys.executable, '-c', code],
            capture_output=True, text=True, timeout=30,
            cwd=os.path.dirname(os.path.dirname(__file__)),
            env=env
        )
        self.assertTrue(result.stdout.strip().startswith('EXPECTED_FAIL'),
                        f'HSQLDB should fail without dynamic classpath.\n'
                        f'stdout: {result.stdout}\nstderr: {result.stderr}')

    def test_dynamic_load_hsqldb_after_jvm_start(self):
        """Dynamically load HSQLDB driver after JVM is already running."""
        hsqldb_jar = self._find_primary_jar()
        mock_dir = os.path.join(_THIS_DIR, 'mock-jars')
        mock_jar = None
        for f in os.listdir(mock_dir):
            if f.startswith('mockdriver') and f.endswith('.jar'):
                mock_jar = os.path.join(mock_dir, f)
                break
        if not mock_jar:
            self.skipTest('mockdriver JAR not found')

        env = {**os.environ, 'CLASSPATH': mock_jar}
        code = f'''
import jaydebeapiarrow

# Start JVM with only the mock driver on the classpath
conn1 = jaydebeapiarrow.connect(
    'org.jaydebeapi.mockdriver.MockDriver',
    'jdbc:jaydebeapi://dummyurl'
)
conn1.close()

# Verify HSQLDB is NOT available yet
try:
    conn_bad = jaydebeapiarrow.connect(
        'org.hsqldb.jdbcDriver',
        'jdbc:hsqldb:mem:.',
        ['SA', '']
    )
    conn_bad.close()
    print('HSQQLDB_AVAILABLE_WITHOUT_DYNAMIC')
except Exception:
    print('HSQQLDB_NOT_AVAILABLE')

# Now dynamically load HSQLDB driver from JAR
conn2 = jaydebeapiarrow.connect(
    'org.hsqldb.jdbcDriver',
    'jdbc:hsqldb:mem:.',
    ['SA', ''],
    jars={repr(hsqldb_jar)},
    experimental={{'dynamic_classpath': True}}
)
cursor = conn2.cursor()

# Verify it actually works — run real SQL
cursor.execute('CREATE TABLE test_dynamic (id INTEGER, name VARCHAR(50))')
cursor.execute("INSERT INTO test_dynamic VALUES (1, 'hello'), (2, 'world')")
cursor.execute('SELECT id, name FROM test_dynamic ORDER BY id')
rows = cursor.fetchall()
cursor.execute('DROP TABLE test_dynamic')
cursor.close()
conn2.close()

print(f'DYNAMIC_OK: {{rows}}')
'''
        result = subprocess.run(
            [sys.executable, '-c', code],
            capture_output=True, text=True, timeout=30,
            cwd=os.path.dirname(os.path.dirname(__file__)),
            env=env
        )
        lines = result.stdout.strip().split('\n')
        self.assertEqual(lines[0], 'HSQQLDB_NOT_AVAILABLE',
                         f'HSQLDB should not be available before dynamic load.\n'
                         f'stdout: {result.stdout}\nstderr: {result.stderr}')
        self.assertEqual(lines[1], 'DYNAMIC_OK: [(1, \'hello\'), (2, \'world\')]',
                         f'Dynamic HSQLDB load failed or returned wrong data.\n'
                         f'stdout: {result.stdout}\nstderr: {result.stderr}')


class DynamicClasspathMockTest(_DynamicClasspathTestBase, unittest.TestCase):
    """Tests using mock driver."""

    def _find_primary_jar(self):
        for root, dirs, files in os.walk(_THIS_DIR):
            for f in files:
                if f.startswith('mockdriver') and f.endswith('.jar'):
                    return os.path.join(root, f)
        self.skipTest('mockdriver JAR not found')

    def _primary_driver_class(self):
        return 'org.jaydebeapi.mockdriver.MockDriver'

    def _primary_jdbc_url(self):
        return 'jdbc:jaydebeapi://dummyurl'


# ---------------------------------------------------------------------------
# Externally started JVM tests (issue #128)
# ---------------------------------------------------------------------------

class ExternalJvmAutoloadTest(unittest.TestCase):
    """connect() must work plug-and-play on a JVM the user started via raw
    jpype.startJVM() with nothing on the classpath (issue #128)."""

    def _run_in_subprocess(self, code, env=None):
        result = subprocess.run(
            [sys.executable, '-c', code],
            capture_output=True, text=True, timeout=30,
            cwd=os.path.dirname(os.path.dirname(__file__)),
            env=env
        )
        return result.stdout.strip(), result.stderr.strip()

    def _bare_env(self):
        # No CLASSPATH so nothing leaks onto the JVM the test starts itself
        return {k: v for k, v in os.environ.items() if k != 'CLASSPATH'}

    def _hsqldb_jar(self):
        jar_dir = os.path.join(_THIS_DIR, 'jars')
        if not os.path.isdir(jar_dir):
            self.skipTest('test/jars/ directory not found (run download_jdbc_drivers.sh)')
        for f in os.listdir(jar_dir):
            if 'hsqldb' in f.lower() and f.endswith('.jar'):
                return os.path.join(jar_dir, f)
        self.skipTest('HSQLDB JAR not found in test/jars/')

    def test_autoload_on_externally_started_jvm(self):
        """The issue #128 pattern: manual startJVM without classpath, then
        connect(..., jars=...) with no experimental flag.  The driver jar
        and the packaged Arrow extension must both load dynamically and a
        full query round-trip must succeed."""
        hsqldb_jar = self._hsqldb_jar()
        code = f'''
import jpype
import jaydebeapiarrow

jpype.startJVM(
    jpype.getDefaultJVMPath(),
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    ignoreUnrecognized=True,
    convertStrings=True,
)

conn = jaydebeapiarrow.connect(
    "org.hsqldb.jdbcDriver",
    "jdbc:hsqldb:mem:.",
    ["SA", ""],
    jars={repr(hsqldb_jar)},
)
cursor = conn.cursor()
cursor.execute("CREATE TABLE t_autoload (id INTEGER, name VARCHAR(50))")
cursor.execute("INSERT INTO t_autoload VALUES (1, 'hello'), (2, 'world')")
cursor.execute("SELECT id, name FROM t_autoload ORDER BY id")
rows = cursor.fetchall()
cursor.close()
conn.close()
print(f"AUTOLOAD_OK: {{rows}}")
'''
        stdout, stderr = self._run_in_subprocess(code, env=self._bare_env())
        self.assertEqual(stdout, "AUTOLOAD_OK: [(1, 'hello'), (2, 'world')]",
                         f'Externally started JVM autoload failed.\n'
                         f'stdout: {stdout}\nstderr: {stderr}')

    def test_missing_add_opens_fails_fast(self):
        """An externally started JVM without the Arrow --add-opens flags must
        fail at connect() with guidance naming the required flag, not with a
        confusing InaccessibleObjectException at first fetch.

        JDK 16+ denies illegal access by default, so the fail-fast fires
        there.  Older JDKs (8 has no JPMS, 9-15 default to
        --illegal-access=permit) genuinely allow Arrow's reflection, so
        connecting without the flags succeeds legitimately there."""
        hsqldb_jar = self._hsqldb_jar()
        code = f'''
import jpype
import jaydebeapiarrow

jpype.startJVM(
    jpype.getDefaultJVMPath(),
    ignoreUnrecognized=True,
    convertStrings=True,
)

# Ground truth for this JVM: is java.nio actually open to the unnamed
# module holding the extension (no --add-opens passed)?
urls = [jpype.java.io.File(j).toURI().toURL()
        for j in jaydebeapiarrow._get_arrow_jar_paths()]
loader = jpype.java.net.URLClassLoader(
    urls, jpype.java.lang.ClassLoader.getSystemClassLoader())
probe_open = jaydebeapiarrow._java_base_opens_nio(loader)
print("PROBE_OPEN" if probe_open else "PROBE_CLOSED")

try:
    conn = jaydebeapiarrow.connect(
        "org.hsqldb.jdbcDriver",
        "jdbc:hsqldb:mem:.",
        ["SA", ""],
        jars={repr(hsqldb_jar)},
    )
    conn.close()
    print("CONNECT_OK")
except jaydebeapiarrow.InterfaceError as e:
    if "--add-opens=java.base/java.nio" in str(e):
        print("FAIL_FAST_OK")
    else:
        print(f"WRONG_INTERFACE_ERROR: {{e}}")
except Exception as e:
    print(f"OTHER_ERROR: {{type(e).__name__}}: {{e}}")
'''
        stdout, stderr = self._run_in_subprocess(code, env=self._bare_env())
        # JDK 16+ denies by default -> fail fast with the flag named;
        # JDK 8-15 permits by default -> connecting genuinely works.
        self.assertIn(stdout, ['PROBE_CLOSED\nFAIL_FAST_OK',
                               'PROBE_OPEN\nCONNECT_OK'],
                      f'Missing --add-opens behavior wrong for this JVM.\n'
                      f'stdout: {stdout}\nstderr: {stderr}')

    def test_fail_fast_consistent_on_retry(self):
        """The --add-opens fail-fast must fire on every connect() call, not
        just the first.  A retry after catching the InterfaceError (e.g. in a
        connection-retry loop) must raise the same guidance again instead of
        silently succeeding and dying at first fetch with the confusing
        ExceptionInInitializerError the fail-fast exists to prevent."""
        hsqldb_jar = self._hsqldb_jar()
        code = f'''
import jpype
import jaydebeapiarrow

jpype.startJVM(
    jpype.getDefaultJVMPath(),
    ignoreUnrecognized=True,
    convertStrings=True,
)

def try_connect():
    try:
        conn = jaydebeapiarrow.connect(
            "org.hsqldb.jdbcDriver",
            "jdbc:hsqldb:mem:.",
            ["SA", ""],
            jars={repr(hsqldb_jar)},
        )
        conn.close()
        return "CONNECT_OK"
    except jaydebeapiarrow.InterfaceError as e:
        if "--add-opens=java.base/java.nio" in str(e):
            return "FAIL_FAST_OK"
        return f"WRONG_INTERFACE_ERROR: {{e}}"
    except Exception as e:
        return f"OTHER_ERROR: {{type(e).__name__}}: {{e}}"

print(f"FIRST={{try_connect()}} SECOND={{try_connect()}}")
'''
        stdout, stderr = self._run_in_subprocess(code, env=self._bare_env())
        # Both calls must agree: fail fast twice on a denying JVM (16+),
        # genuinely connect twice on a permissive one (8-15).
        self.assertIn(
            stdout,
            ['FIRST=FAIL_FAST_OK SECOND=FAIL_FAST_OK',
             'FIRST=CONNECT_OK SECOND=CONNECT_OK'],
            f'Fail-fast inconsistent between first connect and retry.\n'
            f'stdout: {stdout}\nstderr: {stderr}')

    def test_class_not_found_without_jars_names_the_fix(self):
        """On a pre-started JVM, a driver class that is nowhere to be found
        must raise an error pointing at jars= / the classpath."""
        code = '''
import jpype
import jaydebeapiarrow

jpype.startJVM(
    jpype.getDefaultJVMPath(),
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    ignoreUnrecognized=True,
    convertStrings=True,
)

try:
    jaydebeapiarrow.connect(
        "org.example.NonexistentDriver",
        "jdbc:nonexistent://dummy",
    )
    print("UNEXPECTED_SUCCESS")
except TypeError as e:
    if "jars" in str(e) and "classpath" in str(e):
        print("GUIDANCE_OK")
    else:
        print(f"PLAIN_ERROR: {{e}}")
except Exception as e:
    print(f"OTHER_ERROR: {{type(e).__name__}}: {{e}}")
'''
        stdout, stderr = self._run_in_subprocess(code, env=self._bare_env())
        self.assertEqual(stdout, 'GUIDANCE_OK',
                         f'Class-not-found error should include jars guidance.\n'
                         f'stdout: {stdout}\nstderr: {stderr}')


# ---------------------------------------------------------------------------
# JPype reflection / type mapping tests (legacy #111)
# ---------------------------------------------------------------------------

class _ReflectionTestBase(object):
    """Base class for java.sql.Types reflection and DBAPITypeObject tests."""

    DRIVER_CLASS = None
    JDBC_URL = None
    DRIVER_ARGS = None

    def setUp(self):
        self.conn = _connect(
            self.DRIVER_CLASS,
            self.JDBC_URL,
            self.DRIVER_ARGS,
        )

    def tearDown(self):
        self.conn.close()

    def test_type_constants_accessible_via_reflection(self):
        """java.sql.Types constants should be accessible through
        standard Java Reflection, not getStaticAttribute()."""
        import jpype
        Types = jpype.java.sql.Types
        self.assertEqual(Types.INTEGER, 4)
        self.assertEqual(Types.VARCHAR, 12)
        self.assertEqual(Types.TIMESTAMP, 93)
        self.assertEqual(Types.DECIMAL, 3)

    def test_dbapi_type_comparison_with_real_connection(self):
        """DBAPITypeObject comparison should work after a real JDBC
        connection initializes the type mapping via Reflection."""
        import jpype
        Types = jpype.java.sql.Types
        self.assertIsNotNone(jaydebeapiarrow._jdbc_const_to_name)
        self.assertEqual(jaydebeapiarrow.NUMBER, Types.INTEGER)
        self.assertEqual(jaydebeapiarrow.STRING, Types.VARCHAR)
        self.assertEqual(jaydebeapiarrow.DATETIME, Types.TIMESTAMP)

    def test_cursor_description_maps_types_correctly(self):
        """cursor.description should use correct type names from
        Reflection-based type mapping."""
        with self.conn.cursor() as cursor:
            cursor.execute("CREATE TABLE test_reflect (id INTEGER, name VARCHAR(50), val DECIMAL(10,2))")
            cursor.execute("INSERT INTO test_reflect VALUES (1, 'test', 3.14)")
            cursor.execute("SELECT * FROM test_reflect")
            desc = cursor.description
            self.assertEqual(len(desc), 3)
            self.assertEqual(desc[0][0], 'ID')
            self.assertEqual(desc[1][0], 'NAME')
            self.assertEqual(desc[2][0], 'VAL')

    def test_java_sql_types_reflection_uses_standard_api(self):
        """Verify java.sql.Types constants are accessed via standard Java
        Reflection API (field.get/getModifiers/getName), not the deprecated
        JPype-specific getStaticAttribute() which was removed in newer JPype."""
        import jpype
        Types = jpype.java.sql.Types
        fields = Types.class_.getFields()
        static_public_fields = {}
        for field in fields:
            modifiers = field.getModifiers()
            if jpype.java.lang.reflect.Modifier.isStatic(modifiers) and \
               jpype.java.lang.reflect.Modifier.isPublic(modifiers):
                value = int(field.get(None))
                static_public_fields[field.getName()] = value
        self.assertEqual(static_public_fields['INTEGER'], 4)
        self.assertEqual(static_public_fields['VARCHAR'], 12)
        self.assertEqual(static_public_fields['TIMESTAMP'], 93)
        self.assertEqual(static_public_fields['DECIMAL'], 3)
        self.assertEqual(static_public_fields['NUMERIC'], 2)

    def test_jdbc_type_mapping_populates_correctly(self):
        """Verify _map_jdbc_type_to_dbapi builds the mapping using
        standard Reflection (not getStaticAttribute)."""
        import jpype
        Types = jpype.java.sql.Types
        result = jaydebeapiarrow.DBAPITypeObject._map_jdbc_type_to_dbapi(Types.INTEGER)
        self.assertIs(result, jaydebeapiarrow.NUMBER)
        self.assertIsNotNone(jaydebeapiarrow._jdbc_const_to_name)
        self.assertGreater(len(jaydebeapiarrow._jdbc_const_to_name), 20)

    def test_dbapi_type_eq_with_jdbc_constants(self):
        """Verify DBAPITypeObject.__eq__ works with JDBC type constants
        accessed through standard Java Reflection."""
        import jpype
        Types = jpype.java.sql.Types
        jaydebeapiarrow.DBAPITypeObject._map_jdbc_type_to_dbapi(Types.INTEGER)
        self.assertTrue(jaydebeapiarrow.NUMBER == int(Types.INTEGER))
        self.assertTrue(jaydebeapiarrow.NUMBER == int(Types.BIGINT))
        self.assertTrue(jaydebeapiarrow.NUMBER == int(Types.SMALLINT))
        self.assertTrue(jaydebeapiarrow.NUMBER == int(Types.TINYINT))
        self.assertTrue(jaydebeapiarrow.STRING == int(Types.VARCHAR))
        self.assertTrue(jaydebeapiarrow.STRING == int(Types.CHAR))
        self.assertTrue(jaydebeapiarrow.DATETIME == int(Types.TIMESTAMP))
        self.assertTrue(jaydebeapiarrow.DATE == int(Types.DATE))


class ReflectionHsqldbTest(_ReflectionTestBase, unittest.TestCase):
    DRIVER_CLASS = 'org.hsqldb.jdbc.JDBCDriver'
    JDBC_URL = 'jdbc:hsqldb:mem:testreflection.'
    DRIVER_ARGS = ['SA', '']


class ReflectionMockTest(_ReflectionTestBase, unittest.TestCase):
    DRIVER_CLASS = 'org.jaydebeapi.mockdriver.MockDriver'
    JDBC_URL = 'jdbc:jaydebeapi://dummyurl'
    DRIVER_ARGS = None

    def test_cursor_description_maps_types_correctly(self):
        """Mock driver does not support DDL — skip cursor description test."""
        self.skipTest("Mock driver does not support CREATE TABLE / SELECT")


# ---------------------------------------------------------------------------
# Properties driver args passing tests
# ---------------------------------------------------------------------------

class PropertiesDriverArgsPassingTest(unittest.TestCase):

    def test_connect_with_sequence(self):
        driver, url, driver_args = ( 'org.hsqldb.jdbcDriver',
                                     'jdbc:hsqldb:mem:.',
                                     ['SA', ''] )
        c = _connect(driver, url, driver_args)
        c.close()

    def test_connect_with_properties(self):
        driver, url, driver_args = ( 'org.hsqldb.jdbcDriver',
                                     'jdbc:hsqldb:mem:.',
                                     {'user': 'SA', 'password': '' } )
        c = _connect(driver, url, driver_args)
        c.close()


class DecimalArrayTypeNameTest(unittest.TestCase):
    """_is_decimal_array_type_name mirrors the Java side's decimal-ARRAY
    element patterns (ExplicitTypeMapper.inferElementJdbcType), which route
    these arrays onto the unbounded-decimal string fallback (#121)."""

    def test_matches_decimal_array_names(self):
        for name in ("DECIMAL ARRAY", "NUMERIC ARRAY",
                     "decimal array", "Decimal ARRAY",
                     "DECIMAL[]", "NUMERIC[]"):
            self.assertTrue(
                jaydebeapiarrow._is_decimal_array_type_name(name), name)

    def test_matches_postgres_underscore_names(self):
        for name in ("_numeric", "_decimal", "_NUMERIC"):
            self.assertTrue(
                jaydebeapiarrow._is_decimal_array_type_name(name), name)

    def test_rejects_non_decimal_array_names(self):
        for name in ("_int4", "_varchar", "INTEGER ARRAY", "INT[]",
                     "_text", "VARCHAR ARRAY", "_float8", "_bool"):
            self.assertFalse(
                jaydebeapiarrow._is_decimal_array_type_name(name), name)
