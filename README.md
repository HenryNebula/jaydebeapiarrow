# JayDeBeApiArrow - High-Performance JDBC to Python DB-API Bridge

[![Test Status]()]()
[![PyPI version](https://img.shields.io/pypi/v/JayDeBeApiArrow.svg)](https://pypi.python.org/pypi/JayDeBeApiArrow/)

The **JayDeBeApiArrow** module allows you to connect from Python code to databases using Java [JDBC](http://java.sun.com/products/jdbc/overview.html). It provides a Python [DB-API v2.0](http://www.python.org/dev/peps/pep-0249/) to that database.

> **Note:** This is a fork of the original [JayDeBeApi](https://github.com/baztian/jaydebeapi) project.

## Key Differences in this Fork

1.  **High Performance with Apache Arrow:**
    The primary goal of this fork is to significantly improve data fetch performance. Instead of iterating through JDBC ResultSets row-by-row in Python (which has high overhead), this library uses a custom Java extension (`arrow-jdbc-extension`) to convert JDBC data into **Apache Arrow** record batches directly within the JVM. These batches are then efficiently transferred to Python.

2.  **Modernization:**
    *   **Python 3 Only:** Support for Python 2 has been removed.
    *   **JPype Only:** Support for Jython has been removed to focus on the CPython + JPype architecture.
    *   **Strict Typing:** Enforces stricter typing for Decimal and temporal types.

It works on ordinary Python (cPython) using the [JPype](https://pypi.python.org/pypi/JPype1/) Java integration.

## Install

You can get and install JayDeBeApiArrow with pip:

```bash
pip install JayDeBeApiArrow
```

Or you can get a copy of the source by cloning from the [JayDeBeApiArrow github project](https://github.com/HenryNebula/jaydebeapiArrow) and install with:

```bash
python setup.py install
```

Ensure that you have installed [JPype](https://pypi.python.org/pypi/JPype1/) properly.

## Usage

Basically you just import the `jaydebeapiarrow` Python module and execute the `connect` method. This gives you a DB-API conform connection to the database.

The first argument to `connect` is the name of the Java driver class. The second argument is a string with the JDBC connection URL. Third you can optionally supply a sequence consisting of user and password or alternatively a dictionary containing arguments that are internally passed as properties to the Java `DriverManager.getConnection` method. See the Javadoc of `DriverManager` class for details.

The next parameter to `connect` is optional as well and specifies the jar-Files of the driver if your classpath isn't set up sufficiently yet. The classpath set in `CLASSPATH` environment variable will be honored.

Here is an example:

```python
import jaydebeapiarrow
conn = jaydebeapiarrow.connect(
    "org.hsqldb.jdbcDriver",
    "jdbc:hsqldb:mem:.",
    ["SA", ""],
    "/path/to/hsqldb.jar"
)
curs = conn.cursor()
curs.execute('create table CUSTOMER'
             '("CUST_ID" INTEGER not null,'
             ' "NAME" VARCHAR(50) not null,'
             ' primary key ("CUST_ID"))')
curs.execute("insert into CUSTOMER values (?, ?)", (1, 'John'))
curs.execute("select * from CUSTOMER")
print(curs.fetchall())
# Output: [(1, 'John')]
curs.close()
conn.close()
```

If you're having trouble getting this work check if your `JAVA_HOME` environment variable is set correctly. For example:

```bash
JAVA_HOME=/usr/lib/jvm/java-8-openjdk python
```

An alternative way to establish connection using connection properties:

```python
conn = jaydebeapiarrow.connect(
    "org.hsqldb.jdbcDriver",
    "jdbc:hsqldb:mem:.",
    {
        'user': "SA", 'password': "",
        'other_property': "foobar"
    },
    "/path/to/hsqldb.jar"
)
```

Also using the `with` statement might be handy:

```python
with jaydebeapiarrow.connect(
    "org.hsqldb.jdbcDriver",
    "jdbc:hsqldb:mem:.",
    ["SA", ""],
    "/path/to/hsqldb.jar"
) as conn:
    with conn.cursor() as curs:
        curs.execute("select count(*) from CUSTOMER")
        print(curs.fetchall())
        # Output: [(1,)]
```

## Supported Databases

In theory *every database with a suitable JDBC driver should work*. It is confirmed to work with the following databases:

*   SQLite
*   Hypersonic SQL (HSQLDB)
*   IBM DB2
*   IBM DB2 for mainframes
*   Oracle
*   Teradata DB
*   Netezza
*   Mimer DB
*   Microsoft SQL Server
*   MySQL
*   PostgreSQL
*   ...and many more.

## Contributing

Please submit bugs and patches to the [JayDeBeApiArrow issue tracker](https://github.com/HenryNebula/jaydebeapiArrow/issues). All contributors will be acknowledged. Thanks!

## License

JayDeBeApiArrow is released under the GNU Lesser General Public license (LGPL). See the file `COPYING` and `COPYING.LESSER` in the distribution for details.
