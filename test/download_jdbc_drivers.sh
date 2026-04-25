#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEST_DIR="$SCRIPT_DIR/jars"

mkdir -p "$DEST_DIR"

download() {
  local group="$1"
  local artifact="$2"
  local version="$3"
  local jar="${artifact}-${version}.jar"
  local dest="$DEST_DIR/$jar"

  if [ -f "$dest" ]; then
    echo "Already exists: $jar"
  else
    local url="https://repo1.maven.org/maven2/${group//.//}/${artifact}/${version}/${jar}"
    echo "Downloading $jar..."
    curl -o "$dest" -L "$url"
    echo "Downloaded to $dest"
  fi
}

download_classifier() {
  local group="$1"
  local artifact="$2"
  local version="$3"
  local classifier="$4"
  local jar="${artifact}-${version}-${classifier}.jar"
  local dest="$DEST_DIR/$jar"

  if [ -f "$dest" ]; then
    echo "Already exists: $jar"
  else
    local url="https://repo1.maven.org/maven2/${group//.//}/${artifact}/${version}/${jar}"
    echo "Downloading $jar..."
    curl -o "$dest" -L "$url"
    echo "Downloaded to $dest"
  fi
}

echo "Downloading JDBC drivers to $DEST_DIR..."
echo ""

if [ "${1}" = "--java8" ]; then
  # Java 8 compatible driver versions (class file version 52.0)
  download org.postgresql           postgresql          42.7.4
  download com.mysql                mysql-connector-j   8.0.33
  download org.xerial               sqlite-jdbc         3.45.1.0
  download_classifier org.hsqldb   hsqldb              2.7.4 jdk8
  download com.microsoft.sqlserver  mssql-jdbc          12.6.1.jre8
  # Oracle (ojdbc), DB2 (jcc), Trino, Drill skipped — no Java 8 variants needed
else
  download org.postgresql           postgresql          42.7.2
  download com.mysql                mysql-connector-j   9.1.0
  download org.xerial               sqlite-jdbc         3.45.1.0
  download org.hsqldb               hsqldb              2.7.2
  download com.oracle.database.jdbc ojdbc11             23.5.0.24.07
  download com.microsoft.sqlserver  mssql-jdbc          12.6.1.jre11
  download com.ibm.db2              jcc                 11.5.9.0
  download io.trino                 trino-jdbc          461
  download org.apache.drill.exec    drill-jdbc-all      1.21.0
fi

echo ""
echo "All drivers downloaded."
