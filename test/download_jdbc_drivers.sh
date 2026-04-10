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

echo "Downloading JDBC drivers to $DEST_DIR..."
echo ""

download org.postgresql    postgresql  42.7.2
download com.mysql         mysql-connector-j 9.1.0
download org.xerial        sqlite-jdbc 3.45.1.0
download org.hsqldb        hsqldb       2.7.2
download com.oracle.database.jdbc  ojdbc11      23.5.0.24.07
download com.microsoft.sqlserver   mssql-jdbc   12.6.1.jre11
download com.ibm.db2               jcc          11.5.9.0
download io.trino                  trino-jdbc   461

echo ""
echo "Downloading Apache Drill JDBC driver..."
bash "$SCRIPT_DIR/../ci/download_drill_jdbc.sh" "$DEST_DIR"

echo ""
echo "All drivers downloaded."
