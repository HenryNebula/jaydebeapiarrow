#!/bin/bash
set -e

# Downloads Apache Drill JDBC driver from Maven Central

DEST_DIR="${1:-./}"
DRILL_VERSION="1.21.0"
JAR_NAME="drill-jdbc-all-${DRILL_VERSION}.jar"
DEST="${DEST_DIR}/${JAR_NAME}"
URL="https://repo1.maven.org/maven2/org/apache/drill/exec/drill-jdbc-all/${DRILL_VERSION}/${JAR_NAME}"

if [ -f "$DEST" ]; then
    echo "Already exists: $JAR_NAME"
else
    echo "Downloading $JAR_NAME..."
    curl -o "$DEST" -L "$URL"
    echo "Downloaded to $DEST"
fi
