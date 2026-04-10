#!/bin/bash
set -e

# Downloads Apache Drill JDBC driver from Apache archives (not on Maven Central)

DEST_DIR="${1:-./}"
DRILL_VERSION="1.21.0"
JAR_NAME="drill-jdbc-all-${DRILL_VERSION}.jar"
DEST="${DEST_DIR}/${JAR_NAME}"

if [ -f "$DEST" ]; then
    echo "Already exists: $JAR_NAME"
else
    URL="https://archive.apache.org/dist/drill/${DRILL_VERSION}/apache-drill-${DRILL_VERSION}-tar.gz"
    echo "Downloading Drill ${DRILL_VERSION} distribution..."
    TMPDIR=$(mktemp -d)
    curl -L "$URL" | tar xz -C "$TMPDIR"
    SRC="${TMPDIR}/apache-drill-${DRILL_VERSION}/jars/jdbc-driver/${JAR_NAME}"
    if [ ! -f "$SRC" ]; then
        # Try alternate location
        SRC=$(find "${TMPDIR}/apache-drill-${DRILL_VERSION}" -name "drill-jdbc-all*.jar" | head -1)
    fi
    if [ -z "$SRC" ] || [ ! -f "$SRC" ]; then
        echo "ERROR: Could not find Drill JDBC driver JAR in distribution"
        rm -rf "$TMPDIR"
        exit 1
    fi
    cp "$SRC" "$DEST"
    rm -rf "$TMPDIR"
    echo "Downloaded $JAR_NAME to $DEST"
fi
