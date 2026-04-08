#!/bin/bash
set -e

# Define driver version and path
DRIVER_GROUP="org.postgresql"
DRIVER_ARTIFACT="postgresql"
DRIVER_VERSION="42.7.2"
DRIVER_JAR="postgresql-${DRIVER_VERSION}.jar"
DEST_DIR="$(pwd)/test/jars"

mkdir -p "$DEST_DIR"

DEST_PATH="$DEST_DIR/$DRIVER_JAR"

if [ -f "$DEST_PATH" ]; then
    echo "Driver $DEST_PATH already exists."
else
    echo "Downloading PostgreSQL JDBC driver..."
    # Re-use the existing mvnget logic or just curl it directly for simplicity here
    URL="https://repo1.maven.org/maven2/org/postgresql/postgresql/${DRIVER_VERSION}/${DRIVER_JAR}"
    curl -o "$DEST_PATH" -L "$URL"
    echo "Downloaded to $DEST_PATH"
fi

echo "Driver path: $DEST_PATH"
