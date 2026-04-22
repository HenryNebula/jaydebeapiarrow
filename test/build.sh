#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
EXT_DIR="$PROJECT_ROOT/arrow-jdbc-extension"
JARS_DIR="$SCRIPT_DIR/jars"

echo "Building arrow-jdbc-extension JAR..."
cd "$EXT_DIR"
mvn clean compile assembly:single

mkdir -p "$PROJECT_ROOT/jaydebeapiarrow/lib"
cp "$EXT_DIR/target/"*-jar-with-dependencies.jar "$PROJECT_ROOT/jaydebeapiarrow/lib/"

JAR_FILE=$(ls "$PROJECT_ROOT/jaydebeapiarrow/lib/arrow-jdbc"*"-jar-with-dependencies.jar" 2>/dev/null | head -1)
if [ ! -f "$JAR_FILE" ]; then
    echo "Error: Failed to build arrow-jdbc-extension JAR"
    exit 1
fi

echo "Build complete: $JAR_FILE"

# Build MockDriver (used by mock tests)
MOCK_DIR="$PROJECT_ROOT/mockdriver"
MOCK_JARS_DIR="$SCRIPT_DIR/mock-jars"
echo "Building MockDriver..."
cd "$MOCK_DIR"
mvn clean compile assembly:single
mkdir -p "$MOCK_JARS_DIR"
cp "$MOCK_DIR/target/"*-jar-with-dependencies.jar "$MOCK_JARS_DIR/"

MOCK_JAR=$(ls "$MOCK_JARS_DIR/mockdriver"*"-jar-with-dependencies.jar" 2>/dev/null | head -1)
if [ ! -f "$MOCK_JAR" ]; then
    echo "Error: Failed to build MockDriver JAR"
    exit 1
fi
echo "MockDriver build complete: $MOCK_JAR"
