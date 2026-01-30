#!/bin/bash
set -e

# 1. Create a fresh virtual environment
VENV_DIR="benchmark/.venv_bench"
echo "Creating virtual environment in $VENV_DIR..."
python3 -m venv "$VENV_DIR"

# 2. Activate
source "$VENV_DIR/bin/activate"

# 3. Install dependencies
echo "Installing dependencies from benchmark/requirements.txt..."
pip install -U pip
pip install -r benchmark/requirements.txt

# 4. Download Driver
echo "Downloading JDBC Driver..."
bash benchmark/download_driver.sh

# 5. Run Comparison
echo "Running Benchmark..."
python benchmark/compare_performance.py
