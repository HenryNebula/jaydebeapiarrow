import time
import os
import sys
import jpype
import pandas as pd
import jaydebeapi
import jaydebeapiarrow
import pyarrow as pa
import argparse
import subprocess
import json
import psycopg2
import platform
from datetime import datetime
from pathlib import Path

# --- Configuration ---
JDBC_DRIVER_PATH = os.path.abspath("test/jars/postgresql-42.7.2.jar")
JDBC_CLASS = "org.postgresql.Driver"
DB_HOST = os.environ.get("BENCH_DB_HOST", "localhost")
DB_PORT = os.environ.get("BENCH_DB_PORT", "15432")
DB_NAME = os.environ.get("BENCH_DB_NAME", "test_db")
DB_USER = os.environ.get("BENCH_DB_USER", "user")
DB_PASS = os.environ.get("BENCH_DB_PASS", "password")
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
QUERY = "SELECT * FROM benchmark_test"
ITERATIONS = 3 # Reduced iterations for larger datasets to save time

def get_system_info():
    """Collect system information for benchmark metadata"""
    return {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "hostname": platform.node(),
    }

def save_results(results_data, test_type, output_path=None):
    """Save benchmark results to JSON file"""
    if output_path is None:
        # Generate default filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"benchmark/results/{test_type}_benchmark_{timestamp}.json"

    # Ensure directory exists
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Prepare full result with metadata
    full_result = {
        "metadata": get_system_info(),
        "test_type": test_type,
        "iterations": ITERATIONS,
        "results": results_data
    }

    # Save to file
    with open(output_path, 'w') as f:
        json.dump(full_result, f, indent=2)

    print(f"\n✓ Results saved to: {output_path}", flush=True)
    return str(output_path)

def get_connection_original():
    return jaydebeapi.connect(
        JDBC_CLASS,
        JDBC_URL,
        [DB_USER, DB_PASS],
        JDBC_DRIVER_PATH,
    )

def get_connection_arrow():
    return jaydebeapiarrow.connect(
        JDBC_CLASS,
        JDBC_URL,
        [DB_USER, DB_PASS],
        jars=[JDBC_DRIVER_PATH],
    )

def get_connection_psycopg2():
    # Parse JDBC URL for psycopg2 (simple parsing assumption)
    # jdbc:postgresql://localhost:5432/test_db
    clean_url = JDBC_URL.replace("jdbc:postgresql://", "")
    host_port, dbname = clean_url.split("/")
    host, port = host_port.split(":")
    return psycopg2.connect(
        dbname=dbname,
        user=DB_USER,
        password=DB_PASS,
        host=host,
        port=port
    )

def benchmark_psycopg2():
    durations = []
    rows = 0
    for i in range(ITERATIONS):
        try:
            conn = get_connection_psycopg2()
            start = time.time()
            curs = conn.cursor()
            curs.execute(QUERY)
            data = curs.fetchall()
            curs.close()
            conn.close()
            dur = time.time() - start
            durations.append(dur)
            rows = len(data)
            print(f"  Run {i+1}: {dur:.4f}s ({rows} rows)", flush=True)
        except Exception as e:
             print(f"  Run {i+1} failed: {e}", flush=True)
             import traceback
             traceback.print_exc()

    return sum(durations) / len(durations) if durations else 0, rows

def benchmark_original(expected_total_rows=None):
    """
    Benchmark original jaydebeapi with improved progress tracking and extrapolation.

    Improvements:
    - Track time-per-batch to detect performance degradation
    - Use weighted average (recent batches matter more)
    - Require minimum sample size before extrapolating
    - Provide confidence bounds on extrapolation
    """
    durations = []
    rows = 0
    TIMEOUT_SECONDS = 300  # 5 minutes
    BATCH_SIZE = 50000
    MIN_BATCHES_FOR_EXTRAPOLATION = 5  # Require at least 5 batches
    MIN_SAMPLE_RATIO = 0.10  # Require at least 10% of data for extrapolation

    for i in range(ITERATIONS):
        try:
            conn = get_connection_original()
            start = time.time()
            curs = conn.cursor()
            curs.execute(QUERY)

            rows_fetched = 0
            is_timeout = False

            # Track per-batch timing for progress analysis
            batch_times = []
            batch_rows = []
            last_progress_time = start

            while True:
                # Check timeout
                elapsed = time.time() - start
                if elapsed > TIMEOUT_SECONDS:
                    print(f"  Run {i+1} TIMEOUT after {elapsed:.2f}s.", flush=True)
                    is_timeout = True
                    break

                batch_start = time.time()
                batch = curs.fetchmany(BATCH_SIZE)
                batch_end = time.time()

                if not batch:
                    break

                batch_size = len(batch)
                rows_fetched += batch_size

                # Track batch timing
                batch_time = batch_end - batch_start
                batch_times.append(batch_time)
                batch_rows.append(batch_size)

                # Print progress every 10 seconds
                now = time.time()
                if now - last_progress_time >= 10:
                    rows_per_sec = rows_fetched / (now - start)
                    pct_complete = (rows_fetched / expected_total_rows * 100) if expected_total_rows else 0
                    print(f"    Progress: {rows_fetched:,} rows ({pct_complete:.1f}%) at {rows_per_sec:,.0f} rows/s", flush=True)
                    last_progress_time = now

            curs.close()
            conn.close()

            if is_timeout:
                if rows_fetched > 0 and expected_total_rows:
                    # Check if we have enough data for reliable extrapolation
                    num_batches = len(batch_times)
                    sample_ratio = rows_fetched / expected_total_rows

                    has_min_batches = num_batches >= MIN_BATCHES_FOR_EXTRAPOLATION
                    has_min_sample = sample_ratio >= MIN_SAMPLE_RATIO

                    if not (has_min_batches and has_min_sample):
                        print(f"    Warning: Insufficient data for extrapolation", flush=True)
                        print(f"      Batches: {num_batches} (need >= {MIN_BATCHES_FOR_EXTRAPOLATION})", flush=True)
                        print(f"      Sample: {sample_ratio:.1%} (need >= {MIN_SAMPLE_RATIO:.0%})", flush=True)

                        # Still extrapolate but mark as unreliable
                        extrapolation_reliable = False
                    else:
                        extrapolation_reliable = True

                    # Analyze batch timing trend
                    recent_batches = min(10, num_batches)
                    recent_throughput = sum(batch_rows[-recent_batches:]) / sum(batch_times[-recent_batches:])
                    overall_throughput = rows_fetched / elapsed

                    # Use recent throughput for extrapolation (accounts for degradation)
                    throughput_ratio = recent_throughput / overall_throughput if overall_throughput > 0 else 1.0

                    if throughput_ratio < 0.8:
                        print(f"    Warning: Performance degrading (recent: {recent_throughput:,.0f} rows/s, overall: {overall_throughput:,.0f} rows/s)", flush=True)
                        extrapolation_reliable = False

                    # Extrapolate using recent throughput
                    remaining_rows = expected_total_rows - rows_fetched
                    estimated_remaining = remaining_rows / recent_throughput if recent_throughput > 0 else 0
                    dur = elapsed + estimated_remaining
                    rows = expected_total_rows

                    # Calculate confidence bounds (±20% to account for variability)
                    confidence_min = dur * 0.8
                    confidence_max = dur * 1.2

                    reliability_marker = "~" if extrapolation_reliable else "?"
                    print(f"  Run {i+1}: {reliability_marker}{dur:.4f}s (EXTRAPOLATED: {confidence_min:.2f}-{confidence_max:.2f}s)", flush=True)
                    print(f"      Fetched: {rows_fetched:,}/{expected_total_rows:,} rows ({sample_ratio:.1%})", flush=True)
                    print(f"      Recent throughput: {recent_throughput:,.0f} rows/s", flush=True)

                else:
                    # Fallback if we can't extrapolate
                    dur = elapsed
                    rows = rows_fetched
                    print(f"  Run {i+1}: {dur:.4f}s (TIMEOUT, incomplete: {rows:,} rows)", flush=True)
            else:
                dur = time.time() - start
                rows = rows_fetched
                print(f"  Run {i+1}: {dur:.4f}s ({rows} rows)", flush=True)

            durations.append(dur)

        except Exception as e:
             print(f"  Run {i+1} failed: {e}", flush=True)
             import traceback
             traceback.print_exc()

    return sum(durations) / len(durations) if durations else 0, rows

def benchmark_arrow_fetchall():
    durations = []
    rows = 0
    for i in range(ITERATIONS):
        try:
            conn = get_connection_arrow()
            start = time.time()
            curs = conn.cursor()
            curs.execute(QUERY)
            data = curs.fetchall()
            curs.close()
            conn.close()
            dur = time.time() - start
            durations.append(dur)
            rows = len(data)
            print(f"  Run {i+1}: {dur:.4f}s ({rows} rows)", flush=True)
        except Exception as e:
            print(f"  Run {i+1} failed: {e}", flush=True)
            import traceback
            traceback.print_exc()

    return sum(durations) / len(durations) if durations else 0, rows

def benchmark_arrow_native():
    durations = []
    total_rows = 0
    for i in range(ITERATIONS):
        try:
            conn = get_connection_arrow()
            start = time.time()
            curs = conn.cursor()
            curs.execute(QUERY)

            # Use Native Arrow API - zero-copy RecordBatch access
            current_run_rows = 0
            for batch in curs.fetch_arrow_batches():
                # Access batch metadata without Python conversion
                current_run_rows += batch.num_rows
                # In real usage, user would process batch here:
                # df = batch.to_pandas()  # User's choice
                # OR: process batch directly with Arrow-compatible libraries

            curs.close()
            conn.close()
            dur = time.time() - start
            durations.append(dur)
            total_rows = current_run_rows
            print(f"  Run {i+1}: {dur:.4f}s ({current_run_rows} rows)", flush=True)
        except Exception as e:
             print(f"  Run {i+1} failed: {e}", flush=True)
             import traceback
             traceback.print_exc()

    return sum(durations) / len(durations) if durations else 0, total_rows

def run_subprocess(mode, description, rows_count=None, cols_count=None):
    print(f"\n[{description}]", flush=True)
    cmd = [sys.executable, __file__, "--mode", mode]
    if rows_count:
        cmd.extend(["--rows", str(rows_count)])
    if cols_count:
        cmd.extend(["--columns", str(cols_count)])
    
    # Run the subprocess and stream output in real-time
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    last_line = ""
    # We need to read line by line to stream output
    # But we also need to capture the JSON result at the end
    
    while True:
        # Check if process has finished
        retcode = process.poll()
        
        # Read available output
        for line in process.stdout:
            line = line.strip()
            if line:
                # Attempt to detect if this is the JSON result line
                if line.startswith('{"time":') and line.endswith('}'):
                    last_line = line
                else:
                    print(line, flush=True)
        
        # Also print stderr
        for line in process.stderr:
             print(line.strip(), file=sys.stderr, flush=True)

        if retcode is not None:
            break
        
        time.sleep(0.1)

    try:
        return json.loads(last_line)
    except json.JSONDecodeError:
        print(f"Failed to parse result from subprocess. Last line was: {last_line}")
        return {"time": 0, "rows": 0}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["original", "arrow-tuple", "arrow-native", "psycopg2"], help="Benchmark mode (worker)")
    parser.add_argument("--rows", type=int, default=None, help="Expected number of rows (worker/extrapolation)")
    parser.add_argument("--columns", type=int, default=None, help="Expected number of columns")
    parser.add_argument("--test-type", choices=["rows", "columns"], default="rows", help="Type of benchmark suite to run (coordinator)")
    parser.add_argument("--output", type=str, default=None, help="Output JSON file path (default: benchmark/results/<type>_benchmark_<timestamp>.json)")
    parser.add_argument("--skip-original", action="store_true", help="Skip original jaydebeapi baseline (slow)")
    args = parser.parse_args()

    if args.mode:
        # --- Subprocess Mode (Worker) ---
        # 1. Warmup (if needed, or just rely on the first run of the loop)
        
        # --- JVM Initialization Hack ---
        # Ensure JVM is started by jaydebeapiarrow first if we are in arrow mode
        if "arrow" in args.mode:
            try:
                # Dummy connection to force JVM start with arrow classpath
                dummy = get_connection_arrow()
                dummy.close()
            except Exception as e:
                pass # Main connection will likely retry or fail with proper error

        avg_time, rows = 0, 0
        if args.mode == "original":
            avg_time, rows = benchmark_original(expected_total_rows=args.rows)
        elif args.mode == "arrow-tuple":
            avg_time, rows = benchmark_arrow_fetchall()
        elif args.mode == "arrow-native":
            avg_time, rows = benchmark_arrow_native()
        elif args.mode == "psycopg2":
            avg_time, rows = benchmark_psycopg2()
        
        # Output result as JSON on the last line
        print(json.dumps({"time": avg_time, "rows": rows}), flush=True)

    else:
        # --- Main Coordinator Mode ---
        if not os.path.exists(JDBC_DRIVER_PATH):
            print(f"Error: Driver not found at {JDBC_DRIVER_PATH}")
            print("Run 'bash benchmark/download_driver.sh' first.")
            sys.exit(1)
        
        if args.test_type == "rows":
            # --- Variable Rows Benchmark ---
            dataset_sizes = [1000000, 5000000, 10000000]
            fixed_cols = 4
            
            final_report = {}

            for rows_count in dataset_sizes:
                print(f"\n" + "#" * 60)
                print(f" PREPARING DATASET: {rows_count} rows, {fixed_cols} cols")
                print("#" * 60)
                
                # 1. Prepare Data
                subprocess.run([sys.executable, "benchmark/prepare_data.py", "--rows", str(rows_count), "--columns", str(fixed_cols)], check=True)
                
                print(f"\n--- Benchmark Running: {rows_count} Rows ---")
                
                results = []
                
                # 2. Run Benchmarks
                res_p = run_subprocess("psycopg2", f"Baseline (Psycopg2) - {rows_count} rows", rows_count, fixed_cols)
                results.append({"name": "Psycopg2", "time": res_p["time"]})

                if not args.skip_original:
                    res_a = run_subprocess("original", f"Baseline (Original) - {rows_count} rows", rows_count, fixed_cols)
                    results.append({"name": "Original", "time": res_a["time"]})

                res_b = run_subprocess("arrow-tuple", f"Arrow (Drop-in) - {rows_count} rows", rows_count, fixed_cols)
                results.append({"name": "Arrow (Drop-in)", "time": res_b["time"]})

                res_c = run_subprocess("arrow-native", f"Arrow (Native) - {rows_count} rows", rows_count, fixed_cols)
                results.append({"name": "Arrow (Native)", "time": res_c["time"]})

                final_report[rows_count] = results

            # --- Final Summary (Rows) ---
            print("\n" + "=" * 80)
            print(f" FINAL BENCHMARK REPORT (Variable Rows, Fixed 4 Cols)")
            print("=" * 80)

            print(f"{ 'Dataset':<12} | {'Method':<20} | {'Time (s)':<10} | {'Speedup':<10}")
            print("-" * 80)

            for size in dataset_sizes:
                res_list = final_report[size]
                base_time = next((r['time'] for r in res_list if r['name'] == "Original"), 0)

                for res in res_list:
                    speedup = base_time / res['time'] if res['time'] > 0 and base_time > 0 else 0.0
                    print(f"{size:<12} | {res['name']:<20} | {res['time']:<10.4f} | {speedup:<10.2f}x")
                print("-" * 80)

            # Save results to JSON
            save_results(final_report, "rows", args.output)

        elif args.test_type == "columns":
            # --- Variable Columns Benchmark ---
            column_counts = [4, 20, 40]
            fixed_rows = 1000000 # 1 Million
            
            final_report = {}

            for cols_count in column_counts:
                print(f"\n" + "#" * 60)
                print(f" PREPARING DATASET: {fixed_rows} rows, {cols_count} cols")
                print("#" * 60)
                
                # 1. Prepare Data
                subprocess.run([sys.executable, "benchmark/prepare_data.py", "--rows", str(fixed_rows), "--columns", str(cols_count)], check=True)
                
                print(f"\n--- Benchmark Running: {cols_count} Columns ---")
                
                results = []
                
                # 2. Run Benchmarks
                res_p = run_subprocess("psycopg2", f"Baseline (Psycopg2) - {cols_count} cols", fixed_rows, cols_count)
                results.append({"name": "Psycopg2", "time": res_p["time"]})

                if not args.skip_original:
                    res_a = run_subprocess("original", f"Baseline (Original) - {cols_count} cols", fixed_rows, cols_count)
                    results.append({"name": "Original", "time": res_a["time"]})

                res_b = run_subprocess("arrow-tuple", f"Arrow (Drop-in) - {cols_count} cols", fixed_rows, cols_count)
                results.append({"name": "Arrow (Drop-in)", "time": res_b["time"]})

                res_c = run_subprocess("arrow-native", f"Arrow (Native) - {cols_count} cols", fixed_rows, cols_count)
                results.append({"name": "Arrow (Native)", "time": res_c["time"]})

                final_report[cols_count] = results

            # --- Final Summary (Columns) ---
            print("\n" + "=" * 80)
            print(f" FINAL BENCHMARK REPORT (Variable Columns, Fixed 1M Rows)")
            print("=" * 80)

            print(f"{ 'Columns':<12} | {'Method':<20} | {'Time (s)':<10} | {'Speedup':<10}")
            print("-" * 80)

            for size in column_counts:
                res_list = final_report[size]
                base_time = next((r['time'] for r in res_list if r['name'] == "Original"), 0)

                for res in res_list:
                    speedup = base_time / res['time'] if res['time'] > 0 and base_time > 0 else 0.0
                    print(f"{size:<12} | {res['name']:<20} | {res['time']:<10.4f} | {speedup:<10.2f}x")
                print("-" * 80)

            # Save results to JSON
            save_results(final_report, "columns", args.output)
