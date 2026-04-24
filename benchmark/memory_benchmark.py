#!/usr/bin/env python3
"""Memory consumption benchmark for jaydebeapi vs jaydebeapiarrow.

Measures RSS and Python heap memory over 10 rounds of query/fetch cycles
to detect memory accumulation (leaks).

Usage:
    # Run all comparisons (coordinator mode):
    CLASSPATH="test/jars/*" uv run python benchmark/memory_benchmark.py

    # Run a single implementation (worker mode):
    CLASSPATH="test/jars/*" uv run python benchmark/memory_benchmark.py --mode arrow
    CLASSPATH="test/jars/*" uv run python benchmark/memory_benchmark.py --mode original
"""

import argparse
import gc
import json
import os
import resource
import subprocess
import sys
import time
import tracemalloc

# --- Configuration ---
NUM_ROUNDS = 10
ROW_COUNT = 100_000
HSQLDB_JAR = os.path.abspath("test/jars/hsqldb-2.7.4.jar")
JDBC_URL = "jdbc:hsqldb:mem:mem_bench"
JDBC_DRIVER = "org.hsqldb.jdbcDriver"
JDBC_USER = "SA"
JDBC_PASS = ""


def log(msg):
    print(msg, flush=True)


def get_rss_mb():
    """Get current RSS in MB."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def get_tracemalloc_stats():
    """Get current tracemalloc snapshot stats."""
    current, peak = tracemalloc.get_traced_memory()
    return current / 1024.0 / 1024.0, peak / 1024.0 / 1024.0


def setup_hsqldb(conn):
    """Create table and insert ROW_COUNT rows into HSQLDB."""
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS bench_test")
    cursor.execute("CREATE TABLE bench_test (id INTEGER, val_int INTEGER, val_str VARCHAR(50), val_dbl DOUBLE)")

    # Use batch inserts with autocommit disabled for performance
    conn.jconn.setAutoCommit(False)
    stmt = conn.jconn.prepareStatement("INSERT INTO bench_test (id, val_int, val_str, val_dbl) VALUES (?, ?, ?, ?)")

    batch_size = 10_000
    log(f"  Inserting {ROW_COUNT} rows (batch={batch_size})...")
    for offset in range(0, ROW_COUNT, batch_size):
        for i in range(batch_size):
            row_id = offset + i
            if row_id >= ROW_COUNT:
                break
            stmt.setInt(1, row_id)
            stmt.setInt(2, row_id % 1000)
            stmt.setString(3, f"str_{row_id}")
            stmt.setDouble(4, row_id * 0.1)
            stmt.addBatch()
        stmt.executeBatch()
        conn.jconn.commit()
        if (offset + batch_size) % 100_000 == 0 or offset + batch_size >= ROW_COUNT:
            log(f"    {min(offset + batch_size, ROW_COUNT):,}/{ROW_COUNT:,}")

    conn.jconn.setAutoCommit(True)
    stmt.close()
    log(f"  Inserted {ROW_COUNT} rows.")


def run_benchmark_original():
    """Benchmark original jaydebeapi memory consumption."""
    import jaydebeapi

    tracemalloc.start()
    gc.collect()

    rss_before = get_rss_mb()

    conn = jaydebeapi.connect(
        JDBC_DRIVER, JDBC_URL, [JDBC_USER, JDBC_PASS], HSQLDB_JAR
    )
    setup_hsqldb(conn)

    gc.collect()
    rss_after_setup = get_rss_mb()
    tm_after_setup, _ = get_tracemalloc_stats()

    log(f"  Memory after setup: RSS={rss_after_setup:.1f}MB, heap={tm_after_setup:.1f}MB")

    results = []
    for round_num in range(1, NUM_ROUNDS + 1):
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM bench_test")
        rows = cursor.fetchall()
        cursor.close()
        del rows
        gc.collect()

        rss = get_rss_mb()
        tm_current, tm_peak = get_tracemalloc_stats()
        results.append({
            "round": round_num,
            "rss_mb": round(rss, 1),
            "heap_mb": round(tm_current, 1),
            "peak_mb": round(tm_peak, 1),
        })
        log(f"  Round {round_num:2d}: RSS={rss:.1f}MB, heap={tm_current:.1f}MB, peak={tm_peak:.1f}MB")

    conn.close()
    tracemalloc.stop()

    rss_final = get_rss_mb()
    rss_growth = rss_final - rss_before
    heap_first = results[0]["heap_mb"]
    heap_last = results[-1]["heap_mb"]
    heap_growth = heap_last - heap_first

    log(f"\n  RSS growth (start->end): {rss_growth:.1f}MB")
    log(f"  Heap growth (round1->round{NUM_ROUNDS}): {heap_growth:.1f}MB")

    return {
        "rss_before_mb": round(rss_before, 1),
        "rss_after_setup_mb": round(rss_after_setup, 1),
        "heap_growth_mb": round(heap_growth, 1),
        "rounds": results,
    }


def run_benchmark_arrow():
    """Benchmark jaydebeapiarrow memory consumption."""
    import jaydebeapiarrow

    tracemalloc.start()
    gc.collect()

    rss_before = get_rss_mb()

    conn = jaydebeapiarrow.connect(
        JDBC_DRIVER, JDBC_URL, [JDBC_USER, JDBC_PASS], jars=[HSQLDB_JAR]
    )
    setup_hsqldb(conn)

    gc.collect()
    rss_after_setup = get_rss_mb()
    tm_after_setup, _ = get_tracemalloc_stats()

    log(f"  Memory after setup: RSS={rss_after_setup:.1f}MB, heap={tm_after_setup:.1f}MB")

    results = []
    for round_num in range(1, NUM_ROUNDS + 1):
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM bench_test")
        rows = cursor.fetchall()
        cursor.close()
        del rows
        gc.collect()

        rss = get_rss_mb()
        tm_current, tm_peak = get_tracemalloc_stats()
        results.append({
            "round": round_num,
            "rss_mb": round(rss, 1),
            "heap_mb": round(tm_current, 1),
            "peak_mb": round(tm_peak, 1),
        })
        log(f"  Round {round_num:2d}: RSS={rss:.1f}MB, heap={tm_current:.1f}MB, peak={tm_peak:.1f}MB")

    conn.close()
    tracemalloc.stop()

    rss_final = get_rss_mb()
    rss_growth = rss_final - rss_before
    heap_first = results[0]["heap_mb"]
    heap_last = results[-1]["heap_mb"]
    heap_growth = heap_last - heap_first

    log(f"\n  RSS growth (start->end): {rss_growth:.1f}MB")
    log(f"  Heap growth (round1->round{NUM_ROUNDS}): {heap_growth:.1f}MB")

    return {
        "rss_before_mb": round(rss_before, 1),
        "rss_after_setup_mb": round(rss_after_setup, 1),
        "heap_growth_mb": round(heap_growth, 1),
        "rounds": results,
    }


def run_worker(mode):
    """Run a single benchmark worker and output JSON result."""
    if mode == "original":
        result = run_benchmark_original()
    elif mode == "arrow":
        result = run_benchmark_arrow()
    else:
        print(f"Unknown mode: {mode}", file=sys.stderr)
        sys.exit(1)
    print("BENCHMARK_RESULT:" + json.dumps(result), flush=True)


def main():
    global ROW_COUNT, NUM_ROUNDS

    parser = argparse.ArgumentParser(description="Memory consumption benchmark")
    parser.add_argument(
        "--mode",
        choices=["original", "arrow"],
        help="Worker mode: run a single implementation",
    )
    parser.add_argument(
        "--rows", type=int, default=ROW_COUNT, help="Number of rows (default: 100K)"
    )
    parser.add_argument(
        "--rounds", type=int, default=NUM_ROUNDS, help="Number of fetch rounds (default: 10)"
    )
    args = parser.parse_args()

    ROW_COUNT = args.rows
    NUM_ROUNDS = args.rounds

    if args.mode:
        run_worker(args.mode)
    else:
        # Coordinator mode
        print("=" * 70, flush=True)
        print(f" Memory Consumption Benchmark", flush=True)
        print(f" {ROW_COUNT:,} rows x {NUM_ROUNDS} rounds", flush=True)
        print("=" * 70, flush=True)

        results = {}

        for mode, label in [("original", "jaydebeapi (original)"), ("arrow", "jaydebeapiarrow (dev)")]:
            print(f"\n--- {label} ---", flush=True)
            proc = subprocess.Popen(
                [sys.executable, __file__, "--mode", mode, "--rows", str(ROW_COUNT), "--rounds", str(NUM_ROUNDS)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            stdout, stderr = proc.communicate()

            for line in stdout.strip().split("\n"):
                print(f"  {line}", flush=True)
            if stderr.strip():
                for line in stderr.strip().split("\n"):
                    print(f"  [stderr] {line}", file=sys.stderr, flush=True)

            for line in stdout.strip().split("\n"):
                if line.startswith("BENCHMARK_RESULT:"):
                    results[label] = json.loads(line[len("BENCHMARK_RESULT:"):])
                    break

        # --- Summary ---
        print("\n" + "=" * 70, flush=True)
        print(" SUMMARY", flush=True)
        print("=" * 70, flush=True)

        header = f"{'Implementation':<30} | {'Heap Growth':>12} | {'RSS After Setup':>14} | {'Verdict':>10}"
        print(header, flush=True)
        print("-" * len(header), flush=True)

        for label, data in results.items():
            heap_growth = data.get("heap_growth_mb", "N/A")
            rss_setup = data.get("rss_after_setup_mb", "N/A")
            if isinstance(heap_growth, (int, float)):
                verdict = "LEAK" if heap_growth > 10 else "OK"
            else:
                verdict = "N/A"
            print(f"{label:<30} | {heap_growth:>10} MB | {rss_setup:>12} MB | {verdict:>10}", flush=True)

        # Per-round comparison
        print("\n--- Per-Round Heap Memory (MB) ---", flush=True)
        all_labels = list(results.keys())
        if all_labels:
            rounds_header = f"{'Round':<6} | " + " | ".join(f"{l:>20}" for l in all_labels)
            print(rounds_header, flush=True)
            print("-" * len(rounds_header), flush=True)
            max_rounds = max(len(results[l].get("rounds", [])) for l in all_labels)
            for r in range(max_rounds):
                parts = [f"{r+1:<6}"]
                for label in all_labels:
                    rounds_data = results[label].get("rounds", [])
                    if r < len(rounds_data):
                        parts.append(f"{rounds_data[r]['heap_mb']:>18} MB")
                    else:
                        parts.append(f"{'N/A':>20}")
                print(" | ".join(parts), flush=True)


if __name__ == "__main__":
    main()
