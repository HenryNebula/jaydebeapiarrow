#!/usr/bin/env python3
"""
Utility to analyze and compare benchmark results from JSON files.

Usage:
    # Compare multiple result files
    python benchmark/analyze_results.py benchmark/results/*.json

    # Show summary of a single result
    python benchmark/analyze_results.py benchmark/results/rows_benchmark_20250129_204530.json
"""
import json
import sys
from pathlib import Path
import argparse

def load_result(filepath):
    """Load and parse a benchmark result JSON file"""
    with open(filepath, 'r') as f:
        return json.load(f)

def print_summary(result):
    """Print a summary of benchmark results"""
    print(f"\n{'='*80}")
    print(f" Benchmark Summary")
    print(f"{'='*80}")
    print(f"Test Type: {result['test_type']}")
    print(f"Timestamp: {result['metadata']['timestamp']}")
    print(f"Platform: {result['metadata']['platform']}")
    print(f"Python: {result['metadata']['python_version']}")
    print(f"Iterations: {result['iterations']}")

    results = result['results']
    if result['test_type'] == 'rows':
        print(f"\n{'Rows':<12} | {'Method':<20} | {'Time (s)':<12} | {'Speedup':<10}")
        print("-" * 80)

        for size, methods in sorted(results.items(), key=lambda x: int(x[0])):
            base_time = next((m['time'] for m in methods if m['name'] == "Original"), 0)
            for method in methods:
                speedup = base_time / method['time'] if method['time'] > 0 and base_time > 0 else 0.0
                print(f"{int(size):<12} | {method['name']:<20} | {method['time']:<12.4f} | {speedup:<10.2f}x")
            print("-" * 80)

    elif result['test_type'] == 'columns':
        print(f"\n{'Columns':<12} | {'Method':<20} | {'Time (s)':<12} | {'Speedup':<10}")
        print("-" * 80)

        for cols, methods in sorted(results.items(), key=lambda x: int(x[0])):
            base_time = next((m['time'] for m in methods if m['name'] == "Original"), 0)
            for method in methods:
                speedup = base_time / method['time'] if method['time'] > 0 and base_time > 0 else 0.0
                print(f"{int(cols):<12} | {method['name']:<20} | {method['time']:<12.4f} | {speedup:<10.2f}x")
            print("-" * 80)

def compare_results(result_files):
    """Compare multiple benchmark result files"""
    print(f"\n{'='*80}")
    print(f" Benchmark Comparison ({len(result_files)} files)")
    print(f"{'='*80}")

    results = [load_result(f) for f in result_files]

    # Group by test type
    by_type = {}
    for r in results:
        t = r['test_type']
        if t not in by_type:
            by_type[t] = []
        by_type[t].append(r)

    for test_type, type_results in by_type.items():
        print(f"\n{test_type.upper()} Tests:")
        print("-" * 80)

        for r in type_results:
            timestamp = r['metadata']['timestamp']
            # Calculate average speedup for Arrow (Native)
            avg_speedup = 0
            count = 0
            for size, methods in r['results'].items():
                base_time = next((m['time'] for m in methods if m['name'] == "Original"), 0)
                native_time = next((m['time'] for m in methods if m['name'] == "Arrow (Native)"), 0)
                if base_time > 0 and native_time > 0:
                    avg_speedup += base_time / native_time
                    count += 1

            if count > 0:
                avg_speedup /= count

            print(f"  {timestamp}: Avg {avg_speedup:.2f}x speedup (Arrow Native)")

def main():
    parser = argparse.ArgumentParser(description="Analyze benchmark results")
    parser.add_argument("files", nargs="+", help="JSON result files to analyze")
    parser.add_argument("--compare", action="store_true", help="Compare multiple result files")

    args = parser.parse_args()

    # Validate files exist
    files = [Path(f) for f in args.files]
    for f in files:
        if not f.exists():
            print(f"Error: File not found: {f}", file=sys.stderr)
            sys.exit(1)

    if len(files) == 1 or not args.compare:
        # Show summary for each file
        for f in files:
            result = load_result(f)
            print_summary(result)
    else:
        # Compare results
        compare_results(files)

if __name__ == "__main__":
    main()
