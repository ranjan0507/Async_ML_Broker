#!/usr/bin/env python3
"""
Parses the results.csv produced by bench_worker.py and prints end-to-end
throughput and latency stats for AsyncML Broker.

Run this after bench_publisher.py has finished and the worker queue has
drained (i.e. bench_worker.py's terminal has stopped printing new lines).

USAGE
-----
    python3 report.py --file results.csv
"""
import argparse
import csv
import statistics


def main():
    parser = argparse.ArgumentParser(description="Report generator for AsyncML Broker benchmarks")
    parser.add_argument("--file", default="results.csv")
    args = parser.parse_args()

    latencies = []
    recv_times = []
    with open(args.file) as f:
        for row in csv.DictReader(f):
            if row["latency_ms"]:
                latencies.append(float(row["latency_ms"]))
                recv_times.append(float(row["recv_time"]))

    if not latencies:
        print("No completed tasks found in the results file yet.")
        print("Either the worker wasn't running before the publisher started,")
        print("or processing hasn't finished — wait and re-run this script.")
        return

    latencies.sort()
    n = len(latencies)
    span = max(recv_times) - min(recv_times) if n > 1 else 0

    print(f"Tasks completed        : {n}")
    if span > 0:
        print(f"Completion throughput  : {n/span:.1f} tasks/sec "
              f"(measured across the worker's completion window)")
    print(f"\nEnd-to-end latency (publish timestamp -> worker received), ms:")
    print(f"  min  : {latencies[0]:.2f}")
    print(f"  mean : {statistics.mean(latencies):.2f}")
    print(f"  p50  : {latencies[n//2]:.2f}")
    print(f"  p95  : {latencies[int(n*0.95)]:.2f}")
    print(f"  p99  : {latencies[min(int(n*0.99), n-1)]:.2f}")
    print(f"  max  : {latencies[-1]:.2f}")


if __name__ == "__main__":
    main()