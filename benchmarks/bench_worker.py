#!/usr/bin/env python3
"""
Benchmark worker for AsyncML Broker.

Spins up N worker processes that, for every task received, record how long
it took from publish-time to receive-time (end-to-end latency) into a CSV
file. Run this FIRST, before bench_publisher.py — the broker has no
persistence, so tasks published before any worker is connected are lost.

USAGE
-----
1. Start the broker:
       docker run -d -p 8080:8080 --name asyncml-broker ranjan05/async-broker-cpp

2. In one terminal, start this benchmark worker (leave it running):
       python3 bench_worker.py --workers 4 --out results.csv

3. In another terminal, run bench_publisher.py to send tasks.

4. Once the publisher finishes and the queue drains (watch this terminal's
   output slow down / stop), Ctrl+C this script, then run report.py.
"""
import argparse
import csv
import time
from ml_broker import AsyncBrokerClient


def make_callback(out_path):
    def callback(topic, data):
        recv_time = time.time()
        task_id = data.get("task_id")
        send_time = data.get("send_time")
        latency_ms = (recv_time - send_time) * 1000 if send_time is not None else None
        # Small appends (< PIPE_BUF, ~4KB on Linux) are atomic on POSIX,
        # so this is safe even with multiple worker processes writing
        # to the same file concurrently.
        with open(out_path, "a", newline="") as f:
            csv.writer(f).writerow([task_id, send_time, recv_time, latency_ms])
        print(f"[worker] task {task_id} handled in {latency_ms:.1f} ms" if latency_ms else f"[worker] task {task_id} handled")
    return callback


def main():
    parser = argparse.ArgumentParser(description="Benchmark worker for AsyncML Broker")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--workers", type=int, default=4, help="parallel worker processes")
    parser.add_argument("--out", default="results.csv")
    args = parser.parse_args()

    # Fresh results file with header each run
    with open(args.out, "w", newline="") as f:
        csv.writer(f).writerow(["task_id", "send_time", "recv_time", "latency_ms"])

    print(f"Starting {args.workers} benchmark worker process(es).")
    print(f"Writing per-task results to: {args.out}")
    print("Waiting for tasks... (Ctrl+C once the publisher is done and the queue has drained)\n")

    client = AsyncBrokerClient(host=args.host, port=args.port)
    client.start_workers(callback_fn=make_callback(args.out), num_workers=args.workers)


if __name__ == "__main__":
    main()