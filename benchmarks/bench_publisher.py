#!/usr/bin/env python3
"""
Benchmark publisher for AsyncML Broker.

Publishes N tasks as fast as possible and measures how quickly the broker
ACKs them (publish-side throughput). Each task carries a timestamp so the
worker (bench_worker.py) can later compute end-to-end latency.

IMPORTANT: bench_worker.py must already be running and connected before
you run this — the broker has no persistence, so tasks published with no
idle worker available just sit in memory until one connects (or are lost
if the broker restarts).

USAGE
-----
    python3 bench_publisher.py --tasks 1000
"""
import argparse
import time
from ml_broker import AsyncBrokerClient


def main():
    parser = argparse.ArgumentParser(description="Benchmark publisher for AsyncML Broker")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--tasks", type=int, default=1000)
    args = parser.parse_args()

    client = AsyncBrokerClient(host=args.host, port=args.port)

    print(f"Publishing {args.tasks} tasks to {args.host}:{args.port} ...")
    start = time.time()
    for i in range(args.tasks):
        client.publish(topic="bench", data={"task_id": i, "send_time": time.time()})
    elapsed = time.time() - start

    print("\nPublish phase complete.")
    print(f"  Tasks published : {args.tasks}")
    print(f"  Wall time       : {elapsed:.2f}s")
    print(f"  Publish rate    : {args.tasks/elapsed:.1f} tasks/sec (broker ACK throughput)")
    print("\nThis measures how fast the broker accepts+ACKs tasks, NOT full")
    print("worker processing. Wait for the worker terminal to stop logging")
    print("new completions, then run: python3 report.py")


if __name__ == "__main__":
    main()