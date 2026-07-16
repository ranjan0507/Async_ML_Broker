# AsyncML Broker

> **A high-performance, asynchronous task broker for ML inference workloads — built in C++ with a Python SDK.**

AsyncML Broker decouples your ML inference pipeline from your application code. A lightweight C++ server (the broker) sits between your application (publisher) and your ML model processes (workers), routing tasks asynchronously over raw TCP with zero external dependencies. No Kafka. No Redis. No RabbitMQ. Just sockets.

---

## Table of Contents

1. [Why AsyncML Broker?](#1-why-asyncml-broker)
2. [Architecture Overview](#2-architecture-overview)
3. [How It Works — Core Mechanics](#3-how-it-works--core-mechanics)
4. [Quick Start (5 Minutes)](#4-quick-start-5-minutes)
5. [Running the Broker — Docker](#5-running-the-broker--docker)
6. [Python SDK Guide](#6-python-sdk-guide)
   - [Installation](#61-installation)
   - [Publishing Tasks](#62-publishing-tasks)
   - [Running Workers](#63-running-workers)
   - [Binary Data Support](#64-binary-data-support)
7. [Full Integration Example — ML Project](#7-full-integration-example--ml-project)
8. [End-User Workflow](#8-end-user-workflow)
9. [Protocol Reference](#9-protocol-reference)
10. [Configuration](#10-configuration)
11. [Building the Broker from Source](#11-building-the-broker-from-source)
12. [Project Structure](#12-project-structure)
13. [Troubleshooting](#13-troubleshooting)

---

## 1. Why AsyncML Broker?

Running ML inference synchronously inside your application creates bottlenecks:

- Your web server **blocks** waiting for the model to respond.
- Model loading time hits every request.
- You cannot scale workers independently from your application.
- Heavy inference (image classification, NLP, etc.) starves lighter requests.

**AsyncML Broker solves this.** Your application fires a task and moves on immediately. The broker queues it and dispatches it to the next available ML worker process. Workers are persistent — the model loads once and stays hot in memory.

| Feature | AsyncML Broker |
|---|---|
| Language | C++ server core, Python SDK |
| Dependencies | **Zero** (stdlib only, both sides) |
| Transport | Raw TCP sockets |
| Concurrency model | epoll + dispatcher thread pool |
| Worker model | Persistent Python processes (model stays loaded) |
| Binary payload | ✅ (base64 encoded automatically) |
| Deployment | Docker image on Docker Hub |

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        YOUR APPLICATION                          │
│                                                                   │
│   publisher_script.py   ──►  client.publish("topic", data)      │
└───────────────────────────────────┬─────────────────────────────┘
                                    │  TCP: JSON newline-delimited
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│              C++ ASYNC BROKER  (ranjan05/async-broker-cpp)       │
│                                                                   │
│   ┌─────────────┐     ┌──────────────────────────────────────┐  │
│   │  epoll I/O  │────►│  task_queue  (std::queue<string>)    │  │
│   │  event loop │     └─────────────────┬────────────────────┘  │
│   └─────────────┘                       │                        │
│                          ┌──────────────▼───────────────────┐   │
│                          │   Dispatcher Thread Pool (×4)     │   │
│                          │   Picks task + idle worker,       │   │
│                          │   writes task over TCP            │   │
│                          └──────────────┬────────────────────┘   │
└─────────────────────────────────────────┼───────────────────────┘
                                          │  TCP: JSON task
                     ┌────────────────────▼──────────────────────┐
                     │           YOUR ML WORKER PROCESSES          │
                     │                                             │
                     │  worker.py  ──►  your_model_fn(topic,data) │
                     │  (Model loaded ONCE, stays hot in RAM)      │
                     └─────────────────────────────────────────────┘
```

**Three roles, three processes:**

| Role | What it does | Implemented by |
|---|---|---|
| **Broker** | Routes tasks from publishers to idle workers | C++ server (Docker) |
| **Publisher** | Sends tasks to the broker | Your app + SDK `publish()` |
| **Worker** | Receives tasks and runs your ML callback | Your app + SDK `start_workers()` |

---

## 3. How It Works — Core Mechanics

### 3.1 The C++ Broker

The broker (`server.cpp`) is a single-binary TCP server engineered for throughput and low latency.

**Startup sequence:**

1. **Dispatcher thread pool** is spawned first — 4 background threads that block on a condition variable, waiting for both a task and an idle worker to be available simultaneously.
2. A **TCP server socket** is created on port `8080` with `SO_REUSEADDR`.
3. The socket is placed in **non-blocking mode** via `fcntl`.
4. An **`epoll` instance** is created. The server socket is registered on it.
5. The broker enters the **main event loop** — a `epoll_wait` call that sleeps until I/O events arrive.

**When a connection arrives (new client — publisher or worker):**

- `accept()` is called, yielding a new file descriptor.
- The new socket is set non-blocking and registered on `epoll` with `EPOLLIN | EPOLLET` (edge-triggered mode).

**When data arrives on an existing socket:**

The broker reads in a loop (required by edge-triggered epoll) into a **per-client buffer** (`client_buffers[fd]`). It scans for newline (`\n`) characters — every complete newline-terminated message is one complete task or control message.

- If the message is `"WORKER_READY"` → the worker's file descriptor is pushed into the `idle_workers` queue and the dispatcher condition variable is notified.
- Any other message → the raw JSON string is pushed into `task_queue` and the dispatcher is notified. The broker immediately sends `"ACK_ACCEPTED\n"` back to the publisher.

**Dispatcher thread logic:**

Each dispatcher thread blocks on:
```cpp
broker_cv.wait(lock, []{ return !task_queue.empty() && !idle_workers.empty(); });
```
When both conditions are true, it atomically pops one task and one worker fd, then `write()`s the task JSON over the worker's TCP socket. If the write fails (worker died), the task is re-queued and the broker notifies again — **automatic task re-queuing on worker failure**.

### 3.2 The Python SDK Worker Loop

When `start_workers()` is called, it spawns N Python `multiprocessing.Process` instances. Each process runs `_worker_loop(callback_fn)` independently:

1. Opens a TCP connection to the broker.
2. Sends `b"WORKER_READY\n"` — registering itself as available.
3. Blocks in a `recv` loop, accumulating data in a string buffer until a `\n` is found.
4. Parses the JSON payload: `{"topic": "...", "data": {...}}`.
5. Calls `callback_fn(topic, data)` — **your ML inference code**.
6. After the callback returns, immediately sends `b"WORKER_READY\n"` again — re-registering as idle.

Because each worker is a separate **OS process**, they bypass Python's GIL entirely. N workers = N truly parallel model executions.

### 3.3 Binary Payload Handling

ML workloads often involve binary data (images, audio, model tensors). The SDK's `publish()` method automatically detects `bytes` values in the data dictionary and encodes them as **base64 strings** before JSON serialisation. The worker receives a base64 string and can decode it with `base64.b64decode()`.

---

## 4. Quick Start (5 Minutes)

**Prerequisites:** Docker, Python 3.8+

### Step 1 — Start the Broker

```bash
docker run -d -p 8080:8080 --name asyncml-broker ranjan05/async-broker-cpp
```

### Step 2 — Install the SDK

```bash
pip install asyncml-broker
```

### Step 3 — Write your Worker

```python
# worker.py
from ml_broker import AsyncBrokerClient

def my_inference(topic, data):
    print(f"Running inference on topic: {topic}")
    print(f"Data received: {data}")
    # Your model code goes here

if __name__ == "__main__":
    client = AsyncBrokerClient(host="127.0.0.1", port=8080)
    client.start_workers(callback_fn=my_inference, num_workers=2)
```

### Step 4 — Write your Publisher

```python
# publisher.py
from ml_broker import AsyncBrokerClient

client = AsyncBrokerClient(host="127.0.0.1", port=8080)
client.publish(topic="my_topic", data={"input": "hello world"})
print("Task submitted!")
```

### Step 5 — Run

```bash
# Terminal 1: Start workers (keep running)
python worker.py

# Terminal 2: Send tasks
python publisher.py
```

You'll see the worker pick up and process the task in real time.

---

## 5. Running the Broker — Docker

The broker is published as a Docker image on Docker Hub:

**Image:** `ranjan05/async-broker-cpp`

### 5.1 Basic Run

```bash
docker run -d \
  -p 8080:8080 \
  --name asyncml-broker \
  ranjan05/async-broker-cpp
```

| Flag | Purpose |
|---|---|
| `-d` | Run in detached (background) mode |
| `-p 8080:8080` | Map host port 8080 to container port 8080 |
| `--name asyncml-broker` | Give the container a friendly name |

### 5.2 Check Broker Logs

```bash
docker logs asyncml-broker
```

Expected output on successful boot:
```
[Broker] Spawned 4 background Dispatcher Threads
Booted successfully, listening on PORT 8080
```

When workers connect and tasks flow through, you'll see:
```
[Broker] New Worker Connected, FD: 5
[Gateway] Worker FD 5 registered as IDLE
[Gateway] Task received. Pushed to Queue
[Dispatcher 1] Successfully dispatched task to Worker FD 5
```

### 5.3 Stream Logs Live

```bash
docker logs -f asyncml-broker
```

### 5.4 Stop and Remove

```bash
docker stop asyncml-broker
docker rm asyncml-broker
```

### 5.5 Connecting from a Remote Host

If your workers or publishers run on a different machine than the broker, replace `127.0.0.1` with the broker host's IP or hostname:

```python
client = AsyncBrokerClient(host="192.168.1.100", port=8080)
```

Make sure port `8080` is open in any firewall rules.

### 5.6 Running on a Cloud Server

```bash
# On your cloud VM
docker pull ranjan05/async-broker-cpp
docker run -d -p 8080:8080 --restart unless-stopped ranjan05/async-broker-cpp
```

The `--restart unless-stopped` flag ensures the broker restarts automatically if the VM reboots.

---

## 6. Python SDK Guide

**Package:** `asyncml-broker`  
**PyPI:** https://pypi.org/project/asyncml-broker/

### 6.1 Installation

```bash
pip install asyncml-broker
```

No extra dependencies. The SDK uses only Python's standard library (`socket`, `json`, `multiprocessing`, `base64`).

### 6.2 The `AsyncBrokerClient` Class

```python
from ml_broker import AsyncBrokerClient

client = AsyncBrokerClient(host="127.0.0.1", port=8080)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `host` | `str` | `"127.0.0.1"` | IP/hostname of the running broker |
| `port` | `int` | `8080` | TCP port of the broker |

---

### 6.2 Publishing Tasks

```python
client.publish(topic: str, data: dict)
```

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `topic` | `str` | A string label that identifies the type of task. Your worker uses this to route to the right model. |
| `data` | `dict` | A JSON-serialisable dictionary containing the task payload. `bytes` values are automatically base64-encoded. |

**Behaviour:**
- Opens a TCP connection to the broker.
- Serialises the payload as `{"topic": topic, "data": data}` + newline.
- Sends the message and waits for `ACK_ACCEPTED` from the broker.
- Raises `Exception` if the broker does not acknowledge.
- Closes the connection immediately after acknowledgement. Each `publish()` call is a short-lived connection.

**Example:**

```python
from ml_broker import AsyncBrokerClient

client = AsyncBrokerClient(host="127.0.0.1", port=8080)

# Simple text payload
client.publish(
    topic="sentiment_analysis",
    data={"text": "This product is absolutely fantastic!"}
)

# Multiple fields
client.publish(
    topic="fraud_detection",
    data={
        "transaction_id": "TXN-9921",
        "amount": 4500.00,
        "merchant": "ACME Corp",
        "timestamp": "2026-07-16T10:30:00Z"
    }
)
```

**Return value:** `None`. The call returns as soon as the broker acknowledges receipt — it does **not** wait for the worker to finish processing.

---

### 6.3 Running Workers

```python
client.start_workers(callback_fn, num_workers=1)
```

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `callback_fn` | `callable` | Your ML inference function. Called with `(topic: str, data: dict)`. |
| `num_workers` | `int` | Number of parallel worker processes to spawn. Default: `1`. |

**Behaviour:**
- Spawns `num_workers` Python subprocesses.
- Each subprocess connects to the broker independently and registers as `WORKER_READY`.
- When a task arrives, the broker dispatches it to one idle worker.
- The worker calls `callback_fn(topic, data)`.
- After the callback returns, the worker re-registers as idle automatically.
- The main process blocks until all workers exit (i.e., it runs forever until `Ctrl+C`).
- `Ctrl+C` (SIGINT) triggers a clean shutdown — all worker processes are terminated gracefully.

**Writing your callback function:**

```python
def my_inference_router(topic: str, data: dict):
    if topic == "sentiment_analysis":
        text = data.get("text")
        # Run your NLP model
        result = sentiment_model.predict(text)
        print(f"Sentiment: {result}")

    elif topic == "fraud_detection":
        features = [data["amount"], ...]
        prediction = fraud_model.predict([features])
        print(f"Fraud score: {prediction[0]}")
```

> **Important:** Load your ML models at the **module level** (outside the callback function), not inside it. This ensures the model is loaded once per worker process when it starts, not on every task.

**Correct pattern:**

```python
import joblib

# Loaded ONCE when the worker process starts
model = joblib.load("my_model.pkl")

def my_inference(topic, data):
    result = model.predict([data["features"]])  # Fast — model already in RAM
    print(result)
```

---

### 6.4 Binary Data Support

The SDK handles binary payloads transparently.

**Publisher side:** Pass `bytes` values in the `data` dict. The SDK automatically base64-encodes them.

```python
with open("image.jpg", "rb") as f:
    image_bytes = f.read()

client.publish(
    topic="face_recognition",
    data={
        "user_id": "user_42",
        "image": image_bytes   # bytes — auto-encoded to base64
    }
)
```

**Worker side:** The value arrives as a base64 string. Decode it manually:

```python
import base64

def my_vision_model(topic, data):
    if topic == "face_recognition":
        b64_string = data.get("image")
        raw_bytes = base64.b64decode(b64_string)  # Back to original bytes
        # Pass raw_bytes to your image model
```

---

## 7. Full Integration Example — ML Project

This section demonstrates a complete, realistic integration using the examples from the SDK's `examples/` directory. Two topics are handled: log threat analysis and face recognition.

### 7.1 The Worker — `test_worker.py`

```python
# sdk/examples/test_worker.py
from ml_broker.client import AsyncBrokerClient
import base64
import time

def my_ml_router(topic, data):
    print(f"\n--- New Task Received on Topic: {topic}")

    if topic == "sentinel_flow_agent":
        # Simulates a log threat analysis model
        log = data.get("log_data")
        print(f"Analyzing log for threats: {log}")
        time.sleep(1)  # Simulates inference latency
        print("Result: Threat level LOW")

    elif topic == "face_detect":
        # Simulates a face recognition model with binary image input
        user = data.get("user")
        safe_string = data.get("image")
        raw_image_bytes = base64.b64decode(safe_string)  # Recover original bytes

        print(f"Scanning face for user: {user}")
        print(f"Recovered image bytes: {raw_image_bytes}")
        time.sleep(2)  # Simulates heavier vision model latency
        print("Access GRANTED")

if __name__ == "__main__":
    client = AsyncBrokerClient(host="127.0.0.1", port=8080)
    # Spawn 2 parallel worker processes
    client.start_workers(callback_fn=my_ml_router, num_workers=2)
```

### 7.2 The Publisher — `test_publisher.py`

```python
# sdk/examples/test_publisher.py
from ml_broker.client import AsyncBrokerClient
import time

if __name__ == "__main__":
    client = AsyncBrokerClient(host="127.0.0.1", port=8080)

    print("1. Sending standard text payload...")
    client.publish(
        topic="sentinel_flow_agent",
        data={"log_data": "USER_LOGIN_ATTEMPT: IP 192.168.1.50"}
    )

    time.sleep(0.5)

    print("2. Sending binary payload...")
    client.publish(
        topic="face_detect",
        data={
            "user": "Ranjan",
            "image": b'\xff\xd8\xff\xe0\x00\x10\x4a\x46\x49\x46'  # JPEG header bytes
        }
    )

    print("Done.")
```

### 7.3 Running the Full Example

```bash
# Step 1: Start the broker
docker run -d -p 8080:8080 --name asyncml-broker ranjan05/async-broker-cpp

# Step 2: Start workers (Terminal 1)
cd sdk/examples
python test_worker.py

# Step 3: Send tasks (Terminal 2)
cd sdk/examples
python test_publisher.py
```

### 7.4 Expected Console Output

**Worker terminal:**
```
[Master] Booting 2 background ML workers...
[Master] All workers running.
[Worker PID: 12341] Connected and Idle. Waiting for tasks...
[Worker PID: 12342] Connected and Idle. Waiting for tasks...

--- New Task Received on Topic: sentinel_flow_agent
Analyzing log for threats: USER_LOGIN_ATTEMPT: IP 192.168.1.50
Result: Threat level LOW
[Worker PID: 12341] Task complete. Re-assigning...

--- New Task Received on Topic: face_detect
Scanning face for user: Ranjan
Recovered image bytes: b'\xff\xd8\xff\xe0\x00\x10JFIF'
Access GRANTED
[Worker PID: 12342] Task complete. Re-assigning...
```

**Publisher terminal:**
```
1. Sending standard text payload...
2. Sending binary payload...
Done.
```

**Broker logs (`docker logs asyncml-broker`):**
```
[Broker] New Worker Connected, FD: 5
[Gateway] Worker FD 5 registered as IDLE
[Broker] New Worker Connected, FD: 6
[Gateway] Worker FD 6 registered as IDLE
[Gateway] Task received. Pushed to Queue
[Dispatcher 1] Successfully dispatched task to Worker FD 5
[Gateway] Task received. Pushed to Queue
[Dispatcher 2] Successfully dispatched task to Worker FD 6
```

---

## 8. End-User Workflow

Follow these steps every time you want to use AsyncML Broker in your project.

### Step 1 — Pull and Start the Broker

```bash
docker pull ranjan05/async-broker-cpp
docker run -d -p 8080:8080 --name asyncml-broker ranjan05/async-broker-cpp
```

Verify it is running:
```bash
docker logs asyncml-broker
# Expected: "Booted successfully, listening on PORT 8080"
```

### Step 2 — Install the SDK in your Python project

```bash
pip install asyncml-broker
```

### Step 3 — Create your Worker script

Create a file (e.g., `worker.py`) in your project. This script:
- Loads your ML models at the top level.
- Defines a callback function that receives `(topic, data)` and runs inference.
- Calls `client.start_workers()` to connect to the broker and wait for tasks.

```python
# worker.py
from ml_broker import AsyncBrokerClient
import joblib

# Load model once — stays in memory for all tasks
model = joblib.load("models/classifier.pkl")

def handle_task(topic: str, data: dict):
    if topic == "predict":
        features = data["features"]
        prediction = model.predict([features])[0]
        label = "positive" if prediction == 1 else "negative"
        print(f"[Worker] Prediction: {label}")

if __name__ == "__main__":
    client = AsyncBrokerClient(host="127.0.0.1", port=8080)
    client.start_workers(callback_fn=handle_task, num_workers=3)
```

### Step 4 — Create your Publisher script (or integrate into your app)

Anywhere in your application that needs to submit an ML task:

```python
from ml_broker import AsyncBrokerClient

broker = AsyncBrokerClient(host="127.0.0.1", port=8080)

# In your Flask/FastAPI/Django route, or anywhere in your code:
broker.publish(
    topic="predict",
    data={"features": [1.5, 2.3, 0.8, 4.1]}
)
# Returns immediately — does not wait for inference to complete
```

### Step 5 — Run Workers First, then Publish

```bash
# Terminal 1 — keeps running, holds the models in memory
python worker.py

# Terminal 2 (or your application) — send tasks as needed
python publisher.py
```

### Step 6 — Shutdown

Press `Ctrl+C` in the worker terminal. The SDK catches the signal and terminates all worker processes cleanly:
```
[Master] Shutdown signal received. Terminating workers...
[Master] System Offline.
```

Stop the broker when done:
```bash
docker stop asyncml-broker
```

---

## 9. Protocol Reference

The broker communicates using a simple **newline-delimited TCP protocol**. All messages are terminated with `\n`.

### Publisher → Broker

```json
{"topic": "your_topic", "data": {"key": "value"}}\n
```

### Broker → Publisher (acknowledgement)

```
ACK_ACCEPTED\n
```

### Worker → Broker (register as idle)

```
WORKER_READY\n
```

### Broker → Worker (task dispatch)

```json
{"topic": "your_topic", "data": {"key": "value"}}\n
```

The task payload sent to the worker is identical to the message received from the publisher — the broker passes it through unmodified.

---

## 10. Configuration

The broker currently uses compile-time constants defined at the top of `broker/src/server.cpp`:

| Constant | Default | Description |
|---|---|---|
| `PORT` | `8080` | TCP port the broker listens on |
| `MAX_EVENTS` | `1024` | Maximum simultaneous epoll events |
| `BUFFER_SIZE` | `1024` | Per-read buffer size in bytes |
| `NUM_DISPATCHER` | `4` | Number of dispatcher threads |

To change these, rebuild the Docker image from source (see Section 11) or set a different host port mapping when using Docker:

```bash
# Map host port 9090 to container port 8080
docker run -d -p 9090:8080 ranjan05/async-broker-cpp

# Then connect the SDK to port 9090
client = AsyncBrokerClient(host="127.0.0.1", port=9090)
```

---

## 11. Building the Broker from Source

If you want to customise the broker or run it natively (Linux only — uses Linux-specific `epoll` and `fcntl`):

### 11.1 Prerequisites

- GCC with C++11 or later (`g++`)
- Linux (Ubuntu, Debian, etc.)

### 11.2 Build

```bash
git clone https://github.com/ranjan0507/Async_ML_Broker.git
cd Async_ML_server/broker
make
```

The compiled binary is placed at `src/my_broker`.

### 11.3 Run Natively

```bash
./src/my_broker
```

### 11.4 Build the Docker Image Yourself

```bash
cd broker
docker build -t my-async-broker .
docker run -d -p 8080:8080 my-async-broker
```

The `Dockerfile` uses the official `gcc:latest` image, copies the source, runs `make`, and starts `./my_broker`.

---

## 12. Project Structure

```
Async_ML_server/
│
├── broker/                     # C++ Broker Server
│   ├── Dockerfile              # Docker build definition
│   ├── makefile                # Build rules (g++, -O3, -pthread)
│   └── src/
│       └── server.cpp          # Full broker implementation (~233 lines)
│
└── sdk/                        # Python SDK (asyncml-broker on PyPI)
    ├── setup.py                # Package metadata and build config
    ├── ml_broker/
    │   ├── __init__.py         # Exports AsyncBrokerClient
    │   └── client.py           # Full SDK implementation
    └── examples/
        ├── test_publisher.py   # Example: publishing tasks (text + binary)
        └── test_worker.py      # Example: ML worker with topic routing
```

---

## 13. Troubleshooting

### `ConnectionRefusedError` when running the worker or publisher

**Cause:** The broker is not running, or is not accessible on the specified host/port.

**Fix:**
```bash
# Check if the broker container is running
docker ps

# If not listed, start it
docker run -d -p 8080:8080 --name asyncml-broker ranjan05/async-broker-cpp

# Check logs for errors
docker logs asyncml-broker
```

---

### Worker says `"Broker closed connection"` immediately

**Cause:** The broker restarted or the container was stopped while the worker was running.

**Fix:** Restart the broker first, then restart the worker script.

---

### Tasks are published but the worker never receives them

**Cause:** Workers connected to the broker *after* tasks were published. The broker has no persistence — tasks published before any worker connects are held in the in-memory queue. If the broker restarts, queued tasks are lost.

**Fix:** Always start your worker script before publishing tasks. In production, keep workers running as long-lived processes.

---

### `Exception: Broker failed to acknowledge the task`

**Cause:** The broker closed the publisher's connection before sending `ACK_ACCEPTED`. This can happen if the broker is under extreme load or the message was malformed.

**Fix:** Check broker logs with `docker logs asyncml-broker` for error details. Ensure your `data` dict is JSON-serialisable (no non-encodable Python objects).

---

### Worker callback crashes on every task

**Cause:** An unhandled exception in your `callback_fn`. The SDK catches callback exceptions and logs them, then re-registers the worker as idle so it doesn't get stuck.

**Fix:** Add a `try/except` inside your callback for graceful error handling and logging. Check the worker terminal for `[Worker PID: ...] ML Callback crashed: ...` messages.

---

### Port 8080 already in use

```bash
# Use a different host port
docker run -d -p 9000:8080 --name asyncml-broker ranjan05/async-broker-cpp

# Update your SDK client
client = AsyncBrokerClient(host="127.0.0.1", port=9000)
```

---

### Binary data arrives garbled or as a plain string

**Cause:** You passed a `bytes` value correctly in `publish()`, but forgot to `base64.b64decode()` it in the worker.

**Fix:** In your worker callback:
```python
import base64
raw_bytes = base64.b64decode(data["my_bytes_field"])
```

---

## License

MIT License. See `LICENSE` for details.

---

*Built with a C++ async I/O core and a zero-dependency Python SDK.*
