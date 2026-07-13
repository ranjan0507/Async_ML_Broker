import socket
import json
import multiprocessing
import base64

class AsyncBrokerClient:
	def __init__(self , host="127.0.0.1" , port=8080):
		self.host = host
		self.port = port
	
	def publish(self , topic: str , data: dict):

		safe_data = {}
		for key , value in data.items():
			if isinstance(value,bytes):
				encoded_bytes = base64.b64encode(value)
				safe_data[key] = encoded_bytes.decode('utf-8')
			else:
				safe_data[key] = value

		payload = {"topic":topic , "data":safe_data}

		message_str = json.dumps(payload) + "\n"

		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
			sock.connect((self.host,self.port))
			sock.sendall(message_str.encode('utf-8'))

			ack = sock.recv(1024).decode('utf-8')
			if "ACK_ACCEPTED" not in ack:
				raise Exception("Broker failed to acknowledge the task")
			
	def _worker_loop(self,callback_fn):
		import os
		worker_pid = os.getpid()

		sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)

		try:
			sock.connect((self.host,self.port))
		except ConnectionRefusedError:
			print(f"[Worker PID: {worker_pid}] Failed to connect to C++ server.")
			return
		
		sock.sendall(b"WORKER_READY\n")

		print(f"[Worker PID: {worker_pid}] Connected and Idle. Waiting for tasks ..")

		buffer = ""

		while True:
			while "\n" not in buffer:
				try:
					chunk = sock.recv(1024).decode('utf-8')
					if not chunk:
						print(f"[Worker PID: {worker_pid}] Broker close connection.")
						return
					buffer+=chunk
				except ConnectionResetError:
					print(f"[Worker PID: {worker_pid}] Connection forcibly closed by broker.")
					return
				
			message , _ , buffer = buffer.partition("\n")

			try:
				task_dict = json.loads(message)
				topic = task_dict.get('topic')
				print(f"[Worker PID: {worker_pid}] Received task for topic : {topic}")

				callback_fn(topic,task_dict.get("data"))

				print(f"[Worker PID: {worker_pid}] Task complete . Re-assigning ...")
				sock.sendall(b"WORKER_READY\n") ;

			except json.JSONDecodeError:
				print(f"[Worker PID: {worker_pid}] Received malformed JSON from broker.")
			except Exception as e:
				print(f"[Worker PID: {worker_pid}] ML Callback crashed : {e}")
				sock.sendall(b"WORKER_READY\n")


	def start_workers(self, callback_fn , num_workers=1):
		print(f"[Master] Booting {num_workers} background ML worker ...")
		processes = []

		for _ in range(num_workers):
			p = multiprocessing.Process(target=self._worker_loop , args=(callback_fn,))
			p.daemon = True
			p.start()
			processes.append(p)

		print(f"[Master] All workers running .")
		try:
			for p in processes:
				p.join()
		except KeyboardInterrupt:
			print("\n[Master] Shutdown signal received . Terminating workers...")
			for p in processes:
				p.terminate()
				p.join()
			print("[Master] System Offline..")