from ml_broker.client import AsyncBrokerClient
import base64
import time

def my_ml_router(topic,data):
	print(f"\n--- New Task Received on Topic : {topic}")
	if topic == "sentinel_flow_agent":
		log = data.get("log_data") 
		print(f"Analyzing log for threats : {log}")
		time.sleep(1)
		print("Result : Threat level low")
	elif topic == "face_detect":
		user = data.get("user")
		safe_string = data.get("image")
		raw_image_bytes = base64.b64decode(safe_string)

		print(f"Scanning face for user : {user}")
		print(f"Recovered image byted : {raw_image_bytes}")

		time.sleep(2)

		print(f"Access granted")
	
if __name__ == "__main__":
	client = AsyncBrokerClient(host="127.0.0.1",port=8080)

	client.start_workers(callback_fn=my_ml_router,num_workers=2)
