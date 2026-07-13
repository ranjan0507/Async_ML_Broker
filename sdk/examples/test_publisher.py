from ml_broker.client import AsyncBrokerClient
import time

if __name__=="__main__":
	client = AsyncBrokerClient(host="127.0.0.1",port=8080)

	print("1. Sending standard text payload..")
	client.publish(
		topic="sentinel_flow_agent",
		data={"log_data":"USER_LOGIN_ATTEMPT: IP 192.168.1.50"}
	)

	time.sleep(0.5)

	print("2. Sending binary Payload")
	client.publish(
		topic="face_detect",
		data={
			"user":"Ranjan",
			"image": b'\xff\xd8\xff\xe0\x00\x10\x4a\x46\x49\x46'
		}
	)

	print("Done")