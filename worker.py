import redis
import time
import requests
import json
import threading
import os
from app.logger import logger


STREAM = "code_checker"
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")  # default localhost fallback

redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0)

def process_user_code(answer_id, fields):
    print(f"Processing message {answer_id}: {fields}")

    redis_client.hset("checked_code", answer_id, "checking")

    logger.info(f"Processing message {answer_id}: {fields}")

    try:
        logger.info("executing to each language executor")
        # 2. Send to language executor container
        response = requests.post("http://java-api:8090/execute", json=fields)
        logger.info(f"Checking Response : {response}")
        result = response.json()

        # 3. Save result to DB
        logger.info(f"results : {result}")
        # save_result(answer_id, result)

        # 4. Set status to "checked"
        redis_client.hset("checked_code", answer_id, "checked")

        # 5. Notify SSE
        # notifier.notify(str(answer_id), result)

    except Exception as e:
        print(f"[Worker Error] Processing {answer_id} failed: {e}")
        redis_client.hset("checked_code", answer_id, "error")
        # Optionally notify of error

def listen_forever():
    print("Worker started...")
    while True:
        try:
            user_submitted_codes = redis_client.xread(
                    streams={"code_checker": "$"},
                    block=0  
                )
            if user_submitted_codes:
                for stream, messages in user_submitted_codes:
                    for message_id, fields in messages:
                        try:
                            logger.info(f"Got message {message_id}: {fields}")
                            raw_json = fields[b'data'].decode()
                            fields_json = json.loads(raw_json)
                            for item in fields_json:
                                logger.info(f"item {item}")
                                answer_id = str(item["answer_id"])
                                process_user_code(answer_id, item)
                        except Exception as e:
                            print(f"[Parse Error] Message {message_id}: {e}")

                        # Acknowledge after processing
                        # redis_client.xack(STREAM, GROUP, message_id)


        except Exception as e:
            print(f"Error: {e}")
            time.sleep(1)   

def start_redis_worker():
    thread = threading.Thread(target=listen_forever, daemon=True)
    thread.start()

if __name__ == "__main__":
    listen_forever()
