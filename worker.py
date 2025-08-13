import redis
import time
import requests
import json
import threading
import os
from app.logger import logger
from app.statements import update_answer_points

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0)

LOG_SEPARATOR = "-" * 80

STREAM = "code_checker"
GROUP = "async_code_checker"



def process_user_code(answer_id, fields):
    redis_client.hset("checked_code", answer_id, "checking")

    language = fields['language']
    data = fields['data']
    data['request_action'] = 'check'
    logger.info(f"Extract language and data {language}: {data}")

    try:
        logger.info("executing to language executor")
        logger.info(f"This is the fields to send in JSON {data}")

        response = requests.post("http://java-api:8090/execute", json=data)

        logger.info(LOG_SEPARATOR)
        logger.info(f"Checking Response : {response}")

        result = response.json()

        # Save result to DB
        logger.info(f"results : {result}")
        logger.info(LOG_SEPARATOR)

        update_answer_points(answer_id, result)
        
        redis_client.hset("checked_code", answer_id, "checked")

        # 5. Notify SSE
        # notifier.notify(str(answer_id), result)

    except Exception as e:
        print(f"[Worker Error] Processing {answer_id} failed: {e}")
        redis_client.hset("checked_code", answer_id, "error")

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
                            raw_json = fields[b'data'].decode()
                            fields_json = json.loads(raw_json)

                            for item in fields_json:
                                answer_id = str(item["answer_id"])
                                process_user_code(answer_id, item)

                        except Exception as e:
                            print(f"[Parse Error] Message {message_id}: {e}")

                        redis_client.xack(STREAM, GROUP, message_id)


        except Exception as e:
            print(f"Error: {e}")
            time.sleep(1)   

def start_redis_worker():
    try:
        redis_client.xgroup_create(name=STREAM, groupname=GROUP, id='0', mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            raise
    thread = threading.Thread(target=listen_forever, daemon=True)
    thread.start()

if __name__ == "__main__":
    listen_forever()
