import redis
import time
import requests
import json
import threading
import os
from app.logger import logger
from app.statements import update_answer_points
from app.redis_client import redis_client

LOG_SEPARATOR = "-" * 80

STREAM = "code_checker"
GROUP = "async_code_checker"
LANGUAGE_EXECUTOR_URLS = {
    # 'python': "http://python-api:8090/execute" Not implemented yet,
    'java': "http://java-api:8090/execute",
    # 'cpp': "http://cpp-api:8090/execute" Not implemented yet,
}


def process_user_code(coding_answer_id, fields):
    redis_client.hset("checked_code", coding_answer_id, "checking")

    language = fields['language']
    answer_id = fields['answer_id']
    logger.info(f"Extract coding_id and answer_id {coding_answer_id}: {answer_id}")
    data = fields['data']
    data['request_action'] = 'check'
    logger.info(f"Extract language and data {language}: {data}")

    try:
        logger.info("executing to language executor")
        logger.info(f"This is the fields to send in JSON {data}")

        language_executor_url = LANGUAGE_EXECUTOR_URLS.get(language)
        if not language_executor_url:
            raise ValueError(f"No executor URL defined for language: {language}")

        response = requests.post(language_executor_url, json=data)

        logger.info(LOG_SEPARATOR)
        logger.info(f"Checking Response : {response}")

        result = response.json()

        # Save result to DB
        logger.info(f"results : {result}")
        logger.info(LOG_SEPARATOR)

        update_answer_points(coding_answer_id, result, answer_id)

        # 5. Notify SSE
        # notifier.notify(str(coding_answer_id), result)

    except Exception as e:
        logger.exception(f"[Worker Error] Processing {coding_answer_id} failed")
        redis_client.hset("checked_code", coding_answer_id, "error")

def listen_forever():
    print("Worker started...")
    while True:
        try:
            user_submitted_codes = redis_client.xreadgroup(
                groupname=GROUP,
                consumername="worker-1",  
                streams={STREAM: ">"},   
                block=0
            )

            if user_submitted_codes:
                for stream, messages in user_submitted_codes:
                    for message_id, fields in messages:

                        try:
                            raw_json = fields[b'data'].decode()
                            fields_json = json.loads(raw_json)

                            for item in fields_json:
                                coding_answer_id = str(item["coding_answer_id"])
                                process_user_code(coding_answer_id, item)

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

