import redis
import time
import requests
import json
import threading
import os
from app.logger import logger
from app.statements import update_answer_points, update_exam_record
from app.redis_client import redis_client

LOG_SEPARATOR = "-" * 80

STUDENT_CODE_ANSWER_STREAM = "code_checker"
STUDENT_CODE_ANSWER_GROUP = "async_code_checker"
STUDENT_CODE_ANSWER_CONSUMER = "checker-1"

STUDENT_CODE_ANSWER_UPDATE_HASH = "checked_code"
CODE_ANSWER_CHECKING = "checking"
CODE_ANSWER_CHECKED = "checked"
CODE_ANSWER_ERROR = "error"

LANGUAGE_EXECUTOR_URLS = {
    # 'python': "http://python-api:8090/execute" Not implemented yet,
    'java': "http://java-api:8090/execute",
    # 'cpp': "http://cpp-api:8090/execute" Not implemented yet,
}


def process_user_code(coding_answer_id, fields):
    redis_client.hset(STUDENT_CODE_ANSWER_UPDATE_HASH, coding_answer_id, CODE_ANSWER_CHECKING)

    language = fields['language']
    answer_id = fields['answer_id']

    data = fields['data']
    data['request_action'] = 'check'

    try:
        logger.info(LOG_SEPARATOR)
        logger.info("Executing to language executor")
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

    except Exception as e:
        logger.exception(f"[Worker Error] Processing {coding_answer_id} failed")

def listen_forever():
    print("Worker started...")
    while True:
        try:
            user_submitted_codes = redis_client.xreadgroup(
                groupname=STUDENT_CODE_ANSWER_GROUP,
                consumername=STUDENT_CODE_ANSWER_CONSUMER,  
                streams={STUDENT_CODE_ANSWER_STREAM: ">"},   
                block=0
            )

            if user_submitted_codes:
                for stream, messages in user_submitted_codes:
                    for message_id, fields in messages:

                        try:
                            logger.info(LOG_SEPARATOR)
                            logger.info(f"CHECKING USER CODE")
                            raw_json = fields[b'data'].decode()
                            fields_json = json.loads(raw_json)
                            student_paper_id = fields_json[0]['student_paper_id']
                            logger.info(f"Checking paper {student_paper_id}")
                            
                            for item in fields_json:
                                coding_answer_id = str(item["coding_answer_id"])
                                process_user_code(coding_answer_id, item)
                            
                            redis_client.xack(STUDENT_CODE_ANSWER_STREAM, STUDENT_CODE_ANSWER_GROUP, message_id)
                            logger.info(f"DONE CHECKING USER CODE")

                            logger.info(LOG_SEPARATOR)
                            update_exam_record(student_paper_id)
                            logger.info(f"DONE UPDATING USER EXAM RECORD")

                        except Exception as e:
                           logger.error(f"[Error] Message {message_id}: {e}")

        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(1)   

def start_redis_worker():
    try:
        redis_client.xgroup_create(name=STUDENT_CODE_ANSWER_STREAM, groupname=STUDENT_CODE_ANSWER_GROUP, id='0', mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            raise
    thread = threading.Thread(target=listen_forever, daemon=True)
    thread.start()

