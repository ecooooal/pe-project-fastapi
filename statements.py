import os
import psycopg
import json
from app.logger import logger
from app.redis_client import redis_client

DATABASE_URL = f"postgresql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"

def update_answer_points(coding_answer_id, result):
    logger.info(f"🛠️ Entering update_answer_points for {coding_answer_id}")
    logger.info(f"Raw result: {result}")

    try:
        is_code_success = result['success']
        test_results = json.dumps(result['testResults'])
        failures = json.dumps(result['failures'])

        points = result['points'][0] 

        answer_syntax_points = points['syntax']
        answer_runtime_points = points['runtime']
        answer_test_case_points = points['testcase']

        status = 'checked'
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE coding_answers
                    SET status = %s,
                        answer_syntax_points = %s,
                        answer_runtime_points = %s,
                        answer_test_case_points = %s,
                        is_code_success = %s,
                        test_results = %s,
                        failures = %s
                    WHERE id = %s
                    """,
                    (
                        status,
                        answer_syntax_points,
                        answer_runtime_points,
                        answer_test_case_points,
                        is_code_success,
                        test_results,
                        failures,
                        coding_answer_id
                    )
                )
                logger.info(f"✅ Updated student_answers for ID {coding_answer_id}")
            conn.commit()
            redis_client.hset("checked_code", coding_answer_id, "checked")
    except Exception as e:
        logger.exception(f"❌ update_answer_points failed for ID {coding_answer_id}")
     