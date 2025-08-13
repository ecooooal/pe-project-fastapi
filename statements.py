import os
import psycopg
import json

DATABASE_URL = f"postgresql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"

def update_answer_points(answer_id, result):
  is_code_success = result['success']
  test_results = json.dumps(result['test_results'])
  failures = json.dumps(result['failures'])
  answer_syntax_points = result['points'][0]['syntax']
  answer_runtime_points = result['points'][0]['runtime']
  answer_test_case_points = result['points'][0]['testcase']
  status = 'checked'

  with psycopg.connect(DATABASE_URL) as conn:
    with conn.cursor() as cur:
      cur.execute(
            """
            UPDATE student_answers
            SET status = %s,
                answer_syntax_points = %s,
                answer_runtime_points = %s,
                answer_test_case_points = %s,
                is_code_success = %s,
                test_results = %s,
                failures = %s
            WHERE id = %s
            """,
            (status, answer_syntax_points, answer_runtime_points, answer_test_case_points, is_code_success, test_results, failures, answer_id)
        )
      conn.commit() # Commit the changes to the database