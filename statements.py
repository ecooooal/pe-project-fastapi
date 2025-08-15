import os
import psycopg
import json
from datetime import datetime, timezone
from app.logger import logger
from app.redis_client import redis_client

DATABASE_URL = f"postgresql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"

STUDENT_CODE_ANSWER_UPDATE_HASH = "checked_code"
CODE_ANSWER_CHECKED = "checked"

def update_answer_points(coding_answer_id, result, answer_id):
    logger.info(f"🛠️ Entering update_answer_points for {coding_answer_id}")
    logger.info(f"Raw result: {result}")

    try:
        # Fields for updating CodingAnswer
        is_code_success = result['success']
        test_results = json.dumps(result['testResults'])
        failures = json.dumps(result['failures'])

        points = result['points'][0] 
        answer_syntax_points = points['syntax']
        answer_runtime_points = points['runtime']
        answer_test_case_points = points['testcase']

        coding_answer_update_query =    """
                                        UPDATE coding_answers
                                        SET status = %s,
                                            answer_syntax_points = %s,
                                            answer_runtime_points = %s,
                                            answer_test_case_points = %s,
                                            is_code_success = %s,
                                            test_results = %s,
                                            failures = %s
                                        WHERE id = %s
                                        """

        # Fields for updating StudentAnswer
        total_points = int(points['syntax']) + int(points['runtime']) + int(points['testcase'])
        is_answered= True
        is_correct = result['success']
        student_answer_update_query =   """
                                        UPDATE student_answers
                                        SET points = %s,
                                            is_answered = %s,
                                            is_correct = %s
                                        WHERE id = %s
                                        """

        status = 'checked'
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    coding_answer_update_query,
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
                cur.execute(
                   student_answer_update_query,
                    (
                        total_points,
                        is_answered,
                        is_correct,
                        answer_id
                    )
                )
                logger.info(f"✅ Updated coding_answer for ID {coding_answer_id} and answer_id for {answer_id}")

            conn.commit()
            redis_client.hset(STUDENT_CODE_ANSWER_UPDATE_HASH, coding_answer_id, CODE_ANSWER_CHECKED)

    except Exception as e:
        logger.exception(f"❌ update_answer_points failed for ID {coding_answer_id} because {e}")
     
def update_exam_record(student_paper_id):
    logger.info(f"🛠️ Updating Exam Record for paper {student_paper_id}")

    get_answers_by_subjects_query = """
                                    SELECT  subjects.id as id, 
                                            subjects.name as subject_name, 
                                            SUM(student_answers.points) as subject_score_obtained
                                    FROM student_answers
                                    JOIN questions ON student_answers.question_id = questions.id
                                    JOIN topics ON questions.topic_id = topics.id
                                    JOIN subjects ON topics.subject_id = subjects.id 
                                    WHERE student_answers.student_paper_id = %s
                                    GROUP BY subjects.id, subjects.name
                                    """
    
    get_exam_record_query = """
                            SELECT id
                            FROM exam_records
                            WHERE student_paper_id = %s
                            """
    
    update_exam_record_query = """
                                UPDATE exam_records
                                SET 
                                    total_score = %s, 
                                    status = %s
                                WHERE exam_records.id = %s
                                """
    
    get_exam_max_score_query =  """
                                SELECT exams.max_score
                                FROM exam_records
                                JOIN student_papers ON exam_records.student_paper_id = student_papers.id
                                JOIN exams ON student_papers.exam_id = exams.id
                                WHERE exam_records.id = %s
                                """

    update_subject_scores_query =   """
                                    UPDATE exam_records_subjects
                                    SET
                                        score_obtained = %s,
                                        updated_at = %s
                                    WHERE exam_record_id = %s
                                    AND subject_id = %s
                                    """
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                logger.info(f"Updating exam subject scores for student paper id: {student_paper_id}")
                cur.execute(get_exam_record_query, (student_paper_id,))
                row = cur.fetchone()
                if not row:
                    raise ValueError("No exam record found for student_paper_id")
                
                exam_record_id = row['id']

                cur.execute(get_exam_max_score_query, (exam_record_id,))
                row = cur.fetchone()
                if not row:
                    raise ValueError("No max score found for exam record")
                
                exam_max_score = row['max_score']

                cur.execute(get_answers_by_subjects_query, (student_paper_id,))
                rows = cur.fetchall()

                now = datetime.now(timezone.utc)
                transformed = [
                    {
                        'exam_record_id': exam_record_id,
                        'subject_id': row['id'],
                        'subject_score_obtained': row['subject_score_obtained'],
                        'updated_at': now
                    }
                    for row in rows
                ]   

                total_score = sum(row['subject_score_obtained'] for row in transformed)
                status = get_exam_record_status(total_score, exam_max_score)

                logger.info("ATTEMPTING TO UPDATE SUBJECT SCORES NOW")
                cur.executemany(update_subject_scores_query, [
                    (
                        row['subject_score_obtained'],
                        row['updated_at'],
                        row['exam_record_id'],
                        row['subject_id']
                    )
                    for row in transformed
                ])

                logger.info("ATTEMPTING TO UPDATE EXAM RECORD")
                cur.execute(update_exam_record_query, (total_score, status, exam_record_id))

            conn.commit()
            logger.info(f"✅ Updated exam_record for ID {exam_record_id}")

    except Exception as e:
        logger.exception(f"❌ update_exam_record failed for ID of student_paper {student_paper_id} because {e}")

def get_exam_record_status(score_obtained: int, max_score: int) -> str:
    if max_score == 0:
        return "more_review"  

    percentage = (score_obtained / max_score) * 100

    if percentage == 100:
        return "perfect_score"
    elif percentage >= 50:
        return "pass"
    else:
        return "more_review"
