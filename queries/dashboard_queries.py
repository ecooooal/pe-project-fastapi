import os
import psycopg
import json
from collections import defaultdict
from utils.logger import logger

DATABASE_URL = f"postgresql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"

# system queries

# exam queries

def get_exam_data():
    exam_data = {}
    
    exam_count_query = """
        SELECT COUNT(id) FROM exams;
    """
    published_count_query = """
        SELECT COUNT(id) FROM exams WHERE status = 'published';
    """
    unpublished_count_query = """
        SELECT COUNT(id) FROM exams WHERE status = 'unpublished';
    """
    examination_date_query =  """
                SELECT id, TO_CHAR(examination_date, 'YYYY-MM-DD') AS examination_date
                FROM exams
                WHERE examination_date >= date_trunc('month', CURRENT_DATE)
                AND examination_date < date_trunc('month', CURRENT_DATE + INTERVAL '1 month');
    """ 
    question_by_exam_query ="""
                SELECT exam_id, COUNT(question_id) AS question_count
                FROM exam_answers
                GROUP BY exam_id
                ORDER BY question_count ASC;
    """
    exam_by_course_query ="""
                SELECT c.id AS course, COUNT(e.id) AS exam_count
                FROM exams e
                JOIN courses c ON e.course_id = c.id
                GROUP BY c.id
                ORDER BY exam_count ASC;
    """
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                exam_data['exam_count'] = fetch_single_value(cur, exam_count_query)

                exam_data['published_count'] = fetch_single_value(cur, published_count_query)

                exam_data['unpublished_count'] = fetch_single_value(cur, unpublished_count_query)

                cur.execute(examination_date_query)
                exam_data['examination_dates'] = cur.fetchall()

                cur.execute(question_by_exam_query)
                exam_data['question_exams'] = cur.fetchall()
                
                cur.execute(exam_by_course_query)
                exam_data['exam_courses'] = cur.fetchall()
                
        return exam_data

    except Exception as e:
        logger.exception(f"❌ Failed Getting Dashboard Exam Data because {e}")
        exam_data['error'] = f"Getting exam data failed because {e}"
        return exam_data
    
def get_course_data(course_id):
    course_data = {}

    question_count_query = """
                SELECT COUNT(q.id)
                FROM courses c
                JOIN subjects s ON s.course_id = c.id
                JOIN topics t ON t.subject_id = s.id
                JOIN questions q ON q.topic_id = t.id
                WHERE c.id = %s;
    """
    subject_count_query = """
                SELECT COUNT(s.id)
                FROM courses c
                JOIN subjects s ON s.course_id = c.id
                WHERE c.id = %s;
    """
    topic_count_query = """
                SELECT COUNT(t.id)
                FROM courses c
                JOIN subjects s ON s.course_id = c.id
                JOIN topics t ON t.subject_id = s.id
                WHERE c.id = %s;
    """
    exam_count_query =  """
                SELECT COUNT(e.id)
                FROM courses c
                JOIN exams e ON e.course_id = c.id
                WHERE c.id = %s;
    """ 
    unused_questions_query ="""
                SELECT COUNT(q.id) AS unused_questions
                FROM questions q
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM exam_questions eq
                    JOIN exams e ON eq.exam_id = e.id
                    WHERE eq.question_id = q.id
                    AND e.course_id = %s
                );
    """
    reused_questions_query ="""
                SELECT q.id, q.question_text, q.question_type, COUNT(*) AS reused_count
                FROM exam_questions eq
                JOIN exams e ON eq.exam_id = e.id
                JOIN questions q ON eq.question_id = q.id
                WHERE e.course_id = %s
                GROUP BY q.id, q.question_text, q.question_type
                HAVING COUNT(*) >= 2;
    """
    graph_query = """
                SELECT 
                    s.id AS subject_id,
                    s.name AS subject_name,
                    t.id AS topic_id,
                    t.name AS topic_name,
                    q.id AS question_id,
                    q.question_type
                FROM courses c
                JOIN subjects s ON s.course_id = c.id
                JOIN topics t ON t.subject_id = s.id
                JOIN questions q ON q.topic_id = t.id
                WHERE c.id = %s;
    """
    # Group by subject
    questions_by_subject = defaultdict(list)

    # Group by topic
    questions_by_topic = defaultdict(list)

    # Group by question_type
    questions_by_type = defaultdict(list)
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                course_data['question_count'] = fetch_single_value(cur, question_count_query, (course_id,))

                course_data['subject_count'] = fetch_single_value(cur, subject_count_query, (course_id,))

                course_data['topic_count'] = fetch_single_value(cur, topic_count_query, (course_id,))

                course_data['exam_count'] = fetch_single_value(cur, exam_count_query, (course_id,))

                course_data['unused_question_count'] = fetch_single_value(cur, unused_questions_query, (course_id,))

                cur.execute(reused_questions_query, (course_id,))
                reused_questions_rows = cur.fetchall()
                course_data['reused_question_count'] = len(reused_questions_rows)
                course_data['reused_questions'] = reused_questions_rows

                cur.execute(graph_query, (course_id,))
                rows = cur.fetchall()
                for row in rows:
                    subject_name = row[1]
                    topic_name = row[3]
                    question_id = row[4]
                    question_type = row[5]

                    questions_by_subject[subject_name].append(question_id)
                    questions_by_topic[topic_name].append(question_id)
                    questions_by_type[question_type].append(question_id)

                course_data['questions_by_subject'] = dict(questions_by_subject)
                course_data['questions_by_topic'] = dict(questions_by_topic)
                course_data['questions_by_type'] = dict(questions_by_type)

                
        return course_data

    except Exception as e:
        logger.exception(f"❌ Failed Getting Dashboard Course Data because {e}")
        course_data['error'] = f"Getting course data failed because {e}"
        return course_data

def fetch_single_value(cur, query, params):
    cur.execute(query, params or ())
    result = cur.fetchone()
    return result[0] if result else 0