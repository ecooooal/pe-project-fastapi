import os
import psycopg
import json
from collections import defaultdict
from utils.logger import logger

DATABASE_URL = f"postgresql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"

# system queries

# exam queries

def refresh_exam_data():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_matviews WHERE matviewname = 'exam_stats_mv'
                    ) THEN
                        CREATE MATERIALIZED VIEW exam_stats_mv AS
                        SELECT
                                e.id AS exam_id,
                                e.name AS exam_name,
                                c.id AS course_id,
                                c.name AS course_name,
                                c.abbreviation AS course_abbreviation,
                                e.is_published,
                                COALESCE(TO_CHAR(e.examination_date, 'YYYY-MM-DD'), 'No Examination Date') AS examination_date,
                                COALESCE(COUNT(eq.question_id), 0) AS question_count
                                FROM exams e
                                LEFT JOIN exam_question eq ON eq.exam_id = e.id
                                JOIN course_exam ce ON ce.exam_id = e.id 
                                JOIN courses c ON c.id = ce.course_id
                                GROUP BY e.id, c.id, c.name, c.abbreviation, e.is_published, e.examination_date;
                    ELSE
                        REFRESH MATERIALIZED VIEW exam_stats_mv;
                    END IF;
                END
                $$;
                """)
            conn.commit()  
        logger.info("✅ Successfully refreshed exam_stats_mv materialized view.")
    except Exception as e:
        logger.exception(f"❌ Failed Refreshing Dashboard Exam Data because {e}")

def get_exam_data():
    exam_data = {}
    
    exam_count_query = """
        SELECT COUNT(DISTINCT exam_id) AS exam_count FROM exam_stats_mv;
    """
    published_count_query = """
        SELECT COUNT(DISTINCT exam_id) AS published_count FROM exam_stats_mv WHERE is_published  = true;
    """
    unpublished_count_query = """
        SELECT COUNT(DISTINCT exam_id) AS unpublished_count FROM exam_stats_mv WHERE is_published  = false;
    """
    examination_date_query =  """
        SELECT DISTINCT exam_id, exam_name, examination_date
        FROM exam_stats_mv
        WHERE examination_date != 'No Examination Date';
    """ 
    question_by_exam_query ="""
        SELECT DISTINCT exam_id, exam_name, is_published, examination_date, question_count
        FROM exam_stats_mv
        WHERE question_count > 0
        ORDER BY question_count ASC;
    """
    exam_by_course_query ="""
        SELECT course_id, course_name, course_abbreviation, COUNT(exam_id) AS exam_count
        FROM exam_stats_mv
        GROUP BY course_id, course_name, course_abbreviation
        ORDER BY exam_count ASC;
    """
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                exam_data['exam_count'] = fetch_single_value(cur, exam_count_query)

                exam_data['published_count'] = fetch_single_value(cur, published_count_query)

                exam_data['unpublished_count'] = fetch_single_value(cur, unpublished_count_query)

                cur.execute(examination_date_query)
                exam_data['examination_dates'] = [list(row) for row in cur.fetchall()]

                cur.execute(question_by_exam_query)
                exam_data['question_exams'] = [list(row) for row in cur.fetchall()]
                
                cur.execute(exam_by_course_query)
                exam_data['exam_courses'] = [list(row) for row in cur.fetchall()]
                
        return exam_data

    except Exception as e:
        logger.exception(f"❌ Failed Getting Dashboard Exam Data because {e}")
    
# course queries
def get_all_course_id():
    course_ids = []
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT course_id FROM course_subject_topic_question_mv")
                course_ids = [row[0] for row in cur.fetchall()]
            return course_ids
        logger.info("✅ Successfully refreshed course_subject_topic_question_mv materialized view.")
    except Exception as e:
        logger.exception(f"❌ Failed Refreshing Dashboard Course Data because {e}")

def refresh_course_data():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                DO $$
                BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_matviews WHERE matviewname = 'course_subject_topic_question_mv'
                ) THEN
                CREATE MATERIALIZED VIEW course_subject_topic_question_mv AS
                    SELECT DISTINCT
                        c.id               AS course_id,
                        c.abbreviation     AS course_abbreviation,
                        s.id               AS subject_id,
                        s.name             AS subject_name,
                        t.id               AS topic_id,
                        t.name             AS topic_name,
                        q.id               AS question_id,
                        q.question_type    AS question_type,
                        q.name             AS question_name,
                        STRING_AGG(DISTINCT tag.name, ', ') FILTER (
                            WHERE taggable.taggable_type = 'App\Models\Question' 
                            AND taggable.type = 'required'
                        ) AS question_level,
                        STRING_AGG(DISTINCT tag.name, ', ') FILTER (
                            WHERE taggable.taggable_type = 'App\Models\Question'
                            AND taggable.type = 'optional'
                        ) AS optional_tags

                    FROM courses c
                    JOIN course_subject cs ON cs.course_id = c.id
                    JOIN subjects s ON s.id = cs.subject_id
                    JOIN topics t ON t.subject_id = s.id
                    JOIN questions q ON q.topic_id = t.id

                    LEFT JOIN taggables taggable 
                        ON taggable.taggable_id = q.id 
                        AND taggable.taggable_type = 'App\Models\Question'
                    LEFT JOIN tags tag 
                        ON tag.id = taggable.tag_id

                    GROUP BY
                        c.id, c.abbreviation,
                        s.id, s.name,
                        t.id, t.name,
                        q.id, q.question_type, q.name;

                CREATE INDEX idx_mv_course_id ON course_subject_topic_question_mv (course_id);
                CREATE INDEX idx_mv_course_subject ON course_subject_topic_question_mv (course_id, subject_id);
                CREATE INDEX idx_mv_course_topic ON course_subject_topic_question_mv (course_id, topic_id);
                CREATE INDEX idx_mv_course_question_type ON course_subject_topic_question_mv (course_id, question_type);
                ELSE
                    REFRESH MATERIALIZED VIEW course_subject_topic_question_mv;
                END IF;
                END
                $$;
                """)
            conn.commit()  
        logger.info("✅ Successfully refreshed course_subject_topic_question_mv materialized view.")
    except Exception as e:
        logger.exception(f"❌ Failed Refreshing Dashboard Course Data because {e}")

def get_course_data(course_id):
    course_data = {}

    question_count_query = """
        SELECT COUNT(question_id) AS question_count
        FROM course_subject_topic_question_mv
        WHERE course_id = %s;
    """
    subject_query = """
        SELECT 
            subject_id, 
            subject_name, 
            COUNT(question_id) AS question_count,
            (SELECT COUNT(DISTINCT subject_id)
                FROM course_subject_topic_question_mv
                WHERE course_id = %s) AS subject_count
        FROM course_subject_topic_question_mv
        WHERE course_id = %s
        GROUP BY subject_id, subject_name
        ORDER BY question_count;
    """
    topic_query = """
        SELECT 
            topic_id, 
            topic_name, 
            COUNT(question_id) AS question_count,
            (SELECT COUNT(DISTINCT topic_id)
                FROM course_subject_topic_question_mv
                WHERE course_id = %s) AS topic_count
        FROM course_subject_topic_question_mv
        WHERE course_id = %s
        GROUP BY topic_id, topic_name
        ORDER BY question_count;
    """
    question_type_query = """
        select question_type, COUNT(question_id) AS question_count
        from course_subject_topic_question_mv
        WHERE course_id = %s
        group by question_type
        ORDER BY question_count;
    """

    exam_count_query =  """
        SELECT COUNT(DISTINCT mv.exam_id) AS exam_count
        FROM exam_stats_mv mv
        JOIN course_exam ce ON ce.exam_id = mv.exam_id
        WHERE ce.course_id = %s;
    """ 
    unused_questions_query ="""
        SELECT COUNT(q.id) AS unused_questions
        FROM questions q
        JOIN course_subject_topic_question_mv mv
        ON mv.question_id = q.id
        WHERE mv.course_id = %s
        AND NOT EXISTS (
                SELECT 1
                FROM exam_question eq
                JOIN exams e ON eq.exam_id = e.id
                JOIN course_exam ce ON ce.exam_id = eq.exam_id
                WHERE eq.question_id = q.id
                AND ce.course_id = %s
        );
    """
    reused_questions_query ="""
        SELECT 
            q.id AS question_id,
            q.name AS question_name,
            q.question_type,
            mv.question_level,
            
            CASE 
                WHEN question_status.exam_count IS NULL THEN 'unused'
                WHEN question_status.exam_count = 1 THEN 'used'
                ELSE 'reused'
            END AS status

        FROM questions q

        JOIN course_subject_topic_question_mv mv ON mv.question_id = q.id
        LEFT JOIN (
            SELECT eq.question_id, COUNT(DISTINCT eq.exam_id) AS exam_count
            FROM exam_question eq
            GROUP BY eq.question_id
        ) question_status ON question_status.question_id = q.id

        WHERE mv.course_id = %s;
    """

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                course_data['question_count'] = fetch_single_value(cur, question_count_query, (course_id,))
                
                cur.execute(subject_query, (course_id, course_id))
                subject_rows = cur.fetchall()
                course_data['subject_count'] = len(subject_rows)
                course_data['subject_table'] = subject_rows

                cur.execute(topic_query, (course_id, course_id))
                topic_rows = cur.fetchall()
                course_data['topic_count'] = len(topic_rows)
                course_data['topic_table'] = topic_rows

                cur.execute(question_type_query, (course_id,))
                question_type_rows = cur.fetchall()
                course_data['question_type_count'] = len(question_type_rows)
                course_data['question_type_table'] = question_type_rows

                course_data['exam_count'] = fetch_single_value(cur, exam_count_query, (course_id,))

                course_data['unused_question_count'] = fetch_single_value(cur, unused_questions_query, (course_id, course_id))

                cur.execute(reused_questions_query, (course_id,))
                reused_questions_rows = cur.fetchall()
                course_data['reused_question_count'] = len(reused_questions_rows)
                course_data['reused_questions'] = reused_questions_rows

        return course_data

    except Exception as e:
        logger.exception(f"❌ Failed Getting Dashboard Course Data because {e}")
 

def fetch_single_value(cur, query, params=()):
    cur.execute(query, params or ())
    result = cur.fetchone()
    return result[0] if result else 0