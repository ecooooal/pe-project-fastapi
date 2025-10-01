import psycopg
from app.utils.database_config import DATABASE_URL
from app.utils.logger import logger

def get_exam_data():
    exam_data = {}
    
    get_student_performances_query = """
        SELECT *
        FROM student_performances;
    """

    try:
        return "a"

    except Exception as e:
        logger.exception(f"❌ Failed Getting Dashboard Exam Data because {e}")
    