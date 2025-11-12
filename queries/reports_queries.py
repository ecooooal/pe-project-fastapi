import psycopg
from utils.database_config import DATABASE_URL
from utils.logger import logger

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
    