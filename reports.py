from fastapi import APIRouter
import polars as pl
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from utils.logger import logger
from utils.redis_client import redis_client
from utils.database_config import DATABASE_URL
from handlers.reports_interface import Context
from handlers.reports_strategy import CalculateExamOverview, CalculateExamDescriptiveStatistics, CalculateExamHistogramBoxplot, CalculateExamBySubjectsAndTopics, CalculateExamBYTypeWithLevels, CalculateExamQuestionHeatStrip, CalculateIndividualQuestionAnalysis, CalculateIndividualStudentPerformance
from typing import Dict, Any, List

router = APIRouter()

# System Section 
@router.get("/create-store/{exam_id}")
def initial_load_create_store(exam_id : int):

    get_student_performances_query = f"""
        SELECT *
        FROM student_performances sp
        WHERE exam_id = {exam_id}
    """

    student_performance_df = pl.read_database_uri(query=get_student_performances_query, uri=DATABASE_URL)
    context = Context(
                df=student_performance_df, 
                strategies=[
                    CalculateExamOverview(),
                    CalculateExamDescriptiveStatistics(),
                    CalculateExamHistogramBoxplot(),
                    CalculateExamBySubjectsAndTopics(),
                    CalculateExamBYTypeWithLevels(),    
                    CalculateExamQuestionHeatStrip(),    
                    CalculateIndividualQuestionAnalysis(),    
                    CalculateIndividualStudentPerformance()
                ])
    
    exam_performance = context.do_business_logic()

    return exam_performance

@router.get("/example/{exam_id}")
def initial_load_create_store(exam_id : int):

    return exam_id
