from fastapi import APIRouter
import polars as pl
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from utils.logger import logger
from utils.redis_client import redis_client
from utils.database_config import DATABASE_URL
from handlers.reports_interface import Context
from handlers.reports_strategy import CalculateExamOverview, CalculateExamDescriptiveStatistics, CalculateExamHistogramBoxplot, CalculateExamBySubjectsAndTopics, CalculateExamBYTypeWithLevels, CalculateExamQuestionHeatStrip
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
                    CalculateExamQuestionHeatStrip()
                ])
    
    exam_performance = context.do_business_logic()

    return exam_performance

@router.get("/example/{exam_id}")
def initial_load_create_store(exam_id : int):

    get_student_performances_query = f"""
        SELECT *
        FROM student_performances sp
        WHERE exam_id = {exam_id}
    """

    student_performance_df = pl.read_database_uri(query=get_student_performances_query, uri=DATABASE_URL)
    student_ids_list = student_performance_df.select('user_id').unique().to_series().to_list()

    get_student_statuses_query = f"""
        WITH RankedAttempts AS (
            SELECT
                er.attempt, er.status,
                sp.user_id,
                ROW_NUMBER() OVER (
                    PARTITION BY sp.user_id
                    ORDER BY er.attempt DESC
                ) as rn
            FROM
                exam_records er
            JOIN
                student_papers sp ON sp.id = er.student_paper_id
            WHERE
                sp.user_id = ANY (ARRAY{student_ids_list}) 
        )
        SELECT
            *
        FROM
            RankedAttempts
        WHERE
            rn = 1
    """
    student_statuses_df = pl.read_database_uri(query=get_student_statuses_query, uri=DATABASE_URL)
    student_pass_count = student_statuses_df.group_by('status').agg(pl.count().alias("count"))
    final_status_dict = dict(zip(
        student_pass_count.get_column("status").to_list(),
        student_pass_count.get_column("count").to_list()
    ))
    return final_status_dict
