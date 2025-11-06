from fastapi import APIRouter
import polars as pl
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from app.utils.logger import logger
from app.utils.redis_client import redis_client
from app.utils.database_config import DATABASE_URL
from app.handlers.reports_interface import Context
from app.handlers.reports_strategy import CalculateExamOverview, CalculateExamDescriptiveStatistics, CalculateExamHistogramBoxplot, CalculateExamBySubjectsAndTopics, CalculateExamBYTypeWithLevels, CalculateExamQuestionHeatStrip
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
    get_max_attempts = (
        student_performance_df
        .group_by("user_id")
        .agg(
            pl.col("attempt").max().alias("latest_attempt")
        )
    )

    df_with_max = student_performance_df.join(get_max_attempts, on="user_id", how="left")

    df = (
        df_with_max
        .filter(pl.col("attempt") == pl.col("latest_attempt"))
        .drop("latest_attempt")
    )

    df_long = (
        df
        .group_by('course_abbreviation', 'question_type', 'question_level')
        .agg(
            pl.col('points_obtained').sum().alias('raw_score_sum'),
            pl.col('question_points').sum().alias('max_score_sum'),
        )
        .sort(['question_type', 'question_level', 'course_abbreviation'])
    )

    # --- B. Overall QType Aggregation (Only QType Raw Totals) ---
    df_qtype_totals = (
        df_long
        .group_by('question_type')
        .agg(
            pl.col('raw_score_sum').sum().alias('qtype_total_raw_score'),
            pl.col('max_score_sum').sum().alias('qtype_total_max_score'),
        )
    )
    df_combined = (
        df_long
        .join(df_qtype_totals, on='question_type', how='left')
        .with_columns(
            # Normalized Score (Accuracy of this level)
            pl.when(pl.col('max_score_sum') > 0)
            .then(pl.col('raw_score_sum') / pl.col('max_score_sum') * 100)
            .otherwise(pl.lit(0.0))
            .alias('accuracy_percentage'), # Renamed for clarity vs contribution
            
            # Contribution Percentage (The new required metric)
            pl.when(pl.col('qtype_total_raw_score') > 0)
            .then(pl.col('raw_score_sum') / pl.col('qtype_total_raw_score') * 100)
            .otherwise(pl.lit(0.0))
            .alias('contribution_percentage')
        )
    )
    return df_combined.to_dicts()
