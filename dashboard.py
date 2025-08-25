from fastapi import APIRouter
from app.handlers.dashboard_strategy import GetExamDashboardCache, GetCourseDashboardCache
from app.handlers.dashboard_interface import Context
from app.utils.logger import logger

router = APIRouter()

# Initial Load

# System Section 
@router.get("/load-system")
def initial_load():
    # Check redis if it cached
    # if yes give that cached data
    # if not build it then give it
    # Data: Online users, online students, DB queries/sec, User request/sec, container health check, timeseries data
    
    return {"none" : 0}

# Exam Section
@router.get("/load-exam")
def initial_load_exam():
    # Check redis if it cached
    # if yes give that cached data
    # if not build it then give it
    # Data: Total exams count, published count, unpublished count, question group by exams, exams group by courses, exams to open this month
    context = Context(GetExamDashboardCache())
    
    return context.do_business_logic()

# Course Section
@router.get("/load-course/{course_id}")
def initial_load_course(course_id: int):
    # Check redis if it cached
    # if yes give that cached data
    # if not build it then give it
    # Data: Question count, subject count, topic count, exam count for this course, unused question count, reused question count, question group by subject/topic, exam group by reused question
    context = Context(GetCourseDashboardCache(), course_id)

    return context.do_business_logic()