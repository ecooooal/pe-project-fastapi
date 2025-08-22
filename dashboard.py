from fastapi import APIRouter
from app.redis_client import redis_client

router = APIRouter()

dashboard_registry = {
    "system": DashboardHandler("dashboard:system"),
    "exam": DashboardHandler("dashboard:exam"),
    "course": DashboardHandler("dashboard:course"),
}

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
def initial_load():
    # Check redis if it cached
    # if yes give that cached data
    # if not build it then give it
    # Data: Total exams count, published count, unpublished count, question group by exams, exams group by courses, exams to open this month
    return {"none" : 0}

# Course Section
@router.get("/load-course")
def initial_load():
    # Check redis if it cached
    # if yes give that cached data
    # if not build it then give it
    # Data: Question count, subject count, topic count, exam count for this course, unused question count, reused question count, question group by subject/topic, exam group by reused question
    return {"none" : 0}