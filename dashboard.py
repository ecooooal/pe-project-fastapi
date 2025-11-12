from fastapi import APIRouter
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from handlers.dashboard_strategy import GetExamDashboardCache, GetCourseDashboardCache
from handlers.dashboard_interface import Context
from utils.logger import logger
from utils.redis_client import redis_client

router = APIRouter()
TIMER_KEY = 'dashboard:refresh_timer'
INTERVAL_SECONDS = 600  # 10 minutes

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

@router.get("/refresh")
def refresh_dasboard():
    exam_context = Context(GetExamDashboardCache())
    exam_context._strategy.refresh()
    course_context = Context(GetCourseDashboardCache())
    course_context._strategy.refresh()

    return "refreshed"


def refresh_dashboard_job():
    # Attempt to set the timer only if it doesn't exist
    success = redis_client.set(TIMER_KEY, 'running', ex=INTERVAL_SECONDS, nx=True)

    if success:
        print("✅ Dashboard refresh started.")
        exam_context = Context(GetExamDashboardCache())
        exam_context._strategy.refresh()
        course_context = Context(GetCourseDashboardCache())
        course_context._strategy.refresh()
        print(f"⏱️ Timer set. TTL is now {INTERVAL_SECONDS} seconds.")
    else:
        ttl = redis_client.ttl(TIMER_KEY)
        if ttl > 0:
            return(f"⏱️ {ttl} seconds left before next refresh.")


@router.get("/timer")
def get_timer_ttl():
    success = redis_client.set(TIMER_KEY, 'running', ex=INTERVAL_SECONDS, nx=True)

    if success:
        print("✅ Dashboard refresh started.")
        exam_context = Context(GetExamDashboardCache())
        exam_context._strategy.refresh()
        course_context = Context(GetCourseDashboardCache())
        course_context._strategy.refresh()
        print(f"⏱️ Timer set. TTL is now {INTERVAL_SECONDS} seconds.")
    else:
        ttl = redis_client.ttl(TIMER_KEY)
        if ttl > 0:
            return(f"⏱️ {ttl} seconds left before next refresh.")
