from fastapi import APIRouter
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from app.utils.logger import logger
from app.utils.redis_client import redis_client

router = APIRouter()

# System Section 
@router.get("/reports-store")
def initial_load():
    # Check redis if it cached
    # if yes give that cached data
    # if not build it then give it
    # Data: Online users, online students, DB queries/sec, User request/sec, container health check, timeseries data
    
    return {"none" : 0}
