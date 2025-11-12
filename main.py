from typing import Union
from fastapi import FastAPI
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import json
import exam_taking_worker as worker
from utils.redis_client import redis_client
from dashboard import router, refresh_dashboard_job
from reports import router as reports_router

scheduler = BackgroundScheduler()
process = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Add initial data on startup
    worker.start_redis_worker()

    # Schedule the job every 10 minutes
    scheduler.add_job(refresh_dashboard_job, CronTrigger(minute='*/10'))
    scheduler.start()

    # Immediately run once on startup
    refresh_dashboard_job()
    yield

    # Close Redis connection on shutdown
    scheduler.shutdown()
    await redis_client.close()

app = FastAPI(lifespan=lifespan)

app.include_router(router, prefix="/api/dashboard")
app.include_router(reports_router, prefix="/api/reports")

@app.get("/")
async def read_root():
    return 'yes'


@app.get("/dockerhub")
async def test_CI():
    # strategy pattern
    # be dynamic to either send code signal or reports
    return 'I changed to test something'