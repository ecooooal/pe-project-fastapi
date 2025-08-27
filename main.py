from typing import Union
from fastapi import FastAPI
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import json
import app.worker as worker
from app.utils.redis_client import redis_client
from app.dashboard import router, refresh_dashboard_job

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


@app.get("/")
async def read_root():
    return 'yes'


@app.get("/sse")
async def send_signal():
    # strategy pattern
    # be dynamic to either send code signal or reports
    return process