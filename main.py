from typing import Union
from fastapi import FastAPI
from contextlib import asynccontextmanager
import json
import app.worker as worker
from app.utils.redis_client import redis_client
from app.dashboard import router

process = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Add initial data on startup
    worker.start_redis_worker()

    yield

    # Close Redis connection on shutdown
    await redis_client.close()

app = FastAPI(lifespan=lifespan)

app.include_router(router, prefix="/api/dashboard")


@app.get("/")
async def read_root():
    return 'yes'


@app.get("/items")
async def read_items():
    return 'yes'

@app.get("/sse")
async def send_signal():
    # strategy pattern
    # be dynamic to either send code signal or reports
    return process