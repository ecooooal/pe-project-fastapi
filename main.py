from typing import Union
from fastapi import FastAPI
from contextlib import asynccontextmanager
from redis import Redis
import os
import json
import redis.asyncio as redis  # use asyncio Redis client for async support
from app import worker
from app.logger import logger

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")  
redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0)

process = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Add initial data on startup
    worker.start_redis_worker()
    yield
    # Close Redis connection on shutdown
    await redis_client.close()

app = FastAPI(lifespan=lifespan)


@app.get("/")
async def read_root():
    value = await redis_client.xread(
        streams={"code_checker": "$"},
        block=0  
    )
    
    if not value:
        return {"messages": []}

    results = []
    for stream_name, messages in value:
        for message_id, fields in messages:
            decoded_fields = {k.decode(): v.decode() for k, v in fields.items()}
            
            if 'data' in decoded_fields:
                try:
                    decoded_fields = json.loads(decoded_fields['data'])
                except json.JSONDecodeError:
                    pass
            
            results.append({"id": message_id.decode(), "fields": decoded_fields})
    
    process.extend(results)

    return {"messages": results, "stream_names": stream_name}

@app.get("/items")
async def read_items():
    return process

@app.get("/sse")
async def send_signal():
    # strategy pattern
    # be dynamic to either send code signal or reports
    return process