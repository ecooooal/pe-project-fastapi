from typing import Union
from fastapi import FastAPI
from contextlib import asynccontextmanager
import json
from app import worker
from app.redis_client import redis_client

process = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Add initial data on startup
    worker.start_redis_worker()

    yield

    # Close Redis connection on shutdown
    await redis_client.close()

app = FastAPI(lifespan=lifespan)

# app.include_router(dashboard.router, prefix="/api/dashboard")


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