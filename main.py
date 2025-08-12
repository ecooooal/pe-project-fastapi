from typing import Union
from fastapi import FastAPI
from contextlib import asynccontextmanager
from redis import Redis
import os
import json
import redis.asyncio as redis  # use asyncio Redis client for async support

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")  # default localhost fallback

redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0)
process = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Add initial data on startup
    yield
    # Close Redis connection on shutdown
    await redis_client.close()

app = FastAPI(lifespan=lifespan)


@app.get("/")
async def read_root():
    # Non-blocking read of new messages (timeout=100 ms)
    value = await redis_client.xread(
        streams={"code_checker": "$"},
        block=0  # timeout in ms (adjust as needed)
    )
    
    if not value:
        return {"messages": []}

    # Example: decode and format messages
    results = []
    for stream_name, messages in value:
        for message_id, fields in messages:
            # Decode bytes to str for keys and values
            decoded_fields = {k.decode(): v.decode() for k, v in fields.items()}
            
            # Deserialize JSON in the 'data' field, if present
            if 'data' in decoded_fields:
                try:
                    decoded_fields = json.loads(decoded_fields['data'])
                except json.JSONDecodeError:
                    # handle error or leave as string
                    pass
            
            results.append({"id": message_id.decode(), "fields": decoded_fields})
    
    process.extend(results)

    return {"messages": results, "stream_names": stream_name}

@app.get("/items")
async def read_items():
    return process