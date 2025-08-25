import os
import json
from app.handlers.dashboard_interface import Context, Strategy
from app.utils.redis_client import redis_client
from app.queries.dashboard_queries import get_exam_data, get_course_data
from app.utils.logger import logger

DATABASE_URL = f"postgresql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"

CACHE_KEYS = {
    'system' : 'dashboard:system',
    'exam' : 'dashboard:exam',
    'course' : 'dashboard:course'
    }

class GetSystemDashboardCache(Strategy):
    def __init__(self):
        self.id_context = None

    def do_algorithm(self) -> dict:
        redis_key = CACHE_KEYS['system']
        if self.validate(redis_key):
            pass
        else: 
            pass
    
    def build_key(self, redis_key):
        data = {}
        return data

class GetExamDashboardCache(Strategy):
    def __init__(self):
        self.id_context = None

    def do_algorithm(self) -> dict:
        redis_key = CACHE_KEYS['exam']
        if self.validate(redis_key):
            return self.get_cached_data(redis_key)
        else: 
            return self.build_key(redis_key)
    
    def validate(self, redis_key) -> bool:
        return redis_client.exists(redis_key) > 0

    def build_key(self, redis_key):
        # fetch data to database
        exam_data = get_exam_data()

        # cache to redis and redis_key
        redis_ready_data = json.dumps(exam_data)
        redis_client.set(redis_key, redis_ready_data)

        return exam_data
    
    def get_cached_data(self, redis_key):
        cached_data = json.loads(redis_client.get(redis_key))

        print(cached_data)
        return cached_data
        
class GetCourseDashboardCache(Strategy):
    def __init__(self):
        self.id_context = None  # Add this attribute

    def do_algorithm(self) -> dict:
        redis_key = CACHE_KEYS['course']
        if self.validate(redis_key):
            return self.get_cached_data(redis_key)
        else: 
            return self.build_key(redis_key)
        
    def validate(self, redis_key) -> bool:
        field = f"course:{self.id_context}"
        return redis_client.hexists(redis_key, field)

    def build_key(self, redis_key):
        # fetch data to database
        course_data = get_course_data(self.id_context)

        # cache to redis and redis_key
        redis_ready_data = json.dumps(course_data)
        redis_client.hset(redis_key, f"course:{self.id_context}", redis_ready_data)

        return course_data
    
    def get_cached_data(self, redis_key):
        field = f"course:{self.id_context}"
        value = redis_client.hget(redis_key, field)
        print(value)
        if value:
            return json.loads(value)
        return None

def prepare_for_redis_hash(data: dict) -> dict:
    redis_data = {}
    for key, value in data.items():
        if isinstance(value, (list, dict)):
            redis_data[key] = json.dumps(value)
        else:
            redis_data[key] = str(value)
    return redis_data
