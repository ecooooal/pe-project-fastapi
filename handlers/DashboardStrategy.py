import os
import psycopg
from DashboardInterface import Context, Strategy
from utils.redis_client import redis_client
from statements import update_answer_points

DATABASE_URL = f"postgresql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"

CACHE_KEYS = {
    'system' : 'dashboard:system',
    'exam' : 'dashboard:exam',
    'course' : 'dashboard:course'
    }

class GetSystemDashboardCache(Strategy):
    def do_algorithm(self) -> dict:
        redis_key = CACHE_KEYS['system']
        if self.validate(redis_key):
            return self.get_cached_data(redis_key)
        else: 
            return self.build_key(redis_key)
    
    def build_key(self, redis_key):
        # fetch data to database
        query = """
            SELECT 
        """
        try:
            with psycopg.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    

                conn.commit()

        except Exception as e:
            logger.exception(f"❌ update_answer_points failed for ID {coding_answer_id} because {e}")
        # cache to redis and redis_key
        data = {}
        return data

class GetExamDashboardCache(Strategy):
    def do_algorithm(self) -> dict:
        redis_key = CACHE_KEYS['exam']
        if self.validate(redis_key):
            return self.get_cached_data(redis_key)
        else: 
            return self.build_key(redis_key)
    
    def build_key(self, redis_key):
        # fetch data to database
       
        # cache to redis and redis_key
        data = {}
        return data
    
class GetExamDashboardCache(Strategy):
    def do_algorithm(self) -> dict:
        # redis key = get redis key
        # if validate = true
        # then get_cached_data
        # return cached_data
        # else build_key
        # return cached_data
        return {'sample' :'data'}
    
    def build_key(self, redis_key):
        # fetch data to database
        # cache to redis and redis_key
        data = {}
        return data


if __name__ == "__main__":
    context = Context(GetMockSystemDashboardCache())

    key = "dashboard:user:123"

    print("\n--- First call (should build key) ---")
    context.do_some_business_logic(key)

    print("\n--- Second call (should hit cache) ---")
    context.do_some_business_logic(key)

# class DashboardHandler:
#     def __init__(self, redis_key):
#         self.redis_key = redis_key

#     def get_or_build(self):
#         cached_data = redis.get(self.redis_key)
#         if not cached_data or self._has_null_field(cached_data):
#             fresh_data = self.build_data()
#             redis.set(self.redis_key, fresh_data)
#             return fresh_data
#         return cached_data

#     def build_data(self):
#         # This is where you fetch from DB and construct the full object
#         # Example:
#         data = {
#             "online_users": query_online_users(),
#             "db_queries_per_sec": query_db_metrics(),
#             # ... other fields
#         }
#         return data

#     def _has_null_field(self, data):
#         return any(value is None for value in data.values())
