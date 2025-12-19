import os
import redis
import asyncio
import logging
import time
from datetime import datetime

logger = logging.getLogger("HFT_Utils")

# --- LUA Scripts ---
LUA_INC_LIMIT = """
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local current = tonumber(redis.call('get', key) or "0")
if current < limit then
    redis.call('incr', key)
    return current + 1
else
    return -1
end
"""

# Alias for backward compatibility if needed
LUA_CHECK_AND_INC = LUA_INC_LIMIT

class MockRedis:
    """Redis offline hone par migrations ko crash hone se bachata hai"""
    def get(self, *args, **kwargs): return None
    def set(self, *args, **kwargs): return True
    def delete(self, *args, **kwargs): return True
    def smembers(self, *args, **kwargs): return set()
    def sadd(self, *args, **kwargs): return True
    def hgetall(self, *args, **kwargs): return {}
    def hset(self, *args, **kwargs): return True
    def eval(self, *args, **kwargs): return 1
    def ping(self): return True

def get_redis_client():
    """
    Safe Redis connection handler.
    Agar Redis offline hai toh Mock object deta hai.
    """
    redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379')
    try:
        r = redis.from_url(redis_url, decode_responses=True, socket_timeout=1)
        r.ping()
        return r
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
        logger.warning("Redis connection refused. Using MockRedis for this session.")
        return MockRedis()

# Global Client for functions below
REDIS_CLIENT = get_redis_client()

def check_redis_limit(user_id, engine_label, limit):
    """Redis level par atomic limit check"""
    key = f"trading:limit:{user_id}:{engine_label}"
    # Use global client or get new one
    r = REDIS_CLIENT
    result = r.eval(LUA_INC_LIMIT, 1, key, limit)
    return result != -1

def log_performance(strategy, symbol, trigger_time):
    """HFT Latency monitor karne ke liye helper function"""
    latency = (time.perf_counter() - trigger_time) * 1000 # Milliseconds
    perf_key = f"perf:latency:{strategy}"
    REDIS_CLIENT.hset(perf_key, symbol, f"{latency:.2f}ms")
    logger.info(f"PERF | {strategy} | {symbol} | Latency: {latency:.2f}ms")
    return latency

async def db_sync_worker(queue):
    """Background worker jo trades ko Postgres mein save karega"""
    while True:
        trade_data = await queue.get()
        try:
            # Persistence logic placeholder
            pass
        except Exception as e:
            logger.error(f"DB Sync Error: {e}")
        finally:
            queue.task_done()