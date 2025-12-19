import os
import redis
import asyncio
import logging
import time
from datetime import datetime
from django.db import connections

# Heroku REDIS_URL use karein
redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379')
REDIS_CLIENT = redis.from_url(redis_url, decode_responses=True)

# LUA Script for atomic trade limits
LUA_CHECK_AND_INC = """
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

async def db_sync_worker(queue):
    """
    Background worker jo trades ko Postgres mein save karega bina tick loop ko roke.
    """
    while True:
        trade_data = await queue.get()
        try:
            # Trade persistence logic yaha aayegi
            pass
        except Exception as e:
            logging.error(f"DB Sync Error: {e}")
        finally:
            queue.task_done()

def check_redis_limit(user_id, engine_label, limit):
    """
    Redis level par atomic limit check.
    """
    key = f"trading:limit:{user_id}:{engine_label}"
    result = REDIS_CLIENT.eval(LUA_CHECK_AND_INC, 1, key, limit)
    return result != -1

def log_performance(strategy, symbol, trigger_time):
    """
    HFT Latency monitor karne ke liye helper function.
    """
    latency = (time.perf_counter() - trigger_time) * 1000 # Milliseconds mein
    # Redis mein latest latency save karein taaki dashboard pe dikha sakein
    perf_key = f"perf:latency:{strategy}"
    REDIS_CLIENT.hset(perf_key, symbol, f"{latency:.2f}ms")
    logging.info(f"PERF | {strategy} | {symbol} | Latency: {latency:.2f}ms")
    return latency