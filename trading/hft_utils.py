import os
import redis
import asyncio
import logging
import time
import json
from datetime import datetime
from django.db import close_old_connections

logger = logging.getLogger("HFT_Utils")

# --- LUA Script for Atomic Trade Limiting ---
# Isse ensure hota hai ki HFT speed par bhi "Max Trades" limit cross na ho.
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

class MockRedis:
    """Redis offline hone par ya migrations ke waqt crash hone se bachata hai."""
    def __init__(self): self.is_mock = True
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
    Heroku SSL Compatible Redis Client.
    Production mein 'rediss://' URL aur SSL checks bypass handle karta hai.
    """
    redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379')
    
    try:
        if redis_url.startswith('rediss://'):
            # Heroku Redis requires SSL but uses self-signed certs
            r = redis.from_url(
                redis_url, 
                decode_responses=True, 
                ssl_cert_reqs=None 
            )
        else:
            r = redis.from_url(redis_url, decode_responses=True)
            
        r.ping()
        return r
    except Exception as e:
        logger.warning(f"Redis Connection Failed ({e}). Using MockRedis.")
        return MockRedis()

# Global Instance
r_client = get_redis_client()

def log_performance(strategy, symbol, trigger_time):
    """Execution latency measure karne ke liye."""
    latency = (time.perf_counter() - trigger_time) * 1000 
    r_client.hset(f"perf:latency:{strategy}", symbol, f"{latency:.2f}ms")
    logger.info(f"LATENCY | {strategy} | {symbol} | {latency:.2f}ms")

async def hft_db_sync_worker(queue):
    """
    Nexus Engines se trades lekar Postgres Database mein save karne wala worker.
    Isse HFT execution engine par load nahi padta.
    """
    from trading.models import (
        CashBreakoutTrade, CashBreakdownTrade, 
        MomentumBullTrade, MomentumBearTrade
    )
    from django.contrib.auth.models import User

    logger.info("DB Sync Worker Started...")
    user = User.objects.get(id=1) # Default Master User

    while True:
        data = await queue.get()
        try:
            # Django connection refresh (Heroku DB safety)
            close_old_connections()
            
            reason = data.get('reason', '')
            side = data.get('side')
            
            # Model mapping logic
            model = None
            if 'BULL_BREAKOUT' in reason: model = CashBreakoutTrade
            elif 'BEAR_BREAKDOWN' in reason: model = CashBreakdownTrade
            elif 'MOM_BULL' in reason: model = MomentumBullTrade
            elif 'MOM_BEAR' in reason: model = MomentumBearTrade

            if model and side in ['BUY', 'SELL']:
                # New entry creation
                model.objects.create(
                    user=user,
                    symbol=data['symbol'],
                    entry_price=data['price'],
                    qty=data['qty'],
                    order_id=data['oid'],
                    status='OPEN'
                )
            elif model and side in ['EXIT', 'SELL', 'BUY'] and 'EXIT' in reason:
                # Update existing trade status
                trade = model.objects.filter(user=user, symbol=data['symbol'], status='OPEN').last()
                if trade:
                    trade.status = 'EXITED'
                    trade.exit_price = data['price']
                    # PnL automatically calculate logic (optional)
                    trade.save()

        except Exception as e:
            logger.error(f"DB Worker Persistence Error: {e}")
        finally:
            queue.task_done()