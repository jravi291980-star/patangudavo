import os
import json
import redis
import asyncio
import threading
import logging
import time
from datetime import datetime as dt, time as dt_time
from math import floor
import pytz

# --- Django Environment Setup ---
# Isse standalone script Django models aur settings ko access kar sakta hai
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'algotrader.settings')
django.setup()

from django.db import close_old_connections
from django.conf import settings
from kiteconnect import KiteTicker, KiteConnect
from trading.models import Account, MomentumBullTrade, MomentumBearTrade
from trading.hft_utils import LUA_INC_LIMIT, get_redis_client

# --- Performance Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(name)s | %(message)s')
logger = logging.getLogger("Nexus_Momentum")
IST = pytz.timezone("Asia/Kolkata")

# Redis Connection (Heroku aur Local support)
r = redis.from_url(os.environ.get('REDIS_URL', 'redis://localhost:6379'), decode_responses=True)

# --- LUA Script: Race Condition Protection (Atomic Counter) ---
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

class MomentumNexus:
    def __init__(self, user_id):
        self.user_id = user_id
        # 1. Django DB se Account Details fetch karein
        self.acc = Account.objects.get(user__id=user_id)
        if not self.acc.access_token:
            raise Exception("Access Token missing! Dashboard se login kijiye.")
            
        self.kite = KiteConnect(api_key=self.acc.api_key)
        self.kite.set_access_token(self.acc.access_token)
        self.kws = KiteTicker(self.acc.api_key, self.acc.access_token)
        
        # --- ZERO-LATENCY RAM STATE (Pure Python Dictionaries) ---
        self.stocks = {}        # Universe (1700 Stocks)
        self.open_trades = {}   # Active positions monitoring {token: trade_data}
        self.config = {}        # Dashboard parameters cache
        self.banned_set = set()
        self.db_queue = asyncio.Queue()
        self.engine_live = {'mom_bull': True, 'mom_bear': True}
        
        self._load_morning_seeds()
        self._sync_dashboard_params()

    def _load_morning_seeds(self):
        """Morning cache tools ka data RAM mein load karna (O(1) Lookup)"""
        logger.info("Nexus 2: 1700 stocks range building ke liye taiyar hain...")
        
        instr_map = json.loads(r.get('instrument_map') or '{}')
        sma_map = r.hgetall('algo:fixed_vol_sma')
        universe = set(settings.STOCK_INDEX_MAPPING.keys())
        
        for symbol in universe:
            token_str = next((t for t, d in instr_map.items() if d['symbol'] == symbol), None)
            if not token_str: continue
            
            token = int(token_str)
            self.stocks[token] = {
                'symbol': symbol,
                'hi': 0.0, 'lo': 0.0,   # 9:15 - 9:16 Range values
                'sma': float(sma_map.get(symbol, 0)),
                'status': 'WAITING',   # WAITING -> EXECUTING -> OPEN -> CLOSED
                'stock_trades': 0,
                'last_ltp': 0.0
            }
        logger.info(f"Nexus 2: {len(self.stocks)} stocks cached successfully.")

    def _sync_dashboard_params(self):
        """Dashboard settings aur Ban list ko RAM mein refresh karna (har 2 sec)"""
        for side in ['mom_bull', 'mom_bear']:
            data = json.loads(r.get(f"algo:settings:{side}") or '{}')
            self.config[side] = {
                'max_total': int(data.get('max_trades', 3)),
                'sl_pct': float(data.get('stop_loss_pct', 0.5)) / 100.0,
                'rr': float(data.get('risk_reward', '1:2').split(':')[1]),
                'tsl': float(data.get('trailing_sl', '1:1.5').split(':')[1]),
                'risk_flat': float(data.get('risk_per_trade', 2000))
            }
        self.banned_set = r.smembers("algo:banned_symbols")
        self.engine_live['mom_bull'] = r.get("algo:engine:mom_bull:enabled") == "1"
        self.engine_live['mom_bear'] = r.get("algo:engine:mom_bear:enabled") == "1"

    def on_ticks(self, ws, ticks):
        """HOT PATH: High Frequency logic execution"""
        now = dt.now(IST)
        now_time = now.time()
        
        # UI Heartbeat update
        r.set("algo:mom:heartbeat", int(now.timestamp()), ex=10)

        # Timing Windows
        is_range_building = dt_time(9, 15) <= now_time < dt_time(9, 16)
        is_trade_active = now_time >= dt_time(9, 16)

        for tick in ticks:
            token = tick.get('instrument_token')
            stock = self.stocks.get(token)
            if not stock or stock['symbol'] in self.banned_set: continue

            ltp = tick['last_price']
            stock['last_ltp'] = ltp

            # --- STEP 1: EXIT MONITORING (PRIORITY 1) ---
            # 0.02% Buffer logic integrated
            if stock['status'] == 'OPEN':
                # Manual exit signal check
                if r.get(f"algo:manual_exit:{stock['symbol']}") == "1":
                    self._fire_hft_order(token, 'EXIT', ltp, "MANUAL_EXIT")
                    r.delete(f"algo:manual_exit:{stock['symbol']}")
                    continue
                self._monitor_momentum_exit(token, ltp)
                continue

            # --- STEP 2: RANGE BUILDING (9:15 - 9:16) ---
            if is_range_building:
                if stock['hi'] == 0 or ltp > stock['hi']: stock['hi'] = ltp
                if stock['lo'] == 0 or ltp < stock['lo']: stock['lo'] = ltp
                continue

            # --- STEP 3: MOMENTUM BREAKOUT (9:16+) ---
            # 0.01% Entry Buffer included
            if is_trade_active and stock['status'] == 'WAITING':
                # Bull Breakout: Range High + 0.01% Buffer
                if self.engine_live['mom_bull'] and ltp > (stock['hi'] * 1.0001):
                    self._fire_hft_order(token, 'BUY', ltp, "MOM_BULL_HIT")
                
                # Bear Breakdown: Range Low - 0.01% Buffer
                elif self.engine_live['mom_bear'] and ltp < (stock['lo'] * 0.9999):
                    self._fire_hft_order(token, 'SELL', ltp, "MOM_BEAR_HIT")

    def _monitor_momentum_exit(self, token, ltp):
        """Step-wise Trailing Monitoring with 0.02% Slippage Buffer"""
        trade = self.open_trades.get(token)
        if not trade: return

        if trade['side'] == 'BUY':
            # Target/SL Exit with 0.02% Buffer
            if ltp >= (trade['target'] * 1.0002) or ltp <= (trade['sl'] * 0.9998):
                self._fire_hft_order(token, 'SELL', ltp, "MOM_BULL_EXIT")
                return
            # Trailing Profit logic
            profit = ltp - trade['entry_px']
            if profit >= trade['step']:
                lvls = floor(profit / trade['step'])
                new_sl = trade['entry_px'] + ((lvls - 1) * trade['step'])
                if new_sl > trade['sl']: trade['sl'] = round(new_sl * 20) / 20
        
        elif trade['side'] == 'SELL':
            # Target/SL Exit with 0.02% Buffer
            if ltp <= (trade['target'] * 0.9998) or ltp >= (trade['sl'] * 1.0002):
                self._fire_hft_order(token, 'BUY', ltp, "MOM_BEAR_EXIT")
                return
            # Trailing logic
            profit = trade['entry_px'] - ltp
            if profit >= trade['step']:
                lvls = floor(profit / trade['step'])
                new_sl = trade['entry_px'] - ((lvls - 1) * trade['step'])
                if new_sl < trade['sl']: trade['sl'] = round(new_sl * 20) / 20

    def _fire_hft_order(self, token, side, price, reason):
        """Atomic Guard + Non-blocking Order Fire"""
        stock = self.stocks[token]
        # Memory latching (Ghost entry protection)
        stock['status'] = 'EXECUTING'
        asyncio.run_coroutine_threadsafe(self._async_kite_execute(token, side, price, reason), self.loop)

    async def _async_kite_execute(self, token, side, price, reason):
        stock = self.stocks[token]
        label = 'mom_bull' if (side == 'BUY' or 'BULL' in reason) else 'mom_bear'
        
        try:
            # 1. Global LUA Guard (Race condition protection across processes)
            if side in ['BUY', 'SELL']:
                if r.eval(LUA_INC_LIMIT, 1, f"hft:count:{self.user_id}:{label}", self.config[label]['max_total']) == -1:
                    stock['status'] = 'WAITING'; return

            # 2. Risk based Quantity Calculation
            risk_per_share = price * self.config[label]['sl_pct']
            qty = max(1, int(floor(self.config[label]['risk_flat'] / risk_per_share))) if risk_per_share > 0 else 1

            # 3. Kite MIS Market Order
            oid = self.kite.place_order(
                tradingsymbol=stock['symbol'], exchange='NSE', transaction_type=side,
                quantity=qty, order_type='MARKET', product='MIS', variety='regular'
            )

            # --- 4. ACTUAL EXECUTION PRICE SYNC (No Slippage Target/SL) ---
            actual_entry = price # Fallback
            for _ in range(5):
                await asyncio.sleep(0.1) # 100ms interval polling
                hist = self.kite.order_history(oid)
                if hist[-1]['status'] == 'COMPLETE':
                    actual_entry = float(hist[-1]['average_price'])
                    break
            
            # 5. Success Memory Update for RAM Monitoring
            if side in ['BUY', 'SELL']:
                stock['status'] = 'OPEN'
                # Target/SL hamesha demat price (actual_entry) se calculate honge
                risk = actual_entry * self.config[label]['sl_pct']
                self.open_trades[token] = {
                    'side': side, 'entry_px': actual_entry, 'qty': qty, 'oid': oid,
                    'sl': actual_entry - risk if side == 'BUY' else actual_entry + risk,
                    'target': actual_entry + (risk * self.config[label]['rr']) if side == 'BUY' else actual_entry - (risk * self.config[label]['rr']),
                    'step': risk * self.config[label]['tsl']
                }
            else:
                stock['status'] = 'CLOSED'
                self.open_trades.pop(token, None)

            # 6. Background Queue Update for Database
            await self.db_queue.put({'token': token, 'side': side, 'price': actual_entry, 'oid': oid, 'reason': reason})
            logger.info(f"MOMENTUM {side} OK: {stock['symbol']} @ {actual_entry}")

        except Exception as e:
            logger.error(f"Kite API Error: {e}"); stock['status'] = 'WAITING'

    async def broker_integrity_watcher(self):
        """Broker aur Terminal sync (Har 15 sec polling)"""
        while True:
            try:
                await asyncio.sleep(15)
                kite_orders = self.kite.orders()
                kite_map = {o['tradingsymbol']: o['status'] for o in kite_orders if o['product'] == 'MIS'}

                close_old_connections()
                for t in list(self.open_trades.keys()):
                    sym = self.stocks[t]['symbol']
                    # Agar manually terminal par trade band kar diya gaya ho
                    if sym in kite_map and kite_map[sym] in ['REJECTED', 'CANCELLED']:
                        logger.warning(f"Integrity Purge: {sym} stopped at broker.")
                        self.open_trades.pop(t, None); self.stocks[t]['status'] = 'WAITING'
            except: pass

    async def settings_poller(self):
        """Dashboard settings sync har 2 sec mein"""
        while True:
            self._sync_dashboard_params()
            await asyncio.sleep(2)

    def run(self):
        self.loop = asyncio.new_event_loop()
        threading.Thread(target=self.loop.run_forever, daemon=True).start()
        # Start Workers
        asyncio.run_coroutine_threadsafe(self.settings_poller(), self.loop)
        asyncio.run_coroutine_threadsafe(self.broker_integrity_watcher(), self.loop)
        # Kite Ticker Connection
        self.kws.on_ticks = self.on_ticks
        self.kws.connect(threaded=True)

if __name__ == "__main__":
    MomentumNexus(user_id=1).run()