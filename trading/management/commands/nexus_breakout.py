import os
import json
import redis
import asyncio
import threading
import logging
import time
from datetime import datetime as dt, timedelta
from math import floor
import pytz

# --- Django Environment Setup ---
# Isse hum Django Models aur Settings ko standalone script mein use kar sakte hain
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'algotrader.settings')
django.setup()

from django.db import close_old_connections
from django.conf import settings
from kiteconnect import KiteTicker, KiteConnect
from trading.models import Account, CashBreakoutTrade, CashBreakdownTrade
from trading.hft_utils import LUA_INC_LIMIT, get_redis_client

# --- Performance Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(name)s | %(message)s')
logger = logging.getLogger("Nexus_Breakout")
IST = pytz.timezone("Asia/Kolkata")

# Redis Connection (Heroku aur Local dono ke liye)
r = redis.from_url(os.environ.get('REDIS_URL', 'redis://localhost:6379'), decode_responses=True)

# --- LUA Script: Race Condition Protection (Atomic Trade Counter) ---
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

class BreakoutNexus:
    def __init__(self, user_id):
        self.user_id = user_id
        # 1. Django DB se Account Details fetch karna
        self.acc = Account.objects.get(user__id=user_id)
        if not self.acc.access_token:
            raise Exception("Access Token missing! Dashboard se login kijiye.")
            
        self.kite = KiteConnect(api_key=self.acc.api_key)
        self.kite.set_access_token(self.acc.access_token)
        self.kws = KiteTicker(self.acc.api_key, self.acc.access_token)
        
        # --- ZERO-LATENCY RAM STATE (Pure Python Dictionaries) ---
        self.stocks = {}        # 1700 Stocks ka data aur candle state
        self.open_trades = {}   # Active positions monitoring {token: trade_data}
        self.config = {}        # Dashboard parameters (RR, TSL, Volume Matrix)
        self.db_queue = asyncio.Queue()
        self.banned_set = set()
        self.engine_live = {'bull': True, 'bear': True}
        
        self._load_morning_cache()
        self._sync_dashboard_params()

    def _load_morning_cache(self):
        """Pre-loading all seeds (PDL, SMA, Instruments) into RAM"""
        logger.info("Nexus 1: RAM Cache build ho raha hai...")
        
        instr_map = json.loads(r.get('instrument_map') or '{}')
        pdl_map = r.hgetall('prev_day_ohlc')
        sma_map = r.hgetall('algo:fixed_vol_sma')
        universe = set(settings.STOCK_INDEX_MAPPING.keys())
        
        for symbol in universe:
            token_str = next((t for t, d in instr_map.items() if d['symbol'] == symbol), None)
            if not token_str: continue
            
            token = int(token_str)
            pdl_raw = pdl_map.get(symbol)
            if not pdl_raw: continue
            pdl = json.loads(pdl_raw)

            # Building HFT RAM Object per stock
            self.stocks[token] = {
                'symbol': symbol,
                'prev_high': float(pdl['high']),
                'prev_low': float(pdl['low']),
                'sma': float(sma_map.get(symbol, 0)),
                'status': 'PENDING',    # PENDING -> TRIGGER_WATCH -> EXECUTING -> OPEN
                'trigger_px': 0.0,      # Entry level with 0.01% Buffer
                'trigger_at': None,     # Candle close time (6-min timer start)
                'side_latch': None,     # BULL ya BEAR
                'stop_base': 0.0,       # Initial SL calculation reference
                'stock_trades': 0,      # Per-stock counter
                'candle': None,         # Current minute aggregator
                'last_vol': 0,
                'last_ltp': 0
            }
        logger.info(f"Nexus 1: {len(self.stocks)} stocks cached successfully.")

    def _sync_dashboard_params(self):
        """Dashboard settings aur Ban symbols ko RAM mein refresh karna (har 2 sec)"""
        for side in ['bull', 'bear']:
            data = json.loads(r.get(f"algo:settings:{side}") or '{}')
            self.config[side] = {
                'max_total': int(data.get('total_trades', 5)),
                'max_per_stock': int(data.get('trades_per_stock', 2)),
                'rr': float(data.get('risk_reward', '1:2').split(':')[1]),
                'tsl': float(data.get('trailing_sl', '1:1.5').split(':')[1]),
                'vol_matrix': data.get('volume_criteria', []), # 10 Levels
                'risk_tiers': [
                    float(data.get('risk_trade_1', 2000)),
                    float(data.get('risk_trade_2', 1500)),
                    float(data.get('risk_trade_3', 1000))
                ]
            }
        self.banned_set = r.smembers("algo:banned_symbols")
        self.engine_live['bull'] = r.get("algo:engine:bull:enabled") == "1"
        self.engine_live['bear'] = r.get("algo:engine:bear:enabled") == "1"

    def _is_vol_qualified(self, token, candle, side):
        """REARRANGED CALCULATION: Sabse saste checks pehle, mehenge baad mein (CPU Optimization)"""
        stock = self.stocks[token]
        matrix = self.config[side]['vol_matrix']
        c_vol = candle['volume']
        c_close = candle['close']
        s_sma = stock['sma']

        for level in matrix:
            # 1. Sabse Sasta Check: Minimum Average SMA (Simple Float Comparison)
            if s_sma < float(level.get('min_sma_avg', 0)):
                continue 
            
            # 2. Spike Check: SMA Multiplier (One Multiplication)
            if c_vol < (s_sma * float(level.get('sma_multiplier', 1))):
                continue

            # 3. Sabse Mehenga Check: Volume Price in Crores (Multiply + Divide)
            vol_cr = (c_vol * c_close) / 10000000.0
            if vol_cr >= float(level.get('min_vol_price_cr', 0)):
                return True
        return False

    def on_ticks(self, ws, ticks):
        """HOT PATH: Sabse fast execution ke liye logic ka order badla gaya hai"""
        now = dt.now(IST)
        bucket = now.replace(second=0, microsecond=0)
        r.set("algo:data:heartbeat", int(now.timestamp()), ex=10)

        for tick in ticks:
            token = tick.get('instrument_token')
            stock = self.stocks.get(token)
            if not stock or stock['symbol'] in self.banned_set: continue

            ltp = tick['last_price']
            vol = tick.get('volume_traded', 0)
            stock['last_ltp'] = ltp

            # --- STEP 1: EXIT MONITORING (0.02% Buffer Logic) ---
            if stock['status'] == 'OPEN':
                # Dashboard manual exit signal check
                if r.get(f"algo:manual_exit:{stock['symbol']}") == "1":
                    self._fire_hft_order(token, 'EXIT', ltp, 0, "MANUAL_EXIT")
                    r.delete(f"algo:manual_exit:{stock['symbol']}")
                    continue
                # Auto-Exit aur Step-wise Trailing
                self._manage_exit_and_tsl(token, ltp)
                continue

            # --- STEP 2: 6-MINUTE BREAKOUT MONITORING (Trigger Watch) ---
            if stock['status'] == 'TRIGGER_WATCH':
                # Check timer: Kya 6 minute beet gaye?
                if (now - stock['trigger_at']).total_seconds() > 360:
                    logger.info(f"Timer Expired: {stock['symbol']} reset to PENDING.")
                    stock['status'] = 'PENDING'; stock['trigger_px'] = 0.0
                    continue

                # Trigger Condition: 0.01% Buffer already included in trigger_px
                if stock['side_latch'] == 'BULL' and ltp > stock['trigger_px']:
                    self._fire_hft_order(token, 'BUY', ltp, stock['stop_base'], "BULL_TRIGGER_HIT")
                elif stock['side_latch'] == 'BEAR' and ltp < stock['trigger_px']:
                    self._fire_hft_order(token, 'SELL', ltp, stock['stop_base'], "BEAR_TRIGGER_HIT")
                continue

            # --- STEP 3: CANDLE AGGREGATION & BREAKOUT CANDLE DETECTION ---
            if stock['candle'] and stock['candle']['bucket'] != bucket:
                # Boundary hit: Completed candle check karo
                self._check_for_signal(token, stock['candle'])
                stock['candle'] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': 0}
            elif not stock['candle']:
                stock['candle'] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': 0}
            else:
                c = stock['candle']
                c['high'] = max(c['high'], ltp); c['low'] = min(c['low'], ltp); c['close'] = ltp
                if stock['last_vol'] > 0: c['volume'] += max(0, vol - stock['last_vol'])
            
            stock['last_vol'] = vol

    def _check_for_signal(self, token, candle):
        """Breakout candle detect karke stock ko TRIGGER_WATCH mode mein daalna"""
        stock = self.stocks[token]
        if stock['status'] != 'PENDING': return

        # BULL Check: Prev High ke upar close aur qualified volume
        if self.engine_live['bull'] and candle['open'] < stock['prev_high'] < candle['close']:
            if self._is_vol_qualified(token, candle, 'bull'):
                if stock['stock_trades'] < self.config['bull']['max_per_stock']:
                    stock['status'] = 'TRIGGER_WATCH'; stock['side_latch'] = 'BULL'
                    stock['trigger_px'] = candle['high'] * 1.0001 # 0.01% Entry Buffer
                    stock['trigger_at'] = dt.now(IST)
                    stock['stop_base'] = stock['prev_high'] if (candle['high'] - candle['low'])/candle['close'] > 0.007 else candle['low']

        # BEAR Check: Prev Low ke niche close aur qualified volume
        elif self.engine_live['bear'] and candle['open'] > stock['prev_low'] > candle['close']:
            if self._is_vol_qualified(token, candle, 'bear'):
                if stock['stock_trades'] < self.config['bear']['max_per_stock']:
                    stock['status'] = 'TRIGGER_WATCH'; stock['side_latch'] = 'BEAR'
                    stock['trigger_px'] = candle['low'] * 0.9999 # 0.01% Entry Buffer
                    stock['trigger_at'] = dt.now(IST)
                    stock['stop_base'] = stock['prev_low'] if (candle['high'] - candle['low'])/candle['close'] > 0.007 else candle['high']

    def _manage_exit_and_tsl(self, token, ltp):
        """RAM based Step-wise Trailing Monitoring with 0.02% Exit Buffer"""
        trade = self.open_trades.get(token)
        if not trade: return

        if trade['side'] == 'BUY':
            # Exit Conditions with 0.02% Slippage Buffer
            if ltp >= (trade['target'] * 1.0002) or ltp <= (trade['sl'] * 0.9998):
                self._fire_hft_order(token, 'SELL', ltp, 0, "BULL_EXIT")
                return
            # Stepwise Trailing
            profit = ltp - trade['entry_px']
            if profit >= trade['step']:
                lvls = floor(profit / trade['step'])
                new_sl = trade['entry_px'] + ((lvls - 1) * trade['step'])
                if new_sl > trade['sl']: trade['sl'] = round(new_sl * 20) / 20

        elif trade['side'] == 'SELL':
            if ltp <= (trade['target'] * 0.9998) or ltp >= (trade['sl'] * 1.0002):
                self._fire_hft_order(token, 'BUY', ltp, 0, "BEAR_EXIT")
                return
            profit = trade['entry_px'] - ltp
            if profit >= trade['step']:
                lvls = floor(profit / trade['step'])
                new_sl = trade['entry_px'] - ((lvls - 1) * trade['step'])
                if new_sl < trade['sl']: trade['sl'] = round(new_sl * 20) / 20

    def _fire_hft_order(self, token, side, price, stop_base, reason):
        """Atomic Guard + Non-blocking Order Execution"""
        stock = self.stocks[token]
        # Memory Latch (Ghost order protection)
        stock['status'] = 'EXECUTING'
        asyncio.run_coroutine_threadsafe(self._async_kite_execute(token, side, price, stop_base, reason), self.loop)

    async def _async_kite_execute(self, token, side, price, stop_base, reason):
        stock = self.stocks[token]
        label = side.lower() if side in ['BUY', 'SELL'] else ('bull' if 'BULL' in reason else 'bear')
        try:
            # 1. Global LUA Limit Check (Race condition protection)
            if side in ['BUY', 'SELL']:
                if r.eval(LUA_INC_LIMIT, 1, f"hft:count:{self.user_id}:{label}", self.config[label]['max_total']) == -1:
                    stock['status'] = 'PENDING'; return

            # 2. Risk Calculation
            risk_tier = self.config[label]['risk_tiers'][min(stock['stock_trades'], 2)]
            qty = max(1, int(floor(risk_tier / abs(price - stop_base)))) if abs(price - stop_base) > 0 else 1

            # 3. Kite API Call (Market MIS)
            oid = self.kite.place_order(
                tradingsymbol=stock['symbol'], exchange='NSE', transaction_type=side,
                quantity=qty, order_type='MARKET', product='MIS', variety='regular'
            )

            # --- 4. ACTUAL EXECUTION PRICE SYNC (Slippage Control) ---
            actual_entry = price # Fallback price
            for _ in range(5):
                await asyncio.sleep(0.1) # 100ms wait
                hist = self.kite.order_history(oid)
                if hist[-1]['status'] == 'COMPLETE':
                    actual_entry = float(hist[-1]['average_price'])
                    break
            
            # 5. Success Memory Update
            if side in ['BUY', 'SELL']:
                stock['status'] = 'OPEN'; stock['stock_trades'] += 1
                risk = abs(actual_entry - stop_base)
                self.open_trades[token] = {
                    'side': side, 'entry_px': actual_entry, 'sl': stop_base, 'qty': qty,
                    'target': actual_entry + (risk * self.config[label]['rr']) if side == 'BUY' else actual_entry - (risk * self.config[label]['rr']),
                    'step': risk * self.config[label]['tsl'], 'oid': oid
                }
            else:
                stock['status'] = 'CLOSED'; self.open_trades.pop(token, None)
            
            # DB Queue update
            await self.db_queue.put({'token': token, 'side': side, 'price': actual_entry, 'oid': oid, 'reason': reason})
            logger.info(f"HFT {side} OK: {stock['symbol']} @ {actual_entry}")

        except Exception as e:
            logger.error(f"Execution Error: {e}"); stock['status'] = 'PENDING'

    async def broker_integrity_watcher(self):
        """Kite Order Book se RAM monitoring ko hamesha sync rakhna (15s polling)"""
        while True:
            try:
                await asyncio.sleep(15)
                kite_orders = self.kite.orders()
                kite_map = {o['tradingsymbol']: o['status'] for o in kite_orders if o['product'] == 'MIS'}
                
                close_old_connections()
                for token in list(self.open_trades.keys()):
                    sym = self.stocks[token]['symbol']
                    if sym in kite_map and kite_map[sym] in ['REJECTED', 'CANCELLED']:
                        logger.warning(f"Integrity Purge: {sym} manually closed at broker.")
                        self.open_trades.pop(token, None); stock['status'] = 'PENDING'
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
        # Kite Ticker Start
        self.kws.on_ticks = self.on_ticks
        self.kws.connect(threaded=True)

if __name__ == "__main__":
    BreakoutNexus(user_id=1).run()