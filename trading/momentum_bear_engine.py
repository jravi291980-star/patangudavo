"""
Momentum Bear Engine (1-Min First Candle Breakdown - SHORT)
- ARCHITECTURE: Multi-Threaded, Redis Streams, Auto-Healing.
- OPTIMIZATION: Fail-Fast Logic (CPU -> RAM -> Redis -> DB).
- SETTINGS: Cached in RAM (Updates every 5s).
- SAFETY: Thread Heartbeat Monitoring (Restart on Silent Death).
"""

import json
import time
import logging
import threading
from math import floor
from datetime import datetime as dt, time as dt_time, timedelta
from typing import Dict, Any, Set

import pytz
import redis
from django.db import transaction, models, close_old_connections
from django.conf import settings
from tenacity import retry, stop_after_attempt, wait_exponential

from trading.models import Account, MomentumBearTrade
from trading.utils import get_redis_connection, get_kite

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")
redis_client = get_redis_connection()

# --- REDIS KEYS ---
TICK_STREAM_KEY = "tick_stream"
FIRST_CANDLE_PREFIX = "first_candle_915"
PREV_DAY_HASH = getattr(settings, "PREV_DAY_HASH", "prev_day_ohlc")

KEY_SETTINGS = "algo:settings:mom_bear"
KEY_BLACKLIST = "algo:blacklist"
KEY_ENGINE_ENABLED = "algo:engine:mom_bear:enabled"
KEY_PANIC_TRIGGER = "algo:panic:mom_bear"

# --- CONSTANTS ---
SL_BUFFER_PCT = 0.0002
ENTRY_BUFFER_PCT = 0.0001
GAP_LIMIT_PCT = 0.03
MAX_MONITORING_MINUTES = 45

class MomentumBearClient:
    DAILY_RESET_TIME = dt_time(20, 0, 0)

    def __init__(self, account: Account):
        self.account = account
        self.kite = get_kite(account)
        self.running = True
        
        # Internal State
        self.monitoring_list: Set[str] = set()
        self.open_trades = {}
        self.pending_orders = {}
        self.latest_prices: Dict[str, float] = {}

        # Identifiers
        self.group_name = f"MBear_GROUP:{self.account.id}"
        self.consumer_name = f"MBear_CONSUMER:{threading.get_ident()}"

        # --- RAM CACHE ---
        self.cached_settings = {}
        self.cached_blacklist = set()
        self.cached_engine_enabled = True
        self.last_cache_update = 0
        self.log_throttle = {}

        # Redis Keys
        today_iso = dt.now(IST).date().isoformat()
        self.trade_count_key = f"mom_bear_count:{self.account.id}:{today_iso}"
        self.active_entries_set = f"mom_bear_active_entries:{self.account.id}"
        self.exiting_trades_set = f"mom_bear_exiting:{self.account.id}"
        self.lock_prefix = f"mom_bear_lock:{self.account.id}"
        self.daily_pnl_key = f"mom_bear_pnl:{self.account.id}:{today_iso}"
        self.force_exit_set = f"mom_bear_force_exit_requests:{self.account.id}"

        # --- HEARTBEAT VARS ---
        self.last_data_beat = time.time()
        self.data_thread_alive = False

        if not redis_client: 
            logger.error("MBear: Redis unavailable")
            self.running = False
            return

        self._ensure_consumer_group(TICK_STREAM_KEY, '$')
        self._prefill_prices()
        self._load_state_from_db()
        self._update_global_cache(force=True)

    def _ensure_consumer_group(self, stream_key, start_id='$'):
        try:
            redis_client.xgroup_create(stream_key, self.group_name, id=start_id, mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e): logger.error(f"MBear: Group Create Error: {e}")
        except Exception: pass

    def _prefill_prices(self):
        try:
            raw = redis_client.get('live_ohlc_data')
            if raw:
                data = json.loads(raw)
                for sym, info in data.items():
                    self.latest_prices[sym] = float(info.get('ltp', 0))
            logger.info(f"MBear: Prefilled prices for {len(self.latest_prices)} symbols.")
        except Exception: pass

    def _smart_log(self, symbol, message):
        now = time.time()
        if now - self.log_throttle.get(symbol, 0) > 5:
            logger.info(message)
            self.log_throttle[symbol] = now

    def _update_global_cache(self, force=False):
        if force or (time.time() - self.last_cache_update > 5):
            try:
                data = redis_client.get(KEY_SETTINGS)
                self.cached_settings = json.loads(data) if data else {}
                
                bl = redis_client.smembers(KEY_BLACKLIST)
                self.cached_blacklist = {b.decode('utf-8') for b in bl} if bl else set()
                
                status = redis_client.get(KEY_ENGINE_ENABLED)
                self.cached_engine_enabled = (status.decode('utf-8') == "1") if status else True
                
                self.last_cache_update = time.time()
            except: pass

    def _daily_reset(self):
        now = dt.now(IST)
        if now.time() >= self.DAILY_RESET_TIME:
            reset_key = f"mom_bear_reset:{now.date()}"
            if redis_client.set(reset_key, "1", nx=True, ex=86400):
                redis_client.delete(self.trade_count_key, self.active_entries_set, self.exiting_trades_set, self.daily_pnl_key, self.force_exit_set)
                self.monitoring_list.clear()
                logger.info("MBear: Daily Reset Triggered.")

    def _load_state_from_db(self):
        try:
            close_old_connections()
            today_start = IST.localize(dt.combine(dt.now(IST).date(), dt_time.min))
            qs = MomentumBearTrade.objects.filter(account=self.account, created_at__gte=today_start, status__in=['OPEN', 'PENDING_ENTRY', 'PENDING_EXIT'])
            self.open_trades.clear(); self.pending_orders.clear(); active_symbols = []
            for t in qs:
                active_symbols.append(t.symbol)
                if t.status == 'OPEN': self.open_trades[t.symbol] = t
                else: self.pending_orders[t.symbol] = t
            if active_symbols: redis_client.sadd(self.active_entries_set, *active_symbols)
            
            count = MomentumBearTrade.objects.filter(account=self.account, created_at__gte=today_start).exclude(status__in=['FAILED_ENTRY', 'EXPIRED']).count()
            redis_client.set(self.trade_count_key, count)
        except: pass

    def _get_first_candle(self, symbol):
        try: 
            raw = redis_client.get(f"{FIRST_CANDLE_PREFIX}:{symbol}")
            return json.loads(raw) if raw else None
        except: return None

    def _check_and_incr_trade_count(self) -> bool:
        settings_local = self.cached_settings
        max_trades = int(settings_local.get('max_trades', 3))
        lua_script = "local c=tonumber(redis.call('GET',KEYS[1]) or 0); if c>=tonumber(ARGV[1]) then return 0 else redis.call('INCR',KEYS[1]); redis.call('EXPIRE',KEYS[1],86400); return 1 end"
        try: return bool(redis_client.eval(lua_script, 1, self.trade_count_key, max_trades))
        except: return False

    def _rollback_trade_count(self):
        try: redis_client.decr(self.trade_count_key)
        except: pass

    # --- CORE LOGIC: SCANNER ---
    def scan_for_valid_setups(self):
        now = dt.now(IST).time()
        # Only scan between 9:16 and 9:17
        if not (dt_time(9, 16) <= now < dt_time(9, 17)):
            if self.monitoring_list and now >= dt_time(9, 17, 30):
                if now > dt_time(10, 0): self.monitoring_list.clear()
            return
            
        if not self.cached_engine_enabled: return

        active_symbols_keys = list(self.latest_prices.keys())
        vol_criteria = self.cached_settings.get('volume_criteria', [])

        for symbol in active_symbols_keys:
            if symbol in self.monitoring_list: continue
            if symbol in self.cached_blacklist: continue
            
            if redis_client.sismember(self.active_entries_set, symbol): continue
            
            c1 = self._get_first_candle(symbol)
            if not c1: continue

            # Volume Math
            passed_vol = False
            c_vol = c1['volume']; c_sma = float(c1.get('vol_sma_375', 0)); c_vp = float(c1.get('vol_price_cr', 0))
            
            if vol_criteria:
                for crit in vol_criteria:
                    try:
                        if c_vp >= float(crit['min_vol_price_cr']):
                            if c_sma >= float(crit['min_sma_avg']) and c_vol >= (c_sma * float(crit['sma_multiplier'])):
                                passed_vol = True; break
                    except: continue
            else: passed_vol = True 
            
            if not passed_vol: continue

            # Prev Day Gap Check
            raw_pd = redis_client.hget(PREV_DAY_HASH, symbol)
            if not raw_pd: continue
            prev_close = float(json.loads(raw_pd).get('close', 0))
            if prev_close == 0: continue
            
            # Bear Gap Check
            if ((prev_close - c1['low']) / prev_close) > GAP_LIMIT_PCT: continue
            
            self.monitoring_list.add(symbol)
            logger.info(f"MBear: Added {symbol} to monitoring list.")

    # --- CORE LOGIC: MONITOR BREAKDOWN ---
    def monitor_breakdowns(self):
        now = dt.now(IST).time()
        if now < dt_time(9, 16): return
        
        for symbol in list(self.monitoring_list):
            if redis_client.sismember(self.active_entries_set, symbol): 
                self.monitoring_list.discard(symbol); continue
            
            ltp = self.latest_prices.get(symbol, 0.0)
            if ltp == 0: continue
            
            c1 = self._get_first_candle(symbol)
            if not c1: continue

            if ltp < (c1['low'] * (1.0 - ENTRY_BUFFER_PCT)):
                raw_pd = redis_client.hget(PREV_DAY_HASH, symbol)
                pc = float(json.loads(raw_pd).get('close', 0)) if raw_pd else 0
                self._attempt_entry(symbol, ltp, c1['low'], pc)
                self.monitoring_list.discard(symbol)

    def _attempt_entry(self, symbol, entry_price, c1_low, prev_close):
        lock_key = f"{self.lock_prefix}:{symbol}"
        if not redis_client.set(lock_key, "1", nx=True, ex=10): return

        close_old_connections()
        try:
            if not self._check_and_incr_trade_count(): return 
            sett = self.cached_settings
            
            sl_pct = float(sett.get('stop_loss_pct', 0.5)) / 100.0
            stop_price = entry_price * (1.0 + sl_pct + SL_BUFFER_PCT)
            
            rr_str = sett.get('risk_reward', '1:2')
            rr = float(rr_str.split(':')[1]) if ':' in rr_str else 2.0
            risk = stop_price - entry_price
            target_price = entry_price - (risk * rr)

            risk_amt = float(sett.get('risk_per_trade', 2000.0))
            qty = max(1, int(floor(risk_amt / risk))) if risk > 0 else 1

            try:
                order_id = self.kite.place_order(tradingsymbol=symbol, exchange='NSE', transaction_type='SELL', quantity=qty, order_type='MARKET', product='MIS', variety='regular')
            except Exception as e:
                logger.error(f"MBear: Order Failed {symbol}: {e}")
                self._rollback_trade_count(); return

            with transaction.atomic():
                t = MomentumBearTrade.objects.create(
                    user=self.account.user, account=self.account, symbol=symbol, 
                    status='PENDING_ENTRY', entry_level=entry_price, stop_level=stop_price, 
                    target_level=target_price, quantity=qty, entry_order_id=order_id, 
                    entry_time=dt.now(IST), first_candle_low=c1_low, 
                    prev_day_close=prev_close, candle_ts=dt.now(IST)
                )
                self.pending_orders[symbol] = t
                redis_client.sadd(self.active_entries_set, symbol)
                logger.info(f"MBear: SELL Executed {symbol} Qty: {qty}")
        except Exception as e:
            logger.error(f"MBear: Setup DB Error {symbol}: {e}")
            redis_client.delete(lock_key)

    # --- CORE LOGIC: MANAGEMENT ---
    def manage_active_trades(self):
        if redis_client.exists(KEY_PANIC_TRIGGER):
            redis_client.delete(KEY_PANIC_TRIGGER); self._panic_exit_all(); return

        close_old_connections()
        sett = self.cached_settings
        tr_str = sett.get('trailing_sl', '1:1.5')
        trail_ratio = float(tr_str.split(':')[1]) if ':' in tr_str else 1.5

        for symbol, trade in list(self.open_trades.items()):
            if redis_client.sismember(self.force_exit_set, str(trade.id)):
                self._initiate_exit(trade, "Manual Exit")
                redis_client.srem(self.force_exit_set, str(trade.id))
                continue

            if redis_client.sismember(self.exiting_trades_set, trade.id): continue
            
            ltp = self.latest_prices.get(symbol, 0.0)
            if ltp == 0: continue

            self._smart_log(symbol, f"MBear: Monitor {symbol} PnL:{trade.entry_price - ltp:.2f}")

            # Trailing SL
            rr_str = sett.get('risk_reward', '1:2')
            rr = float(rr_str.split(':')[1]) if ':' in rr_str else 2.0
            init_risk = (trade.entry_price - trade.target_level) / rr
            
            if init_risk > 0:
                step_size = init_risk * trail_ratio
                curr_profit = trade.entry_price - ltp
                if curr_profit >= step_size:
                    levels = floor(curr_profit / step_size)
                    new_sl = trade.entry_price - ((levels - 1) * step_size)
                    new_sl = round(new_sl * 20) / 20
                    if new_sl < trade.stop_level:
                        trade.stop_level = new_sl; trade.save(update_fields=['stop_level'])
                        logger.info(f"MBear: Trailed SL {symbol} to {new_sl}")

            if ltp >= trade.stop_level: self._initiate_exit(trade, "SL Hit")
            elif ltp <= trade.target_level: self._initiate_exit(trade, "Target Hit")

    def _initiate_exit(self, trade, reason):
        if not redis_client.sadd(self.exiting_trades_set, trade.id): return
        try:
            oid = self.kite.place_order(tradingsymbol=trade.symbol, exchange='NSE', transaction_type='BUY', quantity=trade.quantity, order_type='MARKET', product='MIS',variety='regular')
            with transaction.atomic():
                trade.status = 'PENDING_EXIT'; trade.exit_reason = reason; trade.exit_order_id = oid; trade.save()
            self.open_trades.pop(trade.symbol, None); self.pending_orders[trade.symbol] = trade
            logger.info(f"MBear: EXIT Initiated {trade.symbol} ({reason})")
        except Exception as e:
            logger.error(f"MBear: Exit Failed {trade.symbol}: {e}")
            redis_client.srem(self.exiting_trades_set, trade.id)

    def _panic_exit_all(self):
        logger.warning("MBear: PANIC EXIT TRIGGERED!")
        for sym, trade in list(self.open_trades.items()): self._initiate_exit(trade, "Panic Exit")

    def _reconcile_loop(self):
        while self.running:
            close_old_connections()
            try:
                if not self.pending_orders:
                    time.sleep(0.5); continue

                try:
                    all_orders = self.kite.orders()
                    order_map = {o['order_id']: o for o in all_orders}
                except Exception:
                    time.sleep(1); continue

                for symbol, trade in list(self.pending_orders.items()):
                    oid = trade.entry_order_id if trade.status == 'PENDING_ENTRY' else trade.exit_order_id
                    if not oid: continue

                    if oid not in order_map: continue
                    
                    order_data = order_map[oid]
                    status = order_data['status']
                    
                    if status == 'COMPLETE':
                        fill = float(order_data['average_price'])
                        if trade.status == 'PENDING_ENTRY':
                            trade.status = 'OPEN'; trade.entry_price = fill
                            sett = self.cached_settings
                            rr_str = sett.get('risk_reward', '1:2')
                            rr = float(rr_str.split(':')[1]) if ':' in rr_str else 2.0
                            risk = trade.stop_level - fill
                            trade.target_level = fill - (risk * rr)
                            trade.save()
                            self.pending_orders.pop(symbol, None); self.open_trades[symbol] = trade
                        
                        elif trade.status == 'PENDING_EXIT':
                            trade.status = 'CLOSED'; trade.exit_price = fill; trade.exit_time = dt.now(IST)
                            trade.pnl = (trade.entry_price - trade.exit_price) * trade.quantity
                            trade.save(); redis_client.incrbyfloat(self.daily_pnl_key, trade.pnl)
                            redis_client.srem(self.active_entries_set, symbol); redis_client.srem(self.exiting_trades_set, trade.id)
                            self.pending_orders.pop(symbol, None)

                    elif status in ['CANCELLED', 'REJECTED']:
                        if trade.status == 'PENDING_ENTRY':
                            trade.status = 'FAILED_ENTRY'; trade.save(); self._rollback_trade_count()
                            redis_client.srem(self.active_entries_set, symbol); self.pending_orders.pop(symbol, None)
                        elif trade.status == 'PENDING_EXIT':
                            trade.status = 'OPEN'; trade.exit_order_id = None; trade.save()
                            redis_client.srem(self.exiting_trades_set, trade.id)
                            self.pending_orders.pop(symbol, None); self.open_trades[symbol] = trade

                time.sleep(0.5)
            except Exception: time.sleep(1)

    # --- DATA CONSUMER (WITH HEARTBEAT) ---
    def _listen_to_tick_stream(self):
        """Heartbeat-enabled Data Consumer"""
        self.data_thread_alive = True
        while self.running:
            self.last_data_beat = time.time() # PULSE
            
            # Occasional Settings Refresh
            if time.time() % 5 < 0.1: self._update_global_cache()

            try:
                # Block 1s max so we loop back to pulse heartbeat
                msgs = redis_client.xreadgroup(self.group_name, self.consumer_name, {TICK_STREAM_KEY: '>'}, count=200, block=1000)
                if msgs:
                    for _, messages in msgs:
                        ack_ids = []
                        for mid, fields in messages:
                            try:
                                symbol_bytes = fields.get(b'symbol') or fields.get('symbol')
                                ltp_bytes = fields.get(b'ltp') or fields.get('ltp')
                                if symbol_bytes and ltp_bytes:
                                    sym = symbol_bytes.decode() if isinstance(symbol_bytes, bytes) else symbol_bytes
                                    ltp = float(ltp_bytes.decode()) if isinstance(ltp_bytes, bytes) else float(ltp_bytes)
                                    self.latest_prices[sym] = ltp
                                ack_ids.append(mid)
                            except Exception: pass
                        if ack_ids: redis_client.xack(TICK_STREAM_KEY, self.group_name, *ack_ids)
            
            except redis.exceptions.ResponseError as e:
                if "NOGROUP" in str(e): self._ensure_consumer_group(TICK_STREAM_KEY, '$')
                time.sleep(1)
            except Exception as e:
                logger.error(f"MBear Stream Error: {e}")
                time.sleep(1)
        
        self.data_thread_alive = False

    def run(self):
        self._daily_reset()
        t_data = threading.Thread(target=self._listen_to_tick_stream, daemon=True, name="MBear_Data")
        t_data.start()
        threading.Thread(target=self._reconcile_loop, daemon=True, name="MBear_Reconcile").start()

        logger.info("MomentumBear: Engine Started.")

        while self.running:
            try:
                # HEARTBEAT CHECK
                if time.time() - self.last_data_beat > 15:
                    logger.critical("MBear: DATA THREAD DIED! Restarting Engine...")
                    self.running = False; break

                self.scan_for_valid_setups()
                self.monitor_breakdowns()
                self.manage_active_trades()
                time.sleep(0.01)
            except Exception as e:
                logger.error(f"MBear Main Loop Error: {e}")
                time.sleep(1)

    def stop(self): self.running = False