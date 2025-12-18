# """
# CashBreakdown Client (Short Sell Engine) - Final Production Version
# - ARCHITECTURE: Multi-Process, Redis Streams, In-Memory Caching.
# - OPTIMIZATION: Fail-Fast Logic (CPU -> RAM -> Redis -> DB).
# - SETTINGS: Cached in RAM (Updates every 5s).
# - LOGGING: Detailed 'Smart Logging' (Throttled to prevents disk overflow).
# - DATA: 'tick_stream' for Execution (No Stale Checks), 'candle_1m' for Scanning.
# """

# import json
# import time
# import logging
# import threading
# from math import floor
# from datetime import datetime as dt, time as dt_time, timedelta
# from typing import Dict, Any

# import pytz
# import redis
# from django.db import transaction, models, close_old_connections
# from django.conf import settings
# from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_not_exception_type
# from kiteconnect.exceptions import TokenException

# from trading.models import Account, CashBreakdownTrade
# from trading.utils import get_redis_connection, get_kite

# logger = logging.getLogger(__name__)
# IST = pytz.timezone("Asia/Kolkata")
# redis_client = get_redis_connection()

# # --- REDIS KEYS ---
# CANDLE_STREAM_KEY = getattr(settings, "BREAKDOWN_CANDLE_STREAM", "candle_1m")
# TICK_STREAM_KEY = "tick_stream"
# LIVE_OHLC_KEY = getattr(settings, "BREAKDOWN_LIVE_OHLC_KEY", "live_ohlc_data")
# PREV_DAY_HASH = getattr(settings, "BREAKDOWN_PREV_DAY_HASH", "prev_day_ohlc")

# KEY_GLOBAL_SETTINGS = "algo:settings:global"
# KEY_BEAR_SETTINGS = "algo:settings:bear"
# KEY_BLACKLIST = "algo:blacklist"
# KEY_ENGINE_ENABLED = "algo:engine:bear:enabled"
# KEY_PANIC_TRIGGER = "algo:panic:bear"

# ENTRY_OFFSET_PCT = 0.0001
# STOP_OFFSET_PCT = 0.0002
# BREAKDOWN_MAX_CANDLE_PCT = 0.007
# MAX_MONITORING_MINUTES = 6 

# # --- HELPER FUNCTIONS ---

# def _get_prev_day_low(redis_conn, symbol):
#     """Fetches Prev Day Low from Redis Hash."""
#     try:
#         raw = redis_conn.hget(PREV_DAY_HASH, symbol)
#         if raw: return float(json.loads(raw).get("low"))
#     except: pass
#     return None

# def _get_prev_day_close(redis_conn, symbol):
#     """Fetches Prev Day Close from Redis Hash."""
#     try:
#         raw = redis_conn.hget(PREV_DAY_HASH, symbol)
#         if raw: return float(json.loads(raw).get("close", 0.0))
#     except: pass
#     return None

# def _parse_candle_ts(ts_str):
#     """Parses timestamp string to IST datetime."""
#     if not ts_str: return dt.now(IST)
#     try:
#         d = dt.fromisoformat(ts_str)
#         if d.tzinfo is None: return IST.localize(d)
#         return d.astimezone(IST)
#     except: return dt.now(IST)

# def _parse_ratio_string(ratio_str, default):
#     """Parses '1:2' to 2.0."""
#     try: return float(ratio_str.split(':')[1])
#     except: return default

# # --- MAIN ENGINE CLASS ---

# class CashBreakdownClient:
#     DAILY_RESET_TIME = dt_time(20, 0, 0)

#     def __init__(self, account: Account):
#         self.account = account
#         self.kite = get_kite(account)
#         self.running = True

#         self.open_trades = {}
#         self.pending_trades = {}
#         self.latest_prices = {}

#         self.group_name = f"CBD_GROUP:{self.account.id}"
#         self.consumer_name = f"CBD_CONSUMER:{threading.get_ident()}" 
        
#         # --- RAM CACHE (Zero Latency) ---
#         self.cached_settings = {}
#         self.cached_blacklist = set()
#         self.cached_engine_enabled = True
#         self.last_cache_update = 0

#         # Logging Throttle Map
#         self.log_throttle = {}

#         # Redis Keys
#         today_iso = dt.now(IST).date().isoformat()
#         self.limit_reached_key = f"breakdown_limit_reached:{self.account.id}:{today_iso}"
#         self.trade_count_key = f"breakdown_trade_count:{self.account.id}:{today_iso}"
#         self.active_entries_set = f"breakdown_active_entries:{self.account.id}"
#         self.exiting_trades_set = f"breakdown_exiting_trades:{self.account.id}"
#         self.force_exit_set = f"breakdown_force_exit_requests:{self.account.id}"
#         self.entry_lock_key_prefix = f"cbd_entry_lock:{self.account.id}"
#         self.daily_pnl_key = f"cbd_daily_realized_pnl:{self.account.id}:{today_iso}"

#         if not redis_client: self.running = False; return

#         # Ensure Groups Exist
#         self._ensure_consumer_group(CANDLE_STREAM_KEY, '0')
#         self._ensure_consumer_group(TICK_STREAM_KEY, '$')

#         self._prefill_prices()
#         self._load_trades_from_db()
#         self._update_global_cache(force=True)

#     def _ensure_consumer_group(self, stream_key, start_id='$'):
#         """Creates the consumer group if it does not exist (Idempotent)."""
#         try:
#             redis_client.xgroup_create(stream_key, self.group_name, id=start_id, mkstream=True)
#             logger.info(f"CBD: Verified consumer group {self.group_name} for {stream_key}")
#         except redis.exceptions.ResponseError as e:
#             if "BUSYGROUP" not in str(e): logger.error(f"CBD: Failed group {stream_key}: {e}")
#         except Exception: pass

#     def _prefill_prices(self):
#         """Loads snapshot to avoid 0 prices at startup."""
#         try:
#             raw = redis_client.get(LIVE_OHLC_KEY)
#             if raw:
#                 data = json.loads(raw)
#                 now = dt.now(IST)
#                 for sym, info in data.items():
#                     self.latest_prices[sym] = {'ltp': float(info.get('ltp', 0)), 'ts': now}
#             logger.info(f"CBD: Prefilled prices for {len(self.latest_prices)} symbols.")
#         except: pass

#     def _update_global_cache(self, force=False):
#         """Fetches Settings, Blacklist, and Status every 5 seconds (Low I/O)."""
#         if force or (time.time() - self.last_cache_update > 5):
#             try:
#                 data = redis_client.get(KEY_BEAR_SETTINGS)
#                 self.cached_settings = json.loads(data) if data else {}
                
#                 bl = redis_client.smembers(KEY_BLACKLIST)
#                 self.cached_blacklist = {b.decode('utf-8') for b in bl} if bl else set()
                
#                 status = redis_client.get(KEY_ENGINE_ENABLED)
#                 self.cached_engine_enabled = (status.decode('utf-8') == "1") if status else True
                
#                 self.last_cache_update = time.time()
#             except: pass

#     def _smart_log(self, symbol, message):
#         """Logs a message only if 5 seconds have passed since the last log for this symbol."""
#         now = time.time()
#         if now - self.log_throttle.get(symbol, 0) > 5:
#             logger.info(message)
#             self.log_throttle[symbol] = now

#     def _daily_reset_trades(self) -> None:
#         now_ist = dt.now(IST)
#         if now_ist.time() >= self.DAILY_RESET_TIME:
#             reset_flag = f"cbd_reset:{now_ist.date()}"
#             if redis_client.set(reset_flag, "1", nx=True, ex=86400):
#                 self.open_trades.clear(); self.pending_trades.clear()
#                 redis_client.delete(self.trade_count_key, self.limit_reached_key, self.active_entries_set, self.exiting_trades_set, self.daily_pnl_key)

#     def _load_trades_from_db(self) -> None:
#         try:
#             close_old_connections()
#             today_start = IST.localize(dt.combine(dt.now(IST).date(), dt_time.min))
#             qs = CashBreakdownTrade.objects.filter(account=self.account, created_at__gte=today_start, status__in=['OPEN', 'PENDING', 'PENDING_ENTRY', 'PENDING_EXIT'])
#             self.open_trades.clear(); self.pending_trades.clear()
#             active_syms = []
#             for t in qs:
#                 active_syms.append(t.symbol)
#                 if t.status == 'PENDING': self.pending_trades[t.symbol] = t
#                 elif t.status in ['OPEN', 'PENDING_EXIT']: self.open_trades[t.symbol] = t
#             if active_syms: redis_client.sadd(self.active_entries_set, *active_syms)
#         except: pass

#     def _get_todays_symbol_counts(self):
#         today_start = IST.localize(dt.combine(dt.now(IST).date(), dt_time.min))
#         qs = CashBreakdownTrade.objects.filter(account=self.account, created_at__gte=today_start).exclude(status__in=['FAILED_ENTRY', 'EXPIRED'])
#         return {x['symbol']: x['count'] for x in qs.values('symbol').annotate(count=models.Count('symbol'))}

#     def _check_and_increment_trade_count(self):
#         settings = self.cached_settings
#         max_trades = int(settings.get("total_trades", 5))
#         lua = "local c=tonumber(redis.call('GET',KEYS[1]) or 0); if c>=tonumber(ARGV[1]) then return 0 end; redis.call('INCR',KEYS[1]); redis.call('EXPIRE',KEYS[1],86400); return 1"
#         try: return bool(redis_client.eval(lua, 1, self.trade_count_key, max_trades))
#         except: return False

#     def _calculate_quantity(self, entry_price, sl_price):
#         try:
#             cnt = int(redis_client.get(self.trade_count_key) or 0)
#             settings = self.cached_settings
#             max_loss = float(settings.get(f"risk_trade_{min(cnt+1,3)}") or settings.get("risk_per_trade") or 1000)
#         except: max_loss = 1000.0

#         risk = abs(sl_price - entry_price)
#         if risk <= 0: return 0
#         return max(0, int(floor(max_loss / risk)))

#     def _place_order(self, symbol, qty, txn_type):
#         return self.kite.place_order(tradingsymbol=symbol, quantity=qty, transaction_type=txn_type, product=self.kite.PRODUCT_MIS, order_type=self.kite.ORDER_TYPE_MARKET, exchange=self.kite.EXCHANGE_NSE, variety=self.kite.VARIETY_REGULAR)

#     def _check_volume_criteria(self, vol, vol_sma, vol_price_cr):
#         # Uses RAM Cached Settings (Instant)
#         criteria = self.cached_settings.get("volume_criteria", [])
#         if not criteria: return False
#         for c in criteria:
#             try:
#                 if vol_price_cr >= float(c.get('min_vol_price_cr', 999999)):
#                     if vol_sma >= float(c.get('min_sma_avg', 0)) and vol >= (vol_sma * float(c.get('sma_multiplier', 99))):
#                         return True
#             except: continue
#         return False

#     def _process_candle(self, payload):
#         """
#         FAIL-FAST SCANNER LOGIC:
#         1. Freshness & Global Switch (Instant)
#         2. JSON Parse (Microseconds)
#         3. Price Structure & Volume Check (Instant CPU)
#         4. RAM Blacklist (Instant)
#         5. Redis Checks (Network I/O)
#         6. DB Checks (Slowest I/O)
#         """
#         symbol = payload.get("symbol")
#         if not symbol: return
        
#         # 1. Freshness (CPU - Nanoseconds)
#         ts = _parse_candle_ts(payload.get("ts"))
#         if (dt.now(IST) - ts).total_seconds() > 300: return

#         # 2. Global Switch (RAM - Nanoseconds)
#         if not self.cached_engine_enabled: return

#         # 3. Parse Data (CPU - Microseconds)
#         try:
#             low = float(payload.get("low", 0)); high = float(payload.get("high", 0))
#             vol = int(payload.get("volume", 0))
#             close = float(payload.get("close", 0)); open_p = float(payload.get("open", 0))
#             vol_sma = float(payload.get("vol_sma_375", 0))
#             vol_price_cr = float(payload.get("vol_price_cr", 0))
#         except: return
        
#         # 4. Price Structure Check (CPU - Instant)
#         if close < 100: return 
#         if not (close < open_p): return

#         # 5. Volume Math Filter (CPU - Instant)
#         if not self._check_volume_criteria(vol, vol_sma, vol_price_cr): return

#         # --- CANDIDATE FOUND (Passed Volume) ---
#         self._smart_log(symbol, f"CBD: Candidate {symbol} | Vol:{vol} SMA:{vol_sma}")

#         # 6. Blacklist Check (RAM)
#         if symbol in self.cached_blacklist: return

#         # 7. Duplicate Check (Redis - Fast)
#         if redis_client.sismember(self.active_entries_set, symbol): return

#         # 8. Redis Check: Prev Low (Medium)
#         prev_low = _get_prev_day_low(redis_client, symbol)
#         if not prev_low: return

#         # 9. Pattern Logic (CPU)
#         if not (open_p > prev_low > close and low < prev_low): return
        
#         stop_base = high
#         if ((high - low)/close) > BREAKDOWN_MAX_CANDLE_PCT:
#             prev_close = _get_prev_day_close(redis_client, symbol)
#             if not prev_close: return
#             if ((low - prev_close)/prev_close < -0.03) or ((low - prev_low)/prev_low < -0.005): return
#             stop_base = prev_low

#         # 10. Database Checks (Postgres Network I/O - ~2ms)
#         close_old_connections()
#         if self._get_todays_symbol_counts().get(symbol, 0) >= int(self.cached_settings.get("trades_per_stock", 2)): return

#         # 11. Execute (Create Pending Trade)
#         entry = low * (1.0 - ENTRY_OFFSET_PCT)
#         stop = stop_base + (stop_base * STOP_OFFSET_PCT)
#         rr = _parse_ratio_string(self.cached_settings.get("risk_reward", "1:2"), 2.0)
#         target = entry - (rr * (stop - entry))

#         try:
#             with transaction.atomic():
#                 t = CashBreakdownTrade.objects.create(
#                     user=self.account.user, account=self.account, symbol=symbol,
#                     candle_ts=ts, candle_open=open_p, candle_high=high, candle_low=low, candle_close=close, candle_volume=vol,
#                     prev_day_low=prev_low, entry_level=entry, stop_level=stop, target_level=target,
#                     volume_price=vol_price_cr, status="PENDING"
#                 )
#                 self.pending_trades[symbol] = t
#                 redis_client.sadd(self.active_entries_set, symbol)
#                 logger.info(f"CBD: Setup Found {symbol}. Target:{target:.2f}")
#         except Exception as e: logger.error(f"CBD: Setup Err: {e}")

#     def _try_enter_pending(self):
#         if not self.cached_engine_enabled or not self.pending_trades: return
#         now = dt.now(IST)
#         to_remove = []

#         for symbol, trade in list(self.pending_trades.items()):
#             # RAM Blacklist Check
#             if symbol in self.cached_blacklist:
#                 trade.status = "EXPIRED"; trade.exit_reason = "Blacklisted"; trade.save()
#                 redis_client.srem(self.active_entries_set, symbol)
#                 to_remove.append(symbol); continue

#             # RAM Price Check
#             tick_data = self.latest_prices.get(symbol)
#             if not tick_data: continue
            
#             ltp = tick_data['ltp']
#             tick_ts = tick_data['ts']
            
#             # --- NO STALE CHECK ---
#             if ltp <= 0: continue

#             # Smart Logging
#             self._smart_log(symbol, f"CBD: Watching {symbol} | LTP:{ltp} Entry:{trade.entry_level}")

#             # Expiry Check (IST aware)
#             if trade.candle_ts:
#                 trade_ts = trade.candle_ts if trade.candle_ts.tzinfo else IST.localize(trade.candle_ts)
#                 if now > trade_ts + timedelta(minutes=MAX_MONITORING_MINUTES):
#                     logger.info(f"CBD: EXPIRED {symbol} at {now.time()}")
#                     trade.status = "EXPIRED"; trade.save()
#                     redis_client.srem(self.active_entries_set, symbol)
#                     to_remove.append(symbol); continue

#             if ltp > trade.stop_level:
#                 trade.status = "EXPIRED"; trade.save()
#                 redis_client.srem(self.active_entries_set, symbol)
#                 to_remove.append(symbol); continue

#             if ltp < trade.entry_level:
#                 if redis_client.set(f"{self.entry_lock_key_prefix}:{symbol}", "1", nx=True, ex=5):
#                     try:
#                         qty = self._calculate_quantity(trade.entry_level, trade.stop_level)
#                         if qty <= 0: raise ValueError("Qty 0")
#                         if self._check_and_increment_trade_count():
#                             oid = self._place_order(symbol, qty, "SELL")
#                             with transaction.atomic():
#                                 trade.status = "PENDING_ENTRY"; trade.quantity = qty; trade.entry_order_id = oid; trade.save()
#                             logger.info(f"CBD: SELL {symbol} Qty:{qty}")
#                     except Exception as e:
#                         logger.error(f"CBD: Entry Err {symbol}: {e}")
#                         redis_client.decr(self.trade_count_key)
#                         trade.status = "FAILED_ENTRY"; trade.save()
#                         redis_client.srem(self.active_entries_set, symbol)
#                     finally:
#                         redis_client.delete(f"{self.entry_lock_key_prefix}:{symbol}")
#                         to_remove.append(symbol)

#         for s in to_remove: self.pending_trades.pop(s, None)

#     def _check_trailing_stop(self, trade, ltp):
#         if trade.status != "OPEN": return
#         risk_reward_rr = _parse_ratio_string(self.cached_settings.get("risk_reward", "1:2"), 2.0)

#         if trade.stop_level <= trade.entry_price:
#             div = risk_reward_rr if risk_reward_rr > 0 else 2.0
#             init_risk = (trade.entry_price - trade.target_level) / div
#         else:
#             init_risk = trade.stop_level - trade.entry_price

#         if init_risk <= 0: return

#         trail_ratio_str = self.cached_settings.get("trailing_sl", "1:1.5")
#         trail_ratio_mult = _parse_ratio_string(trail_ratio_str, 1.5)
#         step_size = init_risk * trail_ratio_mult
#         current_profit = trade.entry_price - ltp

#         if current_profit < step_size: return

#         levels_gained = floor(current_profit / step_size)
#         new_sl_level = trade.entry_price - ((levels_gained - 1) * step_size)
#         new_sl_level = round(new_sl_level * 20) / 20

#         if new_sl_level < trade.stop_level:
#             old_sl = trade.stop_level
#             trade.stop_level = new_sl_level
#             trade.save(update_fields=['stop_level'])
#             logger.info(f"CBD: STEP TRAIL {trade.symbol}. Lvl:{levels_gained} | Profit:{current_profit:.2f} | SL: {old_sl} -> {new_sl_level}")

#     def monitor_trades(self):
#         if redis_client.exists(KEY_PANIC_TRIGGER):
#             redis_client.delete(KEY_PANIC_TRIGGER)
#             self.force_square_off("Panic Button")

#         if not self.open_trades: return
#         trail_ratio = _parse_ratio_string(self.cached_settings.get("trailing_sl", "1:1.5"), 1.5)
        
#         for symbol, trade in list(self.open_trades.items()):
#             if redis_client.sismember(self.force_exit_set, str(trade.id)):
#                 self.exit_trade(trade, self.latest_prices.get(symbol, {}).get('ltp', 0.0), "Manual Exit")
#                 redis_client.srem(self.force_exit_set, str(trade.id)); continue

#             if redis_client.sismember(self.exiting_trades_set, trade.id): continue
            
#             tick = self.latest_prices.get(symbol)
#             if not tick or tick['ltp'] <= 0: continue
#             ltp = tick['ltp']

#             self._smart_log(symbol, f"CBD: Monitor {symbol} PnL:{trade.entry_price - ltp:.2f}")

#             profit = trade.entry_price - ltp
#             risk = (trade.entry_price - trade.target_level) / 2.0 
#             if risk > 0 and profit >= (risk * trail_ratio):
#                 lvl = floor(profit / (risk * trail_ratio))
#                 new_sl = trade.entry_price - ((lvl-1) * (risk * trail_ratio))
#                 if new_sl < trade.stop_level:
#                     trade.stop_level = new_sl; trade.save()

#             if ltp >= trade.stop_level: self._exit_trade(trade, "SL Hit")
#             elif ltp <= trade.target_level: self._exit_trade(trade, "Target Hit")

#     def _exit_trade(self, trade, reason):
#         if redis_client.sadd(self.exiting_trades_set, trade.id):
#             try:
#                 oid = self._place_order(trade.symbol, trade.quantity, "BUY")
#                 with transaction.atomic():
#                     trade.status = "PENDING_EXIT"; trade.exit_reason = reason; trade.exit_order_id = oid; trade.save()
#             except: redis_client.srem(self.exiting_trades_set, trade.id)

#     def _force_exit(self, reason):
#         for s, t in list(self.open_trades.items()): self._exit_trade(t, reason)

#     def _reconcile_loop(self):
#         while self.running:
#             close_old_connections()
#             try:
#                 # Uses CashBreakdownTrade
#                 qs = CashBreakdownTrade.objects.filter(account=self.account, status__in=["PENDING_ENTRY", "PENDING_EXIT"])
#                 if not qs.exists():
#                     time.sleep(1)
#                     continue

#                 try:
#                     all_orders = self.kite.orders()
#                     omap = {o['order_id']: o for o in all_orders}
#                 except Exception as ke:
#                     logger.error(f"CBD RECONCILE: Kite Error: {ke}")
#                     time.sleep(2)
#                     continue

#                 for t in qs:
#                     oid = t.entry_order_id if t.status == "PENDING_ENTRY" else t.exit_order_id
#                     if not oid or oid not in omap: continue
                    
#                     ord_data = omap[oid]
#                     if ord_data['status'] == "COMPLETE":
#                         fill = float(ord_data['average_price'])
#                         if t.status == "PENDING_ENTRY":
#                             t.status = "OPEN"; t.entry_price = fill; t.quantity = int(ord_data['filled_quantity'])
#                             t.entry_time = dt.now(IST)
#                             rr = _parse_ratio_string(self.cached_settings.get("risk_reward", "1:2"), 2.0)
#                             # Bear Target: Entry - Risk
#                             t.target_level = fill - ((t.stop_level - fill) * rr)
#                             t.save()
#                             self.open_trades[t.symbol] = t
#                             logger.info(f"CBD RECONCILE: {t.symbol} OPEN at {fill}")
#                         else:
#                             t.status = "CLOSED"; t.exit_price = fill; t.exit_time = dt.now(IST)
#                             # Bear PnL: Entry - Exit
#                             t.pnl = (t.entry_price - fill) * t.quantity
#                             t.save()
#                             redis_client.incrbyfloat(self.daily_pnl_key, t.pnl)
#                             self.open_trades.pop(t.symbol, None)
#                             redis_client.srem(self.exiting_trades_set, t.id)
#                             redis_client.srem(self.active_entries_set, t.symbol)
#                     elif ord_data['status'] in ["CANCELLED", "REJECTED"]:
#                         if t.status == "PENDING_ENTRY":
#                             t.status = "FAILED_ENTRY"; t.save()
#                             redis_client.decr(self.trade_count_key)
#                             redis_client.srem(self.active_entries_set, t.symbol)
#                         else:
#                             t.status = "OPEN"; t.exit_order_id = None; t.save()
#                             redis_client.srem(self.exiting_trades_set, t.id)
#                 time.sleep(0.1)
#             except Exception as e:
#                 logger.error(f"CBD RECONCILE ERROR: {e}")
#                 time.sleep(2)

#     def _listen_to_stream(self, stream_key, is_candle=False):
#         while self.running:
#             self._update_global_cache() # REFRESH SETTINGS
#             try:
#                 msgs = redis_client.xreadgroup(self.group_name, self.consumer_name, {stream_key: '>'}, count=200, block=1000)
#                 if msgs:
#                     for _, messages in msgs:
#                         ack_ids = []
#                         for mid, fields in messages:
#                             try:
#                                 if is_candle:
#                                     raw = fields.get(b'data') or fields.get('data')
#                                     if raw: 
#                                         payload = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
#                                         now = dt.now(IST).time()
#                                         if now >= self.account.breakdown_start_time and now <= self.account.breakdown_end_time:
#                                             self._process_candle(payload)
#                                 else:
#                                     # Tick Update
#                                     sym = fields.get(b'symbol') or fields.get('symbol')
#                                     ltp = fields.get(b'ltp') or fields.get('ltp')
#                                     if sym and ltp:
#                                         s_str = sym.decode() if isinstance(sym, bytes) else sym
#                                         l_flt = float(ltp.decode() if isinstance(ltp, bytes) else ltp)
#                                         self.latest_prices[s_str] = {'ltp': l_flt, 'ts': dt.now(IST)}
#                                 ack_ids.append(mid)
#                             except: pass
#                         if ack_ids: redis_client.xack(stream_key, self.group_name, *ack_ids)
#             except redis.exceptions.ResponseError:
#                 self._ensure_consumer_group(stream_key, '0' if is_candle else '$'); time.sleep(1)
#             except: time.sleep(1)

#     def run(self):
#         self._daily_reset_trades()
#         threading.Thread(target=self._listen_to_stream, args=(CANDLE_STREAM_KEY, True), daemon=True).start()
#         threading.Thread(target=self._listen_to_stream, args=(TICK_STREAM_KEY, False), daemon=True).start()
#         threading.Thread(target=self._reconcile_loop, daemon=True).start()
#         logger.info("CBD: Engine Started.")
#         while self.running:
#             close_old_connections()
#             try: self._try_enter_pending(); self.monitor_trades(); time.sleep(0.005)
#             except: time.sleep(1)

#     def stop(self): self.running = False

"""
CashBreakdown Client (Short Sell Engine) - Final Production Version
- ARCHITECTURE: Multi-Process, Redis Streams, In-Memory Caching.
- OPTIMIZATION: Fail-Fast Logic (CPU -> RAM -> Redis -> DB).
- SETTINGS: Cached in RAM (Updates every 5s).
- LOGGING: Detailed 'Smart Logging' (Throttled to prevents disk overflow).
- DATA: 'tick_stream' for Execution (No Stale Checks), 'candle_1m' for Scanning.
"""

import json
import time
import logging
import threading
from math import floor
from datetime import datetime as dt, time as dt_time, timedelta
from typing import Dict, Any

import pytz
import redis
from django.db import transaction, models, close_old_connections
from django.conf import settings
from kiteconnect.exceptions import TokenException

from trading.models import Account, CashBreakdownTrade
from trading.utils import get_redis_connection, get_kite

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")
redis_client = get_redis_connection()

# --- REDIS KEYS ---
CANDLE_STREAM_KEY = getattr(settings, "BREAKDOWN_CANDLE_STREAM", "candle_1m")
TICK_STREAM_KEY = "tick_stream"
LIVE_OHLC_KEY = getattr(settings, "BREAKDOWN_LIVE_OHLC_KEY", "live_ohlc_data")
PREV_DAY_HASH = getattr(settings, "BREAKDOWN_PREV_DAY_HASH", "prev_day_ohlc")

KEY_GLOBAL_SETTINGS = "algo:settings:global"
KEY_BEAR_SETTINGS = "algo:settings:bear"
KEY_BLACKLIST = "algo:blacklist"
KEY_ENGINE_ENABLED = "algo:engine:bear:enabled"
KEY_PANIC_TRIGGER = "algo:panic:bear"

ENTRY_OFFSET_PCT = 0.0001
STOP_OFFSET_PCT = 0.0002
BREAKDOWN_MAX_CANDLE_PCT = 0.007
MAX_MONITORING_MINUTES = 6 

# --- HELPER FUNCTIONS ---

def _get_prev_day_low(redis_conn, symbol):
    """Fetches Prev Day Low from Redis Hash."""
    try:
        raw = redis_conn.hget(PREV_DAY_HASH, symbol)
        if raw: return float(json.loads(raw).get("low"))
    except: pass
    return None

def _get_prev_day_close(redis_conn, symbol):
    """Fetches Prev Day Close from Redis Hash."""
    try:
        raw = redis_conn.hget(PREV_DAY_HASH, symbol)
        if raw: return float(json.loads(raw).get("close", 0.0))
    except: pass
    return None

def _parse_candle_ts(ts_str):
    """Parses timestamp string to IST datetime."""
    if not ts_str: return dt.now(IST)
    try:
        d = dt.fromisoformat(ts_str)
        if d.tzinfo is None: return IST.localize(d)
        return d.astimezone(IST)
    except: return dt.now(IST)

def _parse_ratio_string(ratio_str, default):
    """Parses '1:2' to 2.0."""
    try:
        if isinstance(ratio_str, (int, float)): return float(ratio_str)
        if ':' in str(ratio_str): return float(ratio_str.split(':')[1])
        return float(ratio_str)
    except: return default

# --- MAIN ENGINE CLASS ---

class CashBreakdownClient:
    DAILY_RESET_TIME = dt_time(20, 0, 0)

    def __init__(self, account: Account):
        self.account = account
        self.kite = get_kite(account)
        self.running = True

        self.open_trades = {}
        self.pending_trades = {}
        self.latest_prices = {}

        self.group_name = f"CBD_GROUP:{self.account.id}"
        self.consumer_name = f"CBD_CON_{self.account.id}_{int(time.time())}" 
        
        # --- RAM CACHE ---
        self.cached_settings = {}
        self.cached_blacklist = set()
        self.cached_engine_enabled = True
        self.last_cache_update = 0

        # Logging Throttle Map
        self.log_throttle = {}

        # Redis Keys
        today_iso = dt.now(IST).date().isoformat()
        self.limit_reached_key = f"breakdown_limit_reached:{self.account.id}:{today_iso}"
        self.trade_count_key = f"breakdown_trade_count:{self.account.id}:{today_iso}"
        self.active_entries_set = f"breakdown_active_entries:{self.account.id}"
        self.exiting_trades_set = f"breakdown_exiting_trades:{self.account.id}"
        self.force_exit_set = f"breakdown_force_exit_requests:{self.account.id}"
        self.entry_lock_key_prefix = f"cbd_entry_lock:{self.account.id}"
        self.daily_pnl_key = f"cbd_daily_realized_pnl:{self.account.id}:{today_iso}"

        if not redis_client: 
            self.running = False
            return

        self._ensure_consumer_group(CANDLE_STREAM_KEY, '0')
        self._ensure_consumer_group(TICK_STREAM_KEY, '$')

        self._prefill_prices()
        self._load_trades_from_db()
        self._update_global_cache(force=True)

    def _ensure_consumer_group(self, stream_key, start_id='$'):
        try:
            redis_client.xgroup_create(stream_key, self.group_name, id=start_id, mkstream=True)
            logger.info(f"CBD: Verified group {self.group_name} for {stream_key}")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e): logger.error(f"CBD: Group Err {stream_key}: {e}")
        except Exception: pass

    def _prefill_prices(self):
        try:
            raw = redis_client.get(LIVE_OHLC_KEY)
            if raw:
                data = json.loads(raw)
                now = dt.now(IST)
                for sym, info in data.items():
                    self.latest_prices[sym] = {'ltp': float(info.get('ltp', 0)), 'ts': now}
        except: pass

    def _update_global_cache(self, force=False):
        if force or (time.time() - self.last_cache_update > 5):
            try:
                data = redis_client.get(KEY_BEAR_SETTINGS)
                self.cached_settings = json.loads(data) if data else {}
                
                bl = redis_client.smembers(KEY_BLACKLIST)
                self.cached_blacklist = {b.decode('utf-8') for b in bl} if bl else set()
                
                status = redis_client.get(KEY_ENGINE_ENABLED)
                self.cached_engine_enabled = (status.decode('utf-8') == "1") if status else True
                
                self.last_cache_update = time.time()
            except: pass

    def _smart_log(self, symbol, message):
        now = time.time()
        if now - self.log_throttle.get(symbol, 0) > 10:
            logger.info(message)
            self.log_throttle[symbol] = now

    def _daily_reset_trades(self) -> None:
        now_ist = dt.now(IST)
        if now_ist.time() >= self.DAILY_RESET_TIME:
            reset_flag = f"cbd_reset:{now_ist.date()}"
            if redis_client.set(reset_flag, "1", nx=True, ex=86400):
                self.open_trades.clear()
                self.pending_trades.clear()
                redis_client.delete(self.trade_count_key, self.limit_reached_key, self.active_entries_set, self.exiting_trades_set, self.daily_pnl_key)

    def _load_trades_from_db(self) -> None:
        try:
            close_old_connections()
            today_start = IST.localize(dt.combine(dt.now(IST).date(), dt_time.min))
            qs = CashBreakdownTrade.objects.filter(
                account=self.account, 
                created_at__gte=today_start, 
                status__in=['OPEN', 'PENDING', 'PENDING_ENTRY', 'PENDING_EXIT']
            )
            self.open_trades.clear()
            self.pending_trades.clear()
            active_syms = []
            for t in qs:
                active_syms.append(t.symbol)
                if t.status == 'PENDING': self.pending_trades[t.symbol] = t
                elif t.status in ['OPEN', 'PENDING_EXIT']: self.open_trades[t.symbol] = t
            if active_syms: 
                redis_client.sadd(self.active_entries_set, *active_syms)
        except: pass

    def _get_todays_symbol_counts(self):
        today_start = IST.localize(dt.combine(dt.now(IST).date(), dt_time.min))
        qs = CashBreakdownTrade.objects.filter(account=self.account, created_at__gte=today_start).exclude(status__in=['FAILED_ENTRY', 'EXPIRED'])
        return {x['symbol']: x['count'] for x in qs.values('symbol').annotate(count=models.Count('symbol'))}

    def _check_and_increment_trade_count(self):
        settings_map = self.cached_settings
        max_trades = int(settings_map.get("total_trades", 5))
        lua = "local c=tonumber(redis.call('GET',KEYS[1]) or 0); if c>=tonumber(ARGV[1]) then return 0 end; redis.call('INCR',KEYS[1]); redis.call('EXPIRE',KEYS[1],86400); return 1"
        try: return bool(redis_client.eval(lua, 1, self.trade_count_key, max_trades))
        except: return False

    def _calculate_quantity(self, entry_price, sl_price):
        try:
            cnt = int(redis_client.get(self.trade_count_key) or 0)
            settings_map = self.cached_settings
            max_loss = float(settings_map.get(f"risk_trade_{min(cnt+1,3)}") or settings_map.get("risk_per_trade") or 1000)
        except: max_loss = 1000.0

        risk = abs(sl_price - entry_price)
        if risk <= 0: return 0
        return max(1, int(floor(max_loss / risk)))

    def _place_order(self, symbol, qty, txn_type):
        return self.kite.place_order(
            tradingsymbol=symbol, 
            quantity=qty, 
            transaction_type=txn_type, 
            product=self.kite.PRODUCT_MIS, 
            order_type=self.kite.ORDER_TYPE_MARKET, 
            exchange=self.kite.EXCHANGE_NSE, 
            variety=self.kite.VARIETY_REGULAR
        )

    def _check_volume_criteria(self, vol, vol_sma, vol_price_cr):
        criteria = self.cached_settings.get("volume_criteria", [])
        if not criteria: return False
        for c in criteria:
            try:
                if vol_price_cr >= float(c.get('min_vol_price_cr', 999999)):
                    if vol_sma >= float(c.get('min_sma_avg', 0)) and vol >= (vol_sma * float(c.get('sma_multiplier', 99))):
                        return True
            except: continue
        return False

    def _process_candle(self, payload):
        symbol = payload.get("symbol")
        if not symbol: return
        
        ts = _parse_candle_ts(payload.get("ts"))
        if (dt.now(IST) - ts).total_seconds() > 300: return
        if not self.cached_engine_enabled: return

        try:
            low = float(payload.get("low", 0)); high = float(payload.get("high", 0))
            vol = int(payload.get("volume", 0))
            close = float(payload.get("close", 0)); open_p = float(payload.get("open", 0))
            vol_sma = float(payload.get("vol_sma_375", 0))
            vol_price_cr = float(payload.get("vol_price_cr", 0))
        except: return
        
        if close < 100: return 
        if not (close < open_p): return
        if not self._check_volume_criteria(vol, vol_sma, vol_price_cr): return

        if symbol in self.cached_blacklist: return
        if redis_client.sismember(self.active_entries_set, symbol): return

        prev_low = _get_prev_day_low(redis_client, symbol)
        if not prev_low: return

        # Breakdown Pattern
        if not (open_p > prev_low > close and low < prev_low): return
        
        stop_base = high
        if ((high - low)/close) > BREAKDOWN_MAX_CANDLE_PCT:
            prev_close = _get_prev_day_close(redis_client, symbol)
            if not prev_close: return
            # Gap Down Protection
            if ((low - prev_close)/prev_close < -0.03) or ((low - prev_low)/prev_low < -0.005): return
            stop_base = prev_low

        close_old_connections()
        if self._get_todays_symbol_counts().get(symbol, 0) >= int(self.cached_settings.get("trades_per_stock", 2)): return

        entry = low * (1.0 - ENTRY_OFFSET_PCT)
        stop = stop_base + (stop_base * STOP_OFFSET_PCT)
        rr = _parse_ratio_string(self.cached_settings.get("risk_reward", "1:2"), 2.0)
        target = entry - (rr * (stop - entry))

        try:
            with transaction.atomic():
                t = CashBreakdownTrade.objects.create(
                    user=self.account.user, account=self.account, symbol=symbol,
                    candle_ts=ts, candle_open=open_p, candle_high=high, candle_low=low, candle_close=close, candle_volume=vol,
                    prev_day_low=prev_low, entry_level=entry, stop_level=stop, target_level=target,
                    volume_price=vol_price_cr, status="PENDING"
                )
                self.pending_trades[symbol] = t
                redis_client.sadd(self.active_entries_set, symbol)
                logger.info(f"CBD: Signal {symbol}. Entry:{entry:.2f} SL:{stop:.2f} Tgt:{target:.2f}")
        except Exception as e: logger.error(f"CBD: Entry Save Err: {e}")

    def _try_enter_pending(self):
        if not self.cached_engine_enabled or not self.pending_trades: return
        now = dt.now(IST)
        to_remove = []

        for symbol, trade in list(self.pending_trades.items()):
            if symbol in self.cached_blacklist:
                trade.status = "EXPIRED"; trade.exit_reason = "Blacklisted"; trade.save()
                redis_client.srem(self.active_entries_set, symbol)
                to_remove.append(symbol); continue

            tick_data = self.latest_prices.get(symbol)
            if not tick_data: continue
            
            ltp = tick_data['ltp']
            if ltp <= 0: continue

            self._smart_log(symbol, f"CBD: Watching {symbol} | LTP:{ltp} Entry:{trade.entry_level}")

            # Monitoring Expiry
            trade_ts = trade.candle_ts if trade.candle_ts.tzinfo else IST.localize(trade.candle_ts)
            if now > trade_ts + timedelta(minutes=MAX_MONITORING_MINUTES):
                trade.status = "EXPIRED"; trade.save()
                redis_client.srem(self.active_entries_set, symbol)
                to_remove.append(symbol); continue

            # Stop Level Violation (Before Entry)
            if ltp > trade.stop_level:
                trade.status = "EXPIRED"; trade.exit_reason = "SL Violated Pre-Entry"; trade.save()
                redis_client.srem(self.active_entries_set, symbol)
                to_remove.append(symbol); continue

            # Entry Trigger
            if ltp < trade.entry_level:
                lock_key = f"{self.entry_lock_key_prefix}:{symbol}"
                if redis_client.set(lock_key, "1", nx=True, ex=10):
                    try:
                        qty = self._calculate_quantity(trade.entry_level, trade.stop_level)
                        if qty > 0 and self._check_and_increment_trade_count():
                            oid = self._place_order(symbol, qty, "SELL")
                            with transaction.atomic():
                                trade.status = "PENDING_ENTRY"
                                trade.quantity = qty
                                trade.entry_order_id = oid
                                trade.save()
                            logger.info(f"CBD: SELL ORDER {symbol} Qty:{qty} OID:{oid}")
                    except Exception as e:
                        logger.error(f"CBD: Entry Exec Err {symbol}: {e}")
                        redis_client.decr(self.trade_count_key)
                        trade.status = "FAILED_ENTRY"; trade.save()
                        redis_client.srem(self.active_entries_set, symbol)
                    finally:
                        to_remove.append(symbol)

        for s in to_remove: self.pending_trades.pop(s, None)

    def monitor_trades(self):
        """Logic for Exiting and Trailing."""
        if redis_client.exists(KEY_PANIC_TRIGGER):
            logger.warning("PANIC TRIGGER ACTIVE!")
            self._force_exit("Panic Squareoff")
            redis_client.delete(KEY_PANIC_TRIGGER)
            return

        if not self.open_trades: return
        
        trail_ratio = _parse_ratio_string(self.cached_settings.get("trailing_sl", "1:1.5"), 1.5)
        
        for symbol, trade in list(self.open_trades.items()):
            # FIX: Convert trade.id to string for Redis set consistency
            tid_str = str(trade.id)

            if redis_client.sismember(self.force_exit_set, tid_str):
                self._exit_trade(trade, "Manual Exit")
                redis_client.srem(self.force_exit_set, tid_str)
                continue

            if redis_client.sismember(self.exiting_trades_set, tid_str):
                continue
            
            tick = self.latest_prices.get(symbol)
            if not tick or tick['ltp'] <= 0:
                self._smart_log(symbol, f"CBD: Waiting for tick to monitor {symbol}")
                continue
            
            ltp = tick['ltp']
            self._smart_log(symbol, f"CBD: Open {symbol} | LTP:{ltp} SL:{trade.stop_level} Pnl:{(trade.entry_price - ltp)*trade.quantity:.2f}")

            # 1. Trailing SL Logic (Bear)
            current_profit = trade.entry_price - ltp
            initial_risk = trade.stop_level - trade.entry_price
            
            if initial_risk > 0:
                step_size = initial_risk * trail_ratio
                if current_profit >= step_size:
                    levels = floor(current_profit / step_size)
                    # New SL = Entry + Risk - (levels * step)
                    new_sl = trade.entry_price + initial_risk - (levels * step_size)
                    new_sl = round(new_sl * 20) / 20 # 0.05 precision
                    
                    if new_sl < trade.stop_level:
                        logger.info(f"CBD: Trailing {symbol} SL: {trade.stop_level} -> {new_sl}")
                        trade.stop_level = new_sl
                        trade.save(update_fields=['stop_level'])

            # 2. Hard Exit Condition
            if ltp >= trade.stop_level:
                self._exit_trade(trade, "SL Hit")
            elif ltp <= trade.target_level:
                self._exit_trade(trade, "Target Hit")

    def _exit_trade(self, trade, reason):
        tid_str = str(trade.id)
        if redis_client.sadd(self.exiting_trades_set, tid_str):
            try:
                oid = self._place_order(trade.symbol, trade.quantity, "BUY")
                with transaction.atomic():
                    trade.status = "PENDING_EXIT"
                    trade.exit_reason = reason
                    trade.exit_order_id = oid
                    trade.save()
                logger.info(f"CBD: EXIT {trade.symbol} | {reason} | OID:{oid}")
            except Exception as e:
                logger.error(f"CBD: Exit Order Failed {trade.symbol}: {e}")
                redis_client.srem(self.exiting_trades_set, tid_str)

    def _force_exit(self, reason):
        for s, t in list(self.open_trades.items()): 
            self._exit_trade(t, reason)

    def _reconcile_loop(self):
        while self.running:
            close_old_connections()
            try:
                qs = CashBreakdownTrade.objects.filter(account=self.account, status__in=["PENDING_ENTRY", "PENDING_EXIT"])
                if not qs.exists():
                    time.sleep(1); continue

                all_orders = self.kite.orders()
                omap = {str(o['order_id']): o for o in all_orders}

                for t in qs:
                    oid = str(t.entry_order_id if t.status == "PENDING_ENTRY" else t.exit_order_id)
                    if not oid or oid not in omap: continue
                    
                    ord_data = omap[oid]
                    if ord_data['status'] == "COMPLETE":
                        fill = float(ord_data['average_price'])
                        with transaction.atomic():
                            if t.status == "PENDING_ENTRY":
                                t.status = "OPEN"; t.entry_price = fill; t.quantity = int(ord_data['filled_quantity'])
                                t.entry_time = dt.now(IST)
                                rr = _parse_ratio_string(self.cached_settings.get("risk_reward", "1:2"), 2.0)
                                t.target_level = fill - ((t.stop_level - fill) * rr)
                                t.save()
                                self.open_trades[t.symbol] = t
                            else:
                                t.status = "CLOSED"; t.exit_price = fill; t.exit_time = dt.now(IST)
                                t.pnl = (t.entry_price - fill) * t.quantity
                                t.save()
                                redis_client.incrbyfloat(self.daily_pnl_key, t.pnl)
                                self.open_trades.pop(t.symbol, None)
                                redis_client.srem(self.exiting_trades_set, str(t.id))
                                redis_client.srem(self.active_entries_set, t.symbol)
                        logger.info(f"CBD RECONCILE: {t.symbol} now {t.status} at {fill}")
                    
                    elif ord_data['status'] in ["CANCELLED", "REJECTED"]:
                        with transaction.atomic():
                            if t.status == "PENDING_ENTRY":
                                t.status = "FAILED_ENTRY"
                                redis_client.decr(self.trade_count_key)
                                redis_client.srem(self.active_entries_set, t.symbol)
                            else:
                                t.status = "OPEN" # Reset to monitor and try exit again
                                t.exit_order_id = None
                                redis_client.srem(self.exiting_trades_set, str(t.id))
                            t.save()
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"CBD RECONCILE ERR: {e}")
                time.sleep(2)

    def _listen_to_stream(self, stream_key, is_candle=False):
        while self.running:
            try:
                # Group read with 1s block
                msgs = redis_client.xreadgroup(self.group_name, self.consumer_name, {stream_key: '>'}, count=100, block=1000)
                if msgs:
                    for _, messages in msgs:
                        ack_ids = []
                        for mid, fields in messages:
                            try:
                                if is_candle:
                                    raw = fields.get(b'data') or fields.get('data')
                                    if raw: 
                                        payload = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
                                        now = dt.now(IST).time()
                                        if now >= self.account.breakdown_start_time and now <= self.account.breakdown_end_time:
                                            self._process_candle(payload)
                                else:
                                    sym = fields.get(b'symbol') or fields.get('symbol')
                                    ltp = fields.get(b'ltp') or fields.get('ltp')
                                    if sym and ltp:
                                        s_str = sym.decode() if isinstance(sym, bytes) else sym
                                        l_flt = float(ltp.decode() if isinstance(ltp, bytes) else ltp)
                                        self.latest_prices[s_str] = {'ltp': l_flt, 'ts': dt.now(IST)}
                                ack_ids.append(mid)
                            except: pass
                        if ack_ids: redis_client.xack(stream_key, self.group_name, *ack_ids)
            except redis.exceptions.ResponseError:
                self._ensure_consumer_group(stream_key, '0' if is_candle else '$')
                time.sleep(1)
            except: time.sleep(1)

    def run(self):
        self._daily_reset_trades()
        threading.Thread(target=self._listen_to_stream, args=(CANDLE_STREAM_KEY, True), daemon=True).start()
        threading.Thread(target=self._listen_to_stream, args=(TICK_STREAM_KEY, False), daemon=True).start()
        threading.Thread(target=self._reconcile_loop, daemon=True).start()
        
        logger.info(f"CBD: Engine Live for Account {self.account.id}")
        
        while self.running:
            close_old_connections()
            try: 
                self._update_global_cache()
                self._try_enter_pending()
                self.monitor_trades()
                time.sleep(0.01) # 10ms loop
            except Exception as e: 
                logger.error(f"CBD MAIN LOOP ERR: {e}")
                time.sleep(1)

    def stop(self): self.running = False