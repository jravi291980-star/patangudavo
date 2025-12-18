# """
# CashBreakout Client (Long Buy Engine) - Final Production Version
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

# from trading.models import Account, CashBreakoutTrade
# from trading.utils import get_redis_connection, get_kite

# logger = logging.getLogger(__name__)
# IST = pytz.timezone("Asia/Kolkata")
# redis_client = get_redis_connection()

# # --- REDIS KEYS ---
# CANDLE_STREAM_KEY = getattr(settings, "BREAKOUT_CANDLE_STREAM", "candle_1m")
# TICK_STREAM_KEY = "tick_stream"
# LIVE_OHLC_KEY = getattr(settings, "BREAKOUT_LIVE_OHLC_KEY", "live_ohlc_data")
# PREV_DAY_HASH = getattr(settings, "BREAKOUT_PREV_DAY_HASH", "prev_day_ohlc")

# KEY_BULL_SETTINGS = "algo:settings:bull"
# KEY_BLACKLIST = "algo:blacklist"
# KEY_ENGINE_ENABLED = "algo:engine:bull:enabled"
# KEY_PANIC_TRIGGER = "algo:panic:bull"

# ENTRY_OFFSET_PCT = 0.0001
# STOP_OFFSET_PCT = 0.0002
# BREAKOUT_MAX_CANDLE_PCT = 0.007
# MAX_MONITORING_MINUTES = 6 

# # --- HELPER FUNCTIONS ---

# def _get_prev_day_high(redis_conn, symbol):
#     """Fetches Prev Day High from Redis Hash."""
#     try:
#         raw = redis_conn.hget(PREV_DAY_HASH, symbol)
#         if raw: return float(json.loads(raw).get("high"))
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

# class CashBreakoutClient:
#     DAILY_RESET_TIME = dt_time(20, 0, 0)

#     def __init__(self, account: Account):
#         self.account = account
#         self.kite = get_kite(account)
#         self.running = True
        
#         self.open_trades = {}
#         self.pending_trades = {}
#         self.latest_prices = {} 

#         self.group_name = f"CB_GROUP:{self.account.id}"
#         self.consumer_name = f"CB_CONSUMER:{threading.get_ident()}" 
        
#         # --- RAM CACHE (Zero Latency) ---
#         self.cached_settings = {}
#         self.cached_blacklist = set()
#         self.cached_engine_enabled = True
#         self.last_cache_update = 0
        
#         # Logging Throttle Map: {'SYMBOL': timestamp}
#         self.log_throttle = {}

#         # Redis Keys
#         today_iso = dt.now(IST).date().isoformat()
#         self.limit_reached_key = f"breakout_limit_reached:{self.account.id}:{today_iso}"
#         self.trade_count_key = f"breakout_trade_count:{self.account.id}:{today_iso}"
#         self.active_entries_set = f"breakout_active_entries:{self.account.id}"
#         self.exiting_trades_set = f"breakout_exiting_trades:{self.account.id}"
#         self.force_exit_set = f"breakout_force_exit_requests:{self.account.id}"
#         self.entry_lock_key_prefix = f"cb_entry_lock:{self.account.id}"
#         self.daily_pnl_key = f"cb_daily_realized_pnl:{self.account.id}:{today_iso}"

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
#             logger.info(f"CB: Verified consumer group {self.group_name} for {stream_key}")
#         except redis.exceptions.ResponseError as e:
#             if "BUSYGROUP" not in str(e): logger.error(f"CB: Failed group {stream_key}: {e}")
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
#             logger.info(f"CB: Prefilled prices for {len(self.latest_prices)} symbols.")
#         except: pass

#     def _update_global_cache(self, force=False):
#         """Fetches Settings, Blacklist, and Status every 5 seconds (Low I/O)."""
#         if force or (time.time() - self.last_cache_update > 5):
#             try:
#                 # 1. Settings
#                 data = redis_client.get(KEY_BULL_SETTINGS)
#                 self.cached_settings = json.loads(data) if data else {}
                
#                 # 2. Blacklist (Set)
#                 bl = redis_client.smembers(KEY_BLACKLIST)
#                 self.cached_blacklist = {b.decode('utf-8') for b in bl} if bl else set()
                
#                 # 3. Engine Enabled
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

#     def _daily_reset_trades(self):
#         now_ist = dt.now(IST)
#         if now_ist.time() >= self.DAILY_RESET_TIME:
#             reset_flag = f"cbd_reset:{now_ist.date()}"
#             if redis_client.set(reset_flag, "1", nx=True, ex=86400):
#                 self.open_trades.clear(); self.pending_trades.clear()
#                 redis_client.delete(self.trade_count_key, self.limit_reached_key, self.active_entries_set, self.exiting_trades_set, self.daily_pnl_key)

#     def _load_trades_from_db(self):
#         try:
#             close_old_connections()
#             today_start = IST.localize(dt.combine(dt.now(IST).date(), dt_time.min))
#             qs = CashBreakoutTrade.objects.filter(account=self.account, created_at__gte=today_start, status__in=['OPEN', 'PENDING', 'PENDING_ENTRY', 'PENDING_EXIT'])
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
#         qs = CashBreakoutTrade.objects.filter(account=self.account, created_at__gte=today_start).exclude(status__in=['FAILED_ENTRY', 'EXPIRED'])
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
#             # Fallback to general risk if tiered risk is missing
#             max_loss = float(settings.get(f"risk_trade_{min(cnt+1,3)}") or settings.get("risk_per_trade") or 1000)
#         except: max_loss = 1000.0
#         risk = abs(entry_price - sl_price)
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

#     def _process_candle(self, candle_payload: Dict[str, Any]) -> None:
#         """
#         FAIL-FAST SCANNER LOGIC:
#         1. Freshness & Global Switch (Instant)
#         2. JSON Parse (Microseconds)
#         3. Price Structure & Volume Check (Instant CPU) -> Filters 99%
#         4. RAM Blacklist (Instant)
#         5. Redis Checks (Network I/O)
#         6. DB Checks (Slowest I/O)
#         """
#         symbol = candle_payload.get("symbol")
#         if not symbol: return
        
#         # 1. Freshness (CPU - Nanoseconds)
#         ts = _parse_candle_ts(candle_payload.get("ts"))
#         if (dt.now(IST) - ts).total_seconds() > 300: return

#         # 2. Global Switch (RAM - Nanoseconds)
#         if not self.cached_engine_enabled: return

#         # 3. Parse Data (CPU - Microseconds)
#         try:
#             low = float(candle_payload.get("low") or 0); high = float(candle_payload.get("high") or 0)
#             vol = int(candle_payload.get("volume") or 0)
#             close = float(candle_payload.get("close") or 0); open_p = float(candle_payload.get("open") or 0)
#             vol_sma = float(candle_payload.get("vol_sma_375") or 0)
#             vol_price_cr = float(candle_payload.get("vol_price_cr") or 0)
#         except: return

#         # 4. Price Structure Check (CPU - Instant)
#         if close < 100: return 
#         if not (close > open_p): return

#         # 5. Volume Math Filter (CPU - Instant)
#         if not self._check_volume_criteria(vol, vol_sma, vol_price_cr): return

#         # --- CANDIDATE FOUND (Passed Volume) ---
#         self._smart_log(symbol, f"CB: Candidate {symbol} | Vol:{vol} SMA:{vol_sma}")

#         # 6. Blacklist Check (RAM)
#         if symbol in self.cached_blacklist: return
        
#         # 7. Duplicate Check (Redis - Fast)
#         if redis_client.sismember(self.active_entries_set, symbol): return

#         # 8. Redis Check: Prev Day High (Medium)
#         prev_high = _get_prev_day_high(redis_client, symbol)
#         if not prev_high: return
        
#         # 9. Pattern Logic (CPU)
#         if not (low < prev_high < close): return
#         if not (open_p < prev_high): return
        
#         candle_size_pct = (high - low) / close if close > 0 else 0
#         stop_base = low

#         if candle_size_pct > BREAKOUT_MAX_CANDLE_PCT:
#             prev_close = _get_prev_day_close(redis_client, symbol)
#             if not prev_close: return 
#             if ((high - prev_close) / prev_close) > 0.03: return
#             if ((high - prev_high) / prev_high) > 0.005: return
#             stop_base = prev_high

#         # 10. Database Checks (Postgres Network I/O - ~2ms)
#         close_old_connections()
#         if self._get_todays_symbol_counts().get(symbol, 0) >= int(self.cached_settings.get("trades_per_stock", 2)): return

#         # 11. Execute (Create Pending Trade)
#         entry = high * (1.0 + ENTRY_OFFSET_PCT)
#         stop = stop_base - (stop_base * STOP_OFFSET_PCT)
#         rr = _parse_ratio_string(self.cached_settings.get("risk_reward", "1:2"), 2.0)
#         target = entry + (rr * (entry - stop))

#         try:
#             with transaction.atomic():
#                 t = CashBreakoutTrade.objects.create(
#                     user=self.account.user, account=self.account, symbol=symbol,
#                     candle_ts=ts, candle_open=open_p, candle_high=high, candle_low=low, candle_close=close, candle_volume=vol,
#                     prev_day_high=prev_high, entry_level=entry, stop_level=stop, target_level=target,
#                     volume_price=vol_price_cr, status="PENDING"
#                 )
#                 self.pending_trades[symbol] = t
#                 redis_client.sadd(self.active_entries_set, symbol)
#                 logger.info(f"CB: Setup Found {symbol}. Target: {target:.2f}")
#         except Exception as e: logger.error(f"CB: Setup error: {e}")

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
            
#             # --- NO STALE CHECK ---
#             if ltp <= 0: continue

#             # Smart Logging: Show we are watching
#             self._smart_log(symbol, f"CB: Watching {symbol} | LTP:{ltp} Entry:{trade.entry_level}")

#             # Expiry Check (IST aware)
#             if trade.candle_ts:
#                 trade_ts = trade.candle_ts if trade.candle_ts.tzinfo else IST.localize(trade.candle_ts)
#                 if now > trade_ts + timedelta(minutes=MAX_MONITORING_MINUTES):
#                     logger.info(f"CB: EXPIRED {symbol} at {now.time()}")
#                     trade.status = "EXPIRED"; trade.save()
#                     redis_client.srem(self.active_entries_set, symbol)
#                     to_remove.append(symbol); continue

#             if ltp < trade.stop_level:
#                 trade.status = "EXPIRED"; trade.save()
#                 redis_client.srem(self.active_entries_set, symbol)
#                 to_remove.append(symbol); continue

#             if ltp > trade.entry_level:
#                 # Lock to prevent double order
#                 if redis_client.set(f"{self.entry_lock_key_prefix}:{symbol}", "1", nx=True, ex=5):
#                     try:
#                         qty = self._calculate_quantity(ltp, trade.stop_level)
#                         if qty <= 0: raise ValueError("Qty 0")
#                         if self._check_and_increment_trade_count():
#                             oid = self._place_order(symbol, qty, "BUY")
#                             with transaction.atomic():
#                                 trade.status = "PENDING_ENTRY"; trade.quantity = qty; trade.entry_order_id = oid; trade.save()
#                             logger.info(f"CB: BUY {symbol} Qty:{qty}")
#                     except Exception as e:
#                         logger.error(f"CB: Buy Err {symbol}: {e}")
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
        
#         if trade.stop_level >= trade.entry_price:
#             div = risk_reward_rr if risk_reward_rr > 0 else 2.0
#             init_risk = (trade.target_level - trade.entry_price) / div
#         else:
#             init_risk = trade.entry_price - trade.stop_level

#         if init_risk <= 0: return
#         step_size = init_risk * _parse_ratio_string(self.cached_settings.get("trailing_sl", "1:1.5"), 1.5)
#         current_profit = ltp - trade.entry_price
#         if current_profit < step_size: return

#         levels_gained = floor(current_profit / step_size)
#         new_sl_level = trade.entry_price + ((levels_gained - 1) * step_size)
#         new_sl_level = round(new_sl_level * 20) / 20

#         if new_sl_level > trade.stop_level:
#             old_sl = trade.stop_level
#             trade.stop_level = new_sl_level
#             trade.save(update_fields=['stop_level'])
#             logger.info(f"CB: STEP TRAIL {trade.symbol}. Lvl:{levels_gained} | SL: {old_sl} -> {new_sl_level}")

#     def monitor_trades(self):
#         if redis_client.exists(KEY_PANIC_TRIGGER):
#             redis_client.delete(KEY_PANIC_TRIGGER)
#             self.force_square_off("Panic Button")

#         if not self.open_trades: return
        
#         for symbol, trade in list(self.open_trades.items()):
#             if redis_client.sismember(self.force_exit_set, str(trade.id)):
#                 self.exit_trade(trade, self.latest_prices.get(symbol, {}).get('ltp', 0.0), "Manual Exit")
#                 redis_client.srem(self.force_exit_set, str(trade.id)); continue

#             if redis_client.sismember(self.exiting_trades_set, trade.id): continue
            
#             tick = self.latest_prices.get(symbol)
#             if not tick or tick['ltp'] <= 0: continue
#             ltp = tick['ltp']
            
#             # Log Monitor status throttled
#             self._smart_log(symbol, f"CB: Monitor {symbol} PnL:{ltp - trade.entry_price:.2f}")

#             self._check_trailing_stop(trade, ltp)

#             if ltp <= trade.stop_level: self.exit_trade(trade, ltp, "SL Hit")
#             elif ltp >= trade.target_level: self.exit_trade(trade, ltp, "Target Hit")

#         if self.cached_settings.get("pnl_exit_enabled", False):
#             try: realized = float(redis_client.get(self.daily_pnl_key) or 0)
#             except: realized = 0.0
            
#             unrealized = sum([(self.latest_prices.get(t.symbol, {}).get('ltp', t.entry_price) - t.entry_price) * t.quantity for t in self.open_trades.values()])
#             net = realized + unrealized
            
#             if net >= float(self.cached_settings.get("max_profit", 999999)) or net <= -float(self.cached_settings.get("max_loss", 999999)):
#                 redis_client.sadd(self.limit_reached_key, "P&L_EXIT")
#                 self.force_square_off(f"P&L Exit (Net: {net:.2f})")

#     def exit_trade(self, trade, ltp, reason):
#         if redis_client.sismember(self.exiting_trades_set, trade.id): return
#         try:
#             oid = self._place_order(trade.symbol, trade.quantity, "SELL")
#             with transaction.atomic():
#                 t = CashBreakoutTrade.objects.select_for_update().get(id=trade.id)
#                 t.status = "PENDING_EXIT"; t.exit_reason = reason; t.exit_order_id = oid; t.save()
#                 redis_client.sadd(self.exiting_trades_set, t.id)
#         except Exception as e: logger.error(f"CB: Exit failed {trade.symbol}: {e}")

#     def force_square_off(self, reason):
#         for s, t in list(self.open_trades.items()):
#             if t.status == "OPEN": self.exit_trade(t, None, reason)
#     def _reconcile_loop(self):
#         while self.running:
#             close_old_connections()
#             try:
#                 # Uses CashBreakoutTrade
#                 qs = CashBreakoutTrade.objects.filter(account=self.account, status__in=["PENDING_ENTRY", "PENDING_EXIT"])
#                 if not qs.exists():
#                     time.sleep(1)
#                     continue

#                 try:
#                     all_orders = self.kite.orders()
#                     omap = {o['order_id']: o for o in all_orders}
#                 except Exception as ke:
#                     logger.error(f"CB RECONCILE: Kite Error: {ke}")
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
#                             # Bull Target: Entry + Risk
#                             t.target_level = fill + ((fill - t.stop_level) * rr)
#                             t.save()
#                             self.open_trades[t.symbol] = t
#                             logger.info(f"CB RECONCILE: {t.symbol} OPEN at {fill}")
#                         else:
#                             t.status = "CLOSED"; t.exit_price = fill; t.exit_time = dt.now(IST)
#                             # Bull PnL: Exit - Entry
#                             t.pnl = (fill - t.entry_price) * t.quantity
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
#                 logger.error(f"CB RECONCILE ERROR: {e}")
#                 time.sleep(2)
#     # def _reconcile_loop(self):
#     #     while self.running:
#     #         close_old_connections()
#     #         try:
#     #             qs = CashBreakoutTrade.objects.filter(account=self.account, status__in=["PENDING_ENTRY", "PENDING_EXIT"])
#     #             if qs.exists():
#     #                 all_orders = self.kite.orders()
#     #                 omap = {o['order_id']: o for o in all_orders}
#     #                 for t in qs:
#     #                     oid = t.entry_order_id if t.status == "PENDING_ENTRY" else t.exit_order_id
#     #                     if not oid: 
#     #                         t.status = "FAILED_ENTRY" if t.status=="PENDING_ENTRY" else "FAILED_EXIT"; t.save()
#     #                         redis_client.srem(self.active_entries_set, t.symbol)
#     #                         continue
#     #                     if oid not in omap: continue
                        
#     #                     d = omap[oid]; st = d['status']
#     #                     if st == "COMPLETE":
#     #                         fill = float(d['average_price'])
#     #                         if t.status == "PENDING_ENTRY":
#     #                             t.status = "OPEN"; t.entry_price = fill; t.quantity = int(d['filled_quantity'])
#     #                             t.entry_time = timezone.now()
#     #                             rr = _parse_ratio_string(self.cached_settings.get("risk_reward", "1:2"), 2.0)
#     #                             t.target_level = fill + ((fill - t.stop_level) * rr)
#     #                             t.save(); self.open_trades[t.symbol] = t; self.pending_trades.pop(t.symbol, None)
#     #                         else:
#     #                             t.status = "CLOSED"; t.exit_price = fill; t.exit_time = timezone.now()
#     #                             t.pnl = (fill - t.entry_price) * t.quantity
#     #                             t.save(); redis_client.incrbyfloat(self.daily_pnl_key, t.pnl)
#     #                             self.open_trades.pop(t.symbol, None); redis_client.srem(self.exiting_trades_set, t.id); redis_client.srem(self.active_entries_set, t.symbol)
#     #                     elif st in ["CANCELLED", "REJECTED"]:
#     #                         if t.status == "PENDING_ENTRY":
#     #                             t.status = "FAILED_ENTRY"; t.save(); redis_client.decr(self.trade_count_key)
#     #                             redis_client.srem(self.active_entries_set, t.symbol); self.pending_trades.pop(t.symbol, None)
#     #                         else:
#     #                             t.status = "OPEN"; t.exit_order_id = None; t.save(); redis_client.srem(self.exiting_trades_set, t.id)
#     #             time.sleep(0.5)
#     #         except: time.sleep(1)

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
#                                         if now >= self.account.breakout_start_time and now <= self.account.breakout_end_time:
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
#         logger.info("CB: Engine Started.")
#         while self.running:
#             close_old_connections()
#             try: self._try_enter_pending(); self.monitor_trades(); time.sleep(0.005)
#             except: time.sleep(1)

#     def stop(self): self.running = False

import json
import time
import logging
import threading
from math import floor
from datetime import datetime as dt, time as dt_time, timedelta
from typing import Dict, Any, Set, List

import pytz
import redis
from django.db import transaction, models, close_old_connections
from django.conf import settings
from trading.models import Account, CashBreakoutTrade
from trading.utils import get_redis_connection, get_kite

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# --- ENGINE CONSTANTS ---
ENTRY_OFFSET_PCT = 0.0001
STOP_OFFSET_PCT = 0.0002
BREAKOUT_MAX_CANDLE_PCT = 0.007
MAX_MONITORING_MINUTES = 6
HEARTBEAT_THRESHOLD = 30  # Seconds

# --- REDIS KEYS (Centralized) ---
KEY_BULL_SETTINGS = "algo:settings:bull"
KEY_BLACKLIST = "algo:blacklist"
KEY_ENGINE_ENABLED = "algo:engine:bull:enabled"
KEY_PANIC_TRIGGER = "algo:panic:bull"
PREV_DAY_HASH = "prev_day_ohlc"

class CashBreakoutClient:
    """
    Combined Robust Trading Engine (v3 Production)
    Integrates Race-Condition Protection, State-Machine Integrity, and Fail-Fast Scanning.
    """
    DAILY_RESET_TIME = dt_time(20, 0, 0)

    def __init__(self, account: Account):
        self.account = account
        self.kite = get_kite(account)
        self.redis_client = get_redis_connection()
        self.running = True
        
        # 1. In-Memory State (Cache for high-frequency access)
        self.open_trades: Dict[str, CashBreakoutTrade] = {}
        self.pending_trades: Dict[str, CashBreakoutTrade] = {}
        self.latest_prices: Dict[str, Dict[str, Any]] = {} 
        
        # 2. Redis Key Structure (Process Isolation)
        today_iso = dt.now(IST).date().isoformat()
        self.active_sym_set = f"cb:active:{self.account.id}"
        self.exiting_id_set = f"cb:exiting:{self.account.id}"
        self.entry_lock_prefix = f"cb:lock:entry:{self.account.id}"
        self.trade_count_key = f"cb:count:{self.account.id}:{today_iso}"
        self.daily_pnl_key = f"cb:pnl:{self.account.id}:{today_iso}"
        self.force_exit_set = f"cb:force_exit:{self.account.id}"

        # 3. Consumer Config
        self.group_name = f"CB_GROUP:{self.account.id}"
        self.consumer_name = f"CB_CONS:{threading.get_ident()}"
        
        # 4. Diagnostics & Cache
        self.last_data_beat = time.time()
        self.cached_settings = {}
        self.cached_blacklist = set()
        self.cached_engine_enabled = True
        self.last_cache_update = 0
        self.log_throttle = {}

        # Initialize
        self._ensure_consumer_group(getattr(settings, "BREAKOUT_CANDLE_STREAM", "candle_1m"), '0')
        self._ensure_consumer_group("tick_stream", '$')
        self._load_trades_from_db()
        self._update_global_cache(force=True)

    # --- INITIALIZATION & SYNC ---

    def _ensure_consumer_group(self, stream_key: str, start_id: str):
        try:
            self.redis_client.xgroup_create(stream_key, self.group_name, id=start_id, mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                logger.error(f"CB: Redis Group Error {stream_key}: {e}")

    def _load_trades_from_db(self):
        """Re-syncs engine memory with database state (Crucial for restarts)."""
        close_old_connections()
        today_start = IST.localize(dt.combine(dt.now(IST).date(), dt_time.min))
        
        qs = CashBreakoutTrade.objects.filter(
            account=self.account,
            created_at__gte=today_start,
            status__in=['OPEN', 'PENDING', 'PENDING_ENTRY', 'PENDING_EXIT']
        )
        
        self.open_trades.clear()
        self.pending_trades.clear()
        
        for t in qs:
            if t.status == 'PENDING':
                self.pending_trades[t.symbol] = t
            elif t.status in ['OPEN', 'PENDING_EXIT']:
                self.open_trades[t.symbol] = t
            self.redis_client.sadd(self.active_sym_set, t.symbol)
            
        logger.info(f"CB-INIT: Recovered {len(self.open_trades)} Open & {len(self.pending_trades)} Pending.")

    def _update_global_cache(self, force=False):
        """Throttled refresh of strategy settings from Redis."""
        if force or (time.time() - self.last_cache_update > 5):
            try:
                raw_settings = self.redis_client.get(KEY_BULL_SETTINGS)
                self.cached_settings = json.loads(raw_settings) if raw_settings else {}
                
                bl = self.redis_client.smembers(KEY_BLACKLIST)
                self.cached_blacklist = {b.decode() for b in bl} if bl else set()
                
                status = self.redis_client.get(KEY_ENGINE_ENABLED)
                self.cached_engine_enabled = (status.decode() == "1") if status else True
                
                self.last_cache_update = time.time()
            except Exception as e:
                logger.debug(f"Cache update failed: {e}")

    # --- UTILITIES ---

    def _smart_log(self, symbol: str, message: str, level=logging.INFO):
        now = time.time()
        if now - self.log_throttle.get(symbol, 0) > 10:
            logger.log(level, message)
            self.log_throttle[symbol] = now

    def _parse_ratio(self, ratio_str: str, default: float) -> float:
        try:
            return float(ratio_str.split(':')[1])
        except:
            return default

    def _get_pd_high(self, symbol: str) -> float:
        try:
            raw = self.redis_client.hget(PREV_DAY_HASH, symbol)
            if raw: return float(json.loads(raw).get("high", 0))
        except: pass
        return 0.0

    def _check_volume_criteria(self, vol, vol_sma, vol_price_cr) -> bool:
        criteria = self.cached_settings.get("volume_criteria", [])
        if not criteria: return True
        for c in criteria:
            try:
                if vol_sma >= float(c.get('min_sma_avg', 0)):
                    if vol >= (vol_sma * float(c.get('sma_multiplier', 2.0))):
                        if vol_price_cr >= float(c.get('min_vol_price_cr', 0)):
                            return True
            except: continue
        return False

    # --- CORE ENGINE LOGIC ---

    def _process_candle(self, payload: Dict[str, Any]):
        """
        Fast Scanner Logic. 
        Checks candidate validity before creating a DB record.
        """
        symbol = payload.get("symbol")
        if not symbol or not self.cached_engine_enabled or symbol in self.cached_blacklist:
            return

        # Double-check if already active (Memory + Redis)
        if symbol in self.open_trades or symbol in self.pending_trades:
            return
        if self.redis_client.sismember(self.active_sym_set, symbol):
            return

        try:
            close = float(payload['close']); open_p = float(payload['open'])
            high = float(payload['high']); low = float(payload['low'])
            vol = int(payload['volume']); vol_sma = float(payload.get('vol_sma_375', 0))
            vol_price_cr = float(payload.get('vol_price_cr', 0))
            ts_str = payload.get('ts')
            ts = dt.fromisoformat(ts_str).astimezone(IST) if ts_str else dt.now(IST)
        except Exception:
            return

        # Strategy: Bullish Breakout of Previous Day High
        if close <= open_p or close < 100: return
        if not self._check_volume_criteria(vol, vol_sma, vol_price_cr): return

        pd_high = self._get_pd_high(symbol)
        if pd_high <= 0 or not (open_p < pd_high < close):
            return

        # Candidate Validated -> Prepare Trade Levels
        entry = high * (1.0 + ENTRY_OFFSET_PCT)
        stop = low - (low * STOP_OFFSET_PCT)
        
        # Max Candle Size logic from Code 1
        candle_pct = (high - low) / close
        if candle_pct > BREAKOUT_MAX_CANDLE_PCT:
            # Use PDH as tighter stop for large candles
            stop = pd_high - (pd_high * STOP_OFFSET_PCT)

        rr = self._parse_ratio(self.cached_settings.get("risk_reward", "1:2"), 2.0)
        target = entry + (rr * (entry - stop))

        # Atomic Creation
        close_old_connections()
        try:
            with transaction.atomic():
                # Final DB-level check for trades per stock
                existing_count = CashBreakoutTrade.objects.filter(
                    account=self.account, symbol=symbol, created_at__date=dt.now(IST).date()
                ).exclude(status__in=['FAILED_ENTRY', 'EXPIRED']).count()
                
                if existing_count >= int(self.cached_settings.get("trades_per_stock", 2)):
                    return

                t = CashBreakoutTrade.objects.create(
                    account=self.account, user=self.account.user, symbol=symbol,
                    status='PENDING', candle_ts=ts, entry_level=entry,
                    stop_level=stop, target_level=target, volume_price=vol_price_cr
                )
                self.pending_trades[symbol] = t
                self.redis_client.sadd(self.active_sym_set, symbol)
                logger.info(f"CB: Setup Found {symbol} | Entry: {entry:.2f} | Stop: {stop:.2f}")
        except Exception as e:
            logger.error(f"CB: Creation Error {symbol}: {e}")

    def _try_enter_pending(self):
        """Attempts Buy Execution if Price > Entry Level."""
        if not self.pending_trades: return
        now = dt.now(IST)

        for symbol, trade in list(self.pending_trades.items()):
            # 1. Expiry Check
            if now > trade.candle_ts.astimezone(IST) + timedelta(minutes=MAX_MONITORING_MINUTES):
                trade.status = 'EXPIRED'; trade.save(); self.pending_trades.pop(symbol, None)
                self.redis_client.srem(self.active_sym_set, symbol); continue

            # 2. Price Check
            tick = self.latest_prices.get(symbol)
            if not tick or tick['ltp'] <= 0: continue
            ltp = tick['ltp']

            if ltp > trade.entry_level:
                # 3. RACE CONDITION PROTECTION: Redis Lock
                lock_key = f"{self.entry_lock_prefix}:{symbol}"
                if not self.redis_client.set(lock_key, "1", nx=True, ex=10):
                    continue

                try:
                    close_old_connections()
                    with transaction.atomic():
                        # DB Row Lock
                        t = CashBreakoutTrade.objects.select_for_update().get(id=trade.id)
                        if t.status != 'PENDING': 
                            self.pending_trades.pop(symbol, None)
                            continue

                        # Risk Calculation
                        risk = abs(ltp - t.stop_level)
                        risk_amt = float(self.cached_settings.get("risk_per_trade", 1000))
                        qty = max(1, int(floor(risk_amt / risk))) if risk > 0 else 1

                        # Order Placement
                        oid = self.kite.place_order(
                            variety=self.kite.VARIETY_REGULAR, tradingsymbol=symbol, 
                            exchange='NSE', transaction_type='BUY', quantity=qty, 
                            order_type='MARKET', product='MIS'
                        )
                        
                        t.status = 'PENDING_ENTRY'; t.quantity = qty; t.entry_order_id = oid; t.save()
                        self.pending_trades.pop(symbol, None)
                        logger.info(f"CB: BUY SENT {symbol} Qty:{qty} | Order:{oid}")
                except Exception as e:
                    logger.error(f"CB: Entry Order Failed {symbol}: {e}")
                finally:
                    self.redis_client.delete(lock_key)

    def monitor_trades(self):
        """Exits monitoring for OPEN trades."""
        # Global Panic Check
        if self.redis_client.exists(KEY_PANIC_TRIGGER):
            self.redis_client.delete(KEY_PANIC_TRIGGER)
            logger.warning("CB: PANIC BUTTON TRIGGERED.")
            for t in list(self.open_trades.values()): self._exit_trade(t, "Panic Triggered")

        for symbol, trade in list(self.open_trades.items()):
            # Manual Force Exit
            if self.redis_client.sismember(self.force_exit_set, str(trade.id)):
                self._exit_trade(trade, "Manual Exit")
                self.redis_client.srem(self.force_exit_set, str(trade.id)); continue

            tick = self.latest_prices.get(symbol)
            if not tick or tick['ltp'] <= 0: continue
            ltp = tick['ltp']

            self._smart_log(symbol, f"CB: Monitoring {symbol} | LTP: {ltp:.2f} | PnL: {(ltp-trade.entry_price)*trade.quantity:.2f}")

            # 1. Trailing SL
            self._handle_trailing(trade, ltp)

            # 2. Exit Triggers
            if ltp <= trade.stop_level:
                self._exit_trade(trade, "SL Hit")
            elif ltp >= trade.target_level:
                self._exit_trade(trade, "Target Hit")

    def _exit_trade(self, trade, reason: str):
        """Atomic Exit logic to prevent duplicate sell orders."""
        # Layer 1: Atomic Redis Set (Global across processes)
        if not self.redis_client.sadd(self.exiting_id_set, trade.id):
            return 

        close_old_connections()
        try:
            with transaction.atomic():
                # Layer 2: DB Row Lock
                t = CashBreakoutTrade.objects.select_for_update().get(id=trade.id)
                if t.status != 'OPEN': 
                    self.open_trades.pop(t.symbol, None)
                    return

                oid = self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR, tradingsymbol=t.symbol, 
                    exchange='NSE', transaction_type='SELL', quantity=t.quantity, 
                    order_type='MARKET', product='MIS'
                )
                
                t.status = 'PENDING_EXIT'; t.exit_reason = reason; t.exit_order_id = oid; t.save()
                
                # Layer 3: Immediate Memory Clear
                self.open_trades.pop(t.symbol, None)
                logger.info(f"CB: EXIT SENT {t.symbol} | Reason: {reason} | Order: {oid}")
        except Exception as e:
            logger.error(f"CB: Exit Failure {trade.symbol}: {e}")
            self.redis_client.srem(self.exiting_id_set, trade.id) # Re-enable for retry

    def _handle_trailing(self, trade, ltp):
        """Step-based trailing stop loss."""
        try:
            trail_mult = self._parse_ratio(self.cached_settings.get("trailing_sl", "1:1.5"), 1.5)
            # Calculated risk from actual entry
            risk = trade.entry_price - trade.stop_level
            if risk <= 0: return

            step = risk * trail_mult
            profit = ltp - trade.entry_price
            
            if profit >= step:
                levels = floor(profit / step)
                # Move SL to Entry + (N-1) steps
                new_sl = trade.entry_price + ((levels - 1) * step)
                new_sl = round(new_sl * 20) / 20 # Tick size
                
                if new_sl > trade.stop_level:
                    trade.stop_level = new_sl
                    trade.save(update_fields=['stop_level'])
                    logger.info(f"CB: TRAILED SL {trade.symbol} -> {new_sl}")
        except: pass

    # --- LOOPS & STREAMS ---

    def _reconcile_loop(self):
        """Ensures DB matches Kite reality."""
        while self.running:
            close_old_connections()
            try:
                qs = CashBreakoutTrade.objects.filter(account=self.account, status__in=['PENDING_ENTRY', 'PENDING_EXIT'])
                if qs.exists():
                    all_orders = {o['order_id']: o for o in self.kite.orders()}
                    for t in qs:
                        oid = t.entry_order_id if t.status == 'PENDING_ENTRY' else t.exit_order_id
                        if not oid or oid not in all_orders: continue
                        
                        ord_data = all_orders[oid]
                        status = ord_data['status']
                        
                        if status == 'COMPLETE':
                            fill = float(ord_data['average_price'])
                            with transaction.atomic():
                                obj = CashBreakoutTrade.objects.select_for_update().get(id=t.id)
                                if obj.status == 'PENDING_ENTRY':
                                    obj.status = 'OPEN'; obj.entry_price = fill; obj.entry_time = dt.now(IST); obj.save()
                                    self.open_trades[obj.symbol] = obj
                                    logger.info(f"CB-RECON: {obj.symbol} is now OPEN at {fill}")
                                else:
                                    obj.status = 'CLOSED'; obj.exit_price = fill; obj.exit_time = dt.now(IST)
                                    obj.pnl = (fill - obj.entry_price) * obj.quantity; obj.save()
                                    # Final cleanup
                                    self.redis_client.srem(self.active_sym_set, obj.symbol)
                                    self.redis_client.srem(self.exiting_id_set, obj.id)
                                    self.redis_client.incrbyfloat(self.daily_pnl_key, obj.pnl)
                                    logger.info(f"CB-RECON: {obj.symbol} CLOSED. PnL: {obj.pnl:.2f}")
                        
                        elif status in ['CANCELLED', 'REJECTED']:
                            with transaction.atomic():
                                obj = CashBreakoutTrade.objects.select_for_update().get(id=t.id)
                                if obj.status == 'PENDING_ENTRY':
                                    obj.status = 'FAILED_ENTRY'; obj.save()
                                    self.redis_client.srem(self.active_sym_set, obj.symbol)
                                else:
                                    # If exit failed, put back to OPEN to allow retry
                                    obj.status = 'OPEN'; obj.exit_order_id = None; obj.save()
                                    self.open_trades[obj.symbol] = obj
                                    self.redis_client.srem(self.exiting_id_set, obj.id)
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"CB Reconcile Error: {e}")
                time.sleep(5)

    def _listen_to_stream(self, key: str, is_candle: bool):
        """Background thread for Redis Stream consumption."""
        while self.running:
            try:
                msgs = self.redis_client.xreadgroup(self.group_name, self.consumer_name, {key: '>'}, count=100, block=1000)
                if msgs:
                    self.last_data_beat = time.time()
                    for _, messages in msgs:
                        for mid, fields in messages:
                            try:
                                if is_candle:
                                    raw = fields.get(b'data') or fields.get('data')
                                    if raw: self._process_candle(json.loads(raw.decode()))
                                else:
                                    sym = (fields.get(b'symbol') or fields.get('symbol')).decode()
                                    ltp = (fields.get(b'ltp') or fields.get('ltp')).decode()
                                    self.latest_prices[sym] = {'ltp': float(ltp), 'ts': dt.now(IST)}
                            except: pass
                            self.redis_client.xack(key, self.group_name, mid)
            except Exception as e:
                time.sleep(1)

    def run(self):
        """Main Orchestrator."""
        # Start Threads
        threading.Thread(target=self._listen_to_stream, args=(getattr(settings, "BREAKOUT_CANDLE_STREAM", "candle_1m"), True), daemon=True).start()
        threading.Thread(target=self._listen_to_stream, args=("tick_stream", False), daemon=True).start()
        threading.Thread(target=self._reconcile_loop, daemon=True).start()
        
        logger.info("CB-ENGINE: Robust System Online.")
        
        while self.running:
            try:
                # 1. Safety Heartbeat
                if time.time() - self.last_data_beat > HEARTBEAT_THRESHOLD:
                    logger.critical("CB-ENGINE: DATA INGESTION STALLED. EMERGENCY HALT.")
                    self.running = False; break

                # 2. Logic Execution
                self._update_global_cache()
                self._try_enter_pending()
                self.monitor_trades()
                
                time.sleep(0.005) # 200Hz tick
            except Exception as e:
                logger.error(f"CB-ENGINE: Main Loop Panic: {e}")
                time.sleep(1)

    def stop(self):
        self.running = False
        logger.info("CB-ENGINE: Stopping...")