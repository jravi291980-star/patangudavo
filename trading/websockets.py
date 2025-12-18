# # """
# # Refactored Websocket - STRICT FILTERING (UI + ALGO)
# # - LOGIC: ALL Data (Streams, PubSub, Snapshots) is filtered by Volume SMA.
# # - RESULT: Redis contains ONLY relevant stocks. UI shows ONLY relevant stocks.
# # - PERFORMANCE: Maximum reduction in CPU, Bandwidth, and Storage.
# # """

# # import json
# # import logging
# # import time
# # import math
# # import threading
# # from datetime import datetime as dt, date, time as datetime_time, timedelta
# # from typing import Optional, Any, List, Set

# # import pytz
# # import redis
# # from kiteconnect import KiteTicker
# # from tenacity import retry, stop_after_attempt, wait_exponential

# # from django.conf import settings
# # from trading.models import Account
# # from trading.utils import get_kite, get_redis_connection

# # logger = logging.getLogger(__name__)
# # IST = pytz.timezone('Asia/Kolkata')
# # redis_client = get_redis_connection()

# # # --- REDIS KEYS ---
# # CANDLE_STREAM_KEY = 'candle_1m'       # Filtered
# # TICK_STREAM_KEY = 'tick_stream'       # Filtered
# # DATA_HEARTBEAT_KEY = "algo:data:heartbeat"
# # FIXED_SMA_KEY = "algo:fixed_vol_sma"
# # FIXED_SMA_PERIOD = 1875 

# # KEY_BULL_SETTINGS = "algo:settings:bull"
# # KEY_BEAR_SETTINGS = "algo:settings:bear"
# # FIRST_CANDLE_PREFIX = "first_candle_915"

# # # --- HELPER FUNCTIONS ---

# # def parse_date_safe(expiry_raw) -> Optional[date]:
# #     if not expiry_raw: return None
# #     if isinstance(expiry_raw, date) and not isinstance(expiry_raw, dt): return expiry_raw
# #     if isinstance(expiry_raw, dt): return expiry_raw.date()
# #     s = str(expiry_raw).strip()
# #     fmts = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d-%b-%Y", "%d%b%Y", "%d-%b-%y"]
# #     for f in fmts:
# #         try: return dt.strptime(s, f).date()
# #         except Exception: continue
# #     try: return dt.fromisoformat(s).date()
# #     except Exception: return None

# # def safe_int(x, default=None):
# #     try: return int(x)
# #     except Exception:
# #         try: return int(float(x))
# #         except Exception: return default

# # def _default_json_serializer(obj):
# #     try:
# #         if isinstance(obj, (date, dt)): return obj.isoformat()
# #         if isinstance(obj, float):
# #             if math.isinf(obj) or math.isnan(obj): return None
# #             return obj
# #         return str(obj)
# #     except Exception: return None

# # # --- PREVIOUS DAY CACHE ---

# # def cache_previous_day_hl(master_account=None):
# #     if not redis_client: return
# #     try:
# #         if not master_account:
# #             master_account = Account.objects.filter(is_master=True, user__is_superuser=True).first()
# #         if not master_account: return

# #         kite = get_kite(master_account)
# #         instrument_map_json = redis_client.get('instrument_map')
# #         if not instrument_map_json: return

# #         if isinstance(instrument_map_json, (bytes, bytearray)):
# #             instrument_map_json = instrument_map_json.decode('utf-8')

# #         instr_map = json.loads(instrument_map_json)
# #         today = dt.now(IST).date()
# #         prev_day = today - timedelta(days=1)
# #         while prev_day.weekday() >= 5: prev_day -= timedelta(days=1)

# #         symbols = list(settings.STOCK_INDEX_MAPPING.keys())
# #         pipe = redis_client.pipeline()
# #         stored = 0

# #         for symbol in symbols:
# #             token = None
# #             for t, data in instr_map.items():
# #                 if (data.get('symbol') == symbol and data.get('exchange') == 'NSE' and data.get('instrument_type') in ('EQ', 'EQUITY')):
# #                     token = safe_int(t)
# #                     break
# #             if not token: continue

# #             try:
# #                 hist = kite.historical_data(instrument_token=token, from_date=prev_day, to_date=prev_day, interval='day')
# #                 if not hist: continue
# #                 row = hist[-1]
# #                 high = float(row.get('high', 0.0) or 0.0)
# #                 low = float(row.get('low', 0.0) or 0.0)
# #                 close = float(row.get('close', 0.0) or 0.0)
# #                 payload = json.dumps({'date': prev_day.isoformat(), 'high': high, 'low': low, 'close': close}, default=_default_json_serializer)
# #                 pipe.hset('prev_day_ohlc', symbol, payload)
# #                 stored += 1
# #             except Exception: continue

# #         if stored: pipe.execute()
# #     except Exception as e: logger.error(f"PREV_HL: Overall failure: {e}")

# # # --- SMA CALCULATION ---
# # def cache_instruments_for_day():
# #     """Fetches NSE instruments from Kite and caches them in Redis."""
# #     logger.info("WEBSOCKET_CACHE: Starting daily unified instrument caching process (EQUITY ONLY)...")
# #     try:
# #         master_account = Account.objects.filter(is_master=True, user__is_superuser=True).first()
# #         if not master_account:
# #             logger.error("WEBSOCKET_CACHE: No master account found.")
# #             return

# #         kite = get_kite(master_account)

# #         logger.info("WEBSOCKET_CACHE: Fetching NSE instruments...")
# #         nse_instruments = kite.instruments("NSE") or []
# #         all_instruments = nse_instruments 

# #         symbols_to_fetch = list(settings.STOCK_INDEX_MAPPING.keys()) + list(settings.INDEX_SYMBOLS)
# #         full_instrument_symbols = [f"NSE:{s}" for s in symbols_to_fetch]

# #         initial_quotes = {}
# #         try:
# #             if full_instrument_symbols:
# #                 initial_quotes = kite.quote(full_instrument_symbols) or {}
# #         except Exception: pass

# #         instrument_map = {}
# #         circuit_limits = {}

# #         for inst in all_instruments:
# #             token_val = inst.get('instrument_token') or inst.get('token')
# #             if not token_val: continue
# #             token_str = str(token_val)

# #             symbol = (inst.get('tradingsymbol') or inst.get('symbol') or "").strip()
# #             exchange = inst.get('exchange') or inst.get('segment') or ""
# #             instrument_type = 'EQ'

# #             expiry = parse_date_safe(inst.get('expiry'))
# #             strike = safe_int(inst.get('strike'))
# #             name = inst.get('name', '')
# #             full_symbol = f"{exchange}:{symbol}"

# #             instrument_data = {
# #                 'symbol': symbol, 'exchange': exchange, 'segment': inst.get('segment', exchange),
# #                 'instrument_type': instrument_type, 'lot_size': inst.get('lot_size', 1), 'tick_size': inst.get('tick_size', 0.05),
# #                 'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0, 'ltp': 0.0, 'change': 0.0, 'volume': 0,
# #                 'expiry': expiry, 'strike': strike, 'name': name,
# #             }
# #             if inst.get('instrument_type') in ('EQ', 'EQUITY'):
# #                 circuit_limits[symbol] = {
# #                     'upper': inst.get('upper_circuit_limit', float('inf')),
# #                     'lower': inst.get('lower_circuit_limit', 0)
# #                 }

# #             if full_symbol in initial_quotes:
# #                 quote = initial_quotes[full_symbol] or {}
# #                 ohlc = quote.get('ohlc') or {}
# #                 ltp = quote.get('last_price') or 0.0
# #                 prev_close = ohlc.get('close', 0.0)
# #                 seeded_volume = ohlc.get('volume') or quote.get('volume') or 0

# #                 instrument_data.update({
# #                     'close': prev_close, 'ltp': ltp,
# #                     'change': ((ltp - prev_close) / prev_close * 100) if prev_close else 0.0,
# #                     'volume': int(seeded_volume or 0), 'open': ohlc.get('open', 0.0),
# #                     'high': max(ohlc.get('high', 0.0), ltp),
# #                     'low': min(ohlc.get('low', float('inf')), ltp) if ltp > 0 else 0.0,
# #                 })
# #                 if instrument_data['low'] == 0.0 and ltp > 0: instrument_data['low'] = ltp

# #             instrument_map[token_str] = instrument_data

# #         pipe = redis_client.pipeline()
# #         pipe.set('instrument_map', json.dumps(instrument_map, default=_default_json_serializer))
# #         pipe.set('circuit_limits', json.dumps(circuit_limits, default=_default_json_serializer))
# #         pipe.execute()
# #         logger.info(f"WEBSOCKET_CACHE: Successfully cached {len(instrument_map)} total instruments.")
# #     except Exception as e:
# #         logger.error(f"WEBSOCKET_CACHE: Failed to cache instruments: {e}", exc_info=True)


# # def calculate_and_cache_fixed_volume_sma():
# #     if not redis_client: return
    
# #     logger.info(f"SMA_CACHE: Calculating fixed SMA (Period: {FIXED_SMA_PERIOD})...")
# #     symbols = list(settings.STOCK_INDEX_MAPPING.keys())
# #     fixed_sma_map = {}
    
# #     for symbol in symbols:
# #         key = f'candles:1m:{symbol}'
# #         try:
# #             raw_candles = redis_client.lrange(key, -FIXED_SMA_PERIOD, -1)
# #             total_vol = 0
# #             if raw_candles:
# #                 for raw in raw_candles:
# #                     try:
# #                         c_data = json.loads(raw)
# #                         total_vol += int(c_data.get('volume', 0))
# #                     except Exception: pass
            
# #             avg_vol = int(total_vol / FIXED_SMA_PERIOD)
# #             fixed_sma_map[symbol] = avg_vol
# #         except Exception: fixed_sma_map[symbol] = 0

# #     if fixed_sma_map:
# #         try:
# #             redis_client.delete(FIXED_SMA_KEY)
# #             redis_client.hset(FIXED_SMA_KEY, mapping=fixed_sma_map)
# #             logger.info(f"SMA_CACHE: Cached fixed SMA for {len(fixed_sma_map)} symbols.")
# #         except Exception: pass
        
# # # --- Cache instruments ---
# # def get_instrument_map_from_cache():
# #     if not redis_client:
# #         return {}
# #     instrument_map_json = redis_client.get('instrument_map')
# #     if instrument_map_json:
# #         try:
# #             if isinstance(instrument_map_json, (bytes, bytearray)):
# #                 instrument_map_json = instrument_map_json.decode('utf-8')
# #             return json.loads(instrument_map_json)
# #         except Exception:
# #             return {}
# #     return {}
# # # --- CANDLE AGGREGATOR ---

# # class CandleAggregator:
# #     def __init__(self, redis_client, filter_criteria, qualified_set_ref, whitelist_ref, max_candles=10000):
# #         self.redis = redis_client
# #         self.max_candles = max_candles
# #         self.active_candle = {}
# #         self.last_total_volume = {}
# #         self.lock = threading.Lock()
        
# #         self.fixed_sma_map = {}
# #         self.filter_criteria = filter_criteria 
        
# #         # References to Shared Sets
# #         self.qualified_symbols = qualified_set_ref
# #         self.whitelist_ref = whitelist_ref
        
# #         self.load_fixed_sma()

# #     def load_fixed_sma(self):
# #         try:
# #             raw_map = self.redis.hgetall(FIXED_SMA_KEY)
# #             if raw_map:
# #                 self.fixed_sma_map = {
# #                     (k.decode('utf-8') if isinstance(k, bytes) else k): int(safe_int(v, 0)) 
# #                     for k, v in raw_map.items()
# #                 }
# #         except Exception: pass

# #     def _is_trading_minute(self, ts: dt) -> bool:
# #         t = ts.astimezone(IST).time()
# #         start = datetime_time(9, 15)
# #         end = datetime_time(15, 30)
# #         return (t >= start) and (t < end)

# #     def _bucket_for(self, ts: dt) -> dt:
# #         if ts.tzinfo is None: ts = pytz.utc.localize(ts)
# #         ts_ist = ts.astimezone(IST)
# #         return ts_ist.replace(second=0, microsecond=0)

# #     # --- THE QUALIFICATION LOGIC ---
# #     def is_qualified(self, symbol, vol, close):
# #         """
# #         Determines if a stock should be processed (UI + Algo).
# #         """
# #         # 1. Fast Path
# #         if symbol in self.qualified_symbols: return True
# #         if symbol in self.whitelist_ref: 
# #             self.qualified_symbols.add(symbol)
# #             return True

# #         # 2. Check Criteria
# #         if not self.filter_criteria: return True
        
# #         sma = self.fixed_sma_map.get(symbol, 0)
# #         if close <= 0: return False

# #         vol_price_cr = round((vol * close) / 10000000.0, 4)

# #         for criteria in self.filter_criteria:
# #             try:
# #                 min_vp = criteria.get('min_vp', 0)
# #                 min_sma = criteria.get('min_sma', 0)
# #                 mult = criteria.get('mult', 0)
                
# #                 if vol_price_cr >= min_vp:
# #                     if sma >= min_sma and vol >= (sma * mult):
# #                         self.qualified_symbols.add(symbol) # LATCH
# #                         return True
# #             except: continue
# #         return False

# #     def process_tick(self, symbol, ltp, ts, cumulative_volume):
# #         if not symbol or ltp is None: return
# #         try: self.redis.set(DATA_HEARTBEAT_KEY, int(time.time()), ex=10)
# #         except: pass

# #         if not self._is_trading_minute(ts): return
# #         bucket = self._bucket_for(ts)
        
# #         with self.lock:
# #             active = self.active_candle.get(symbol)
# #             delta_vol = 0
            
# #             try:
# #                 if cumulative_volume is not None:
# #                     cur_total = int(cumulative_volume)
# #                     prev_total = self.last_total_volume.get(symbol)
# #                     delta_vol = max(0, cur_total - prev_total) if prev_total is not None else 0
# #                     self.last_total_volume[symbol] = cur_total
# #                 else: delta_vol = 0
# #             except: delta_vol = 0

# #             if active and active['bucket'] != bucket:
# #                 self._publish_and_store(symbol, active)
# #                 self.active_candle[symbol] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': int(delta_vol)}
# #             elif not active:
# #                 self.active_candle[symbol] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': int(delta_vol)}
# #             else:
# #                 c = active
# #                 c['high'] = max(c['high'], ltp)
# #                 c['low'] = min(c['low'], ltp)
# #                 c['close'] = ltp
# #                 c['volume'] = int(c.get('volume', 0) + delta_vol)
            
# #             # --- LIVE SNAPSHOT UPDATE (FILTERED) ---
# #             # Even the UI Live Candle is filtered now!
# #             # It will only appear in Redis if qualified.
# #             try:
# #                 cur = self.active_candle[symbol]
# #                 # Check qualification using CURRENT total volume (approx)
# #                 # Since 'cumulative_volume' is raw, we use that for filtering check
# #                 is_valid = self.is_qualified(symbol, int(cur.get('volume', 0)), ltp)
                
# #                 if is_valid:
# #                     payload = {'ts': cur['bucket'].isoformat(), 'open': cur['open'], 'high': cur['high'], 'low': cur['low'], 'close': cur['close'], 'volume': cur['volume']}
# #                     self.redis.set(f'current_1m_candle:{symbol}', json.dumps(payload, default=_default_json_serializer))
# #             except: pass

# #     def _publish_and_store(self, symbol, candle):
# #         vol = int(candle.get('volume', 0))
# #         close = float(candle['close'])
        
# #         # --- STRICT FILTERING (ALGO) ---
# #         if not self.is_qualified(symbol, vol, close):
# #             return 
# #         # -------------------------------

# #         vol_sma = self.fixed_sma_map.get(symbol, 0)
# #         vol_price_cr = round((vol * close) / 10000000.0, 4)
        
# #         payload = {
# #             'symbol': symbol, 'interval': '1m', 'ts': candle['bucket'].isoformat(),
# #             'open': candle['open'], 'high': candle['high'], 'low': candle['low'], 
# #             'close': candle['close'], 'volume': vol,
# #             'vol_sma_375': vol_sma, 'vol_price_cr': vol_price_cr,
# #         }
        
# #         try:
# #             key = f'candles:1m:{symbol}'
# #             self.redis.rpush(key, json.dumps(payload, default=_default_json_serializer))
# #             self.redis.ltrim(key, -self.max_candles, -1)
            
# #             if candle['bucket'].time() == datetime_time(9, 15):
# #                 self.redis.set(f"{FIRST_CANDLE_PREFIX}:{symbol}", json.dumps(payload, default=_default_json_serializer), ex=86400)

# #             try: self.redis.xadd(CANDLE_STREAM_KEY, {'data': json.dumps(payload, default=_default_json_serializer)}, maxlen=self.max_candles, approximate=True)
# #             except TypeError: self.redis.xadd(CANDLE_STREAM_KEY, {'data': json.dumps(payload, default=_default_json_serializer)})

# #         except Exception as e: logger.error(f"AGG: Finalize Error {symbol}: {e}")

# #     def flush_all(self):
# #         with self.lock:
# #             keys = list(self.active_candle.keys())
# #             for k in keys:
# #                 c = self.active_candle.pop(k, None)
# #                 if c: self._publish_and_store(k, c)

# # # --- LIVE TICKER ---

# # class LiveTickerManager:
# #     def __init__(self, api_key, access_token, account_id):
# #         self.kws = KiteTicker(api_key, access_token)
# #         self.account_id = account_id
# #         self.redis_client = get_redis_connection()
# #         self.instrument_map = {}
# #         self.subscribed_tokens = set()
# #         self.last_publish_time = 0
# #         self.running = True
        
# #         # --- QUALIFIED SETS ---
# #         self.qualified_symbols = set() 
# #         self.whitelist = set()
# #         self.last_whitelist_update = 0
        
# #         self.filter_criteria = self._load_strategy_criteria()
        
# #         self.candle_agg = CandleAggregator(self.redis_client, self.filter_criteria, self.qualified_symbols, self.whitelist, max_candles=5000)

# #         self.kws.on_ticks = self.on_ticks
# #         self.kws.on_connect = self.on_connect
# #         self.kws.on_close = self.on_close
# #         self.kws.on_error = self.on_error
# #         self.kws.on_order_update = self.on_order_update

# #     def _load_strategy_criteria(self):
# #         """Fetches Settings at Startup."""
# #         criteria = []
# #         try:
# #             bull_raw = self.redis_client.get(KEY_BULL_SETTINGS)
# #             if bull_raw:
# #                 bull = json.loads(bull_raw).get('volume_criteria', [])
# #                 for c in bull: criteria.append({'min_vp': float(c.get('min_vol_price_cr', 0)), 'min_sma': float(c.get('min_sma_avg', 0)), 'mult': float(c.get('sma_multiplier', 0))})
            
# #             bear_raw = self.redis_client.get(KEY_BEAR_SETTINGS)
# #             if bear_raw:
# #                 bear = json.loads(bear_raw).get('volume_criteria', [])
# #                 for c in bear: criteria.append({'min_vp': float(c.get('min_vol_price_cr', 0)), 'min_sma': float(c.get('min_sma_avg', 0)), 'mult': float(c.get('sma_multiplier', 0))})
            
# #             logger.info(f"WS_INIT: Loaded {len(criteria)} volume criteria sets.")
# #         except: pass
# #         return criteria

# #     def _refresh_whitelist(self):
# #         """Syncs active trade list from Redis."""
# #         if time.time() - self.last_whitelist_update < 2: return 
# #         try:
# #             keys = [
# #                 f"breakout_active_entries:{self.account_id}",
# #                 f"breakdown_active_entries:{self.account_id}",
# #                 f"mom_bull_active_entries:{self.account_id}",
# #                 f"mom_bear_active_entries:{self.account_id}"
# #             ]
# #             active_symbols = self.redis_client.sunion(keys)
# #             new_whitelist = {s.decode('utf-8') if isinstance(s, bytes) else s for s in active_symbols} if active_symbols else set()
# #             self.whitelist.clear()
# #             self.whitelist.update(new_whitelist)
# #             self.last_whitelist_update = time.time()
# #         except: pass

# #     def _load_instruments_from_cache(self) -> bool:
# #         try:
# #             instrument_map_json = self.redis_client.get('instrument_map')
# #             if instrument_map_json:
# #                 if isinstance(instrument_map_json, (bytes, bytearray)):
# #                     instrument_map_json = instrument_map_json.decode('utf-8')
# #                 full_map = json.loads(instrument_map_json)
# #                 self.instrument_map = {k: v for k, v in full_map.items() if v.get('exchange') == 'NSE' or v.get('symbol') in settings.INDEX_SYMBOLS}
# #                 logger.info(f"WEBSOCKET: Loaded {len(self.instrument_map)} Cash/Index instruments.")
# #                 return True
# #             return False
# #         except Exception: return False

# #     def on_order_update(self, ws, order):
# #         try:
# #             user_id = order.get('user_id')
# #             if not user_id: return
# #             account = Account.objects.filter(user__username=user_id).first()
# #             if not account: return
# #             channel = f"order_updates:{account.id}"
# #             message = json.dumps({
# #                 'order_id': order.get('order_id'), 'status': order.get('status'), 'status_message': order.get('status_message'),
# #                 'filled_quantity': order.get('filled_quantity', 0), 'average_price': order.get('average_price', 0.0),
# #             })
# #             self.redis_client.publish(channel, message)
# #         except Exception: pass

# #     def on_ticks(self, ws, ticks):
# #         self._refresh_whitelist()
        
# #         pipe = self.redis_client.pipeline()
# #         has_stream_data = False

# #         for tick in ticks:
# #             token = tick.get('instrument_token') or tick.get('token')
# #             if token is None: continue
# #             token_str = str(token)
# #             if token_str not in self.instrument_map: continue
# #             instrument = self.instrument_map[token_str]

# #             ltp_raw = tick.get('last_price') or tick.get('ltp') or instrument.get('ltp', 0.0)
# #             try: instrument['ltp'] = float(ltp_raw)
# #             except Exception: instrument['ltp'] = float(instrument.get('ltp', 0.0) or 0.0)

# #             # (OHLC Updates)
# #             if 'ohlc' in tick and isinstance(tick['ohlc'], dict):
# #                 ohlc = tick['ohlc']
# #                 instrument['open'] = ohlc.get('open', instrument.get('open', 0.0))
# #                 instrument['high'] = ohlc.get('high', instrument.get('high', 0.0))
# #                 instrument['low'] = ohlc.get('low', instrument.get('low', 0.0))
# #                 instrument['close'] = ohlc.get('close', instrument.get('close', 0.0))
# #             else:
# #                 instrument['open'] = tick.get('open') or instrument.get('open', 0.0)
# #                 instrument['high'] = tick.get('high') or instrument.get('high', 0.0)
# #                 instrument['low'] = tick.get('low') or instrument.get('low', 0.0)
# #                 instrument['close'] = tick.get('close') or instrument.get('close', 0.0)

# #             if instrument['ltp'] > 0.0:
# #                 instrument['high'] = max(instrument.get('high', 0.0), instrument['ltp'])
# #                 cur_low = instrument.get('low', float('inf'))
# #                 if cur_low == 0.0 or cur_low == float('inf') or instrument['ltp'] < cur_low: instrument['low'] = instrument['ltp']

# #             raw_vol = tick.get('volume_traded') or tick.get('total_traded_quantity')
# #             if raw_vol is not None:
# #                 try: instrument['volume'] = int(raw_vol)
# #                 except Exception: pass

# #             prev = instrument.get('close', 0.0)
# #             if prev:
# #                 try: instrument['change'] = ((instrument['ltp'] - prev) / prev) * 100
# #                 except Exception: pass

# #             try:
# #                 sym = instrument.get('symbol')
# #                 if sym and sym in settings.STOCK_INDEX_MAPPING and instrument.get('exchange') == 'NSE':
# #                     ts_raw = tick.get('timestamp') or tick.get('tradable_at')
# #                     if ts_raw:
# #                         try:
# #                             if isinstance(ts_raw, dt): ts = ts_raw
# #                             else: ts = dt.fromisoformat(str(ts_raw).replace('Z', '+00:00'))
# #                             if ts.tzinfo is None: ts = pytz.utc.localize(ts).astimezone(IST)
# #                         except Exception: ts = dt.now(IST)
# #                     else: ts = dt.now(IST)

# #                     cum_vol = int(raw_vol) if raw_vol is not None else None
                    
# #                     # 1. Update Candle (Takes care of Candle Stream Filtering inside)
# #                     self.candle_agg.process_tick(sym, float(instrument['ltp'] or 0.0), ts, cum_vol)
                    
# #                     # 2. TICK STREAM FILTER (SAFE)
# #                     # Use CandleAggregator logic to check if Qualified
# #                     is_valid = self.candle_agg.is_qualified(sym, instrument['volume'], instrument['ltp'])
                    
# #                     if is_valid:
# #                         stream_data = {
# #                             'symbol': sym,
# #                             'ltp': str(instrument['ltp']),
# #                             'volume': str(instrument['volume']),
# #                             'ts': ts.isoformat()
# #                         }
# #                         pipe.xadd(TICK_STREAM_KEY, stream_data, maxlen=100000, approximate=True)
# #                         has_stream_data = True
# #             except Exception: pass

# #         if has_stream_data:
# #             pipe.execute()

# #         # UI Updates (Filtered)
# #         now = time.time()
# #         if now - self.last_publish_time > 0.1:
# #             self.publish_to_redis()
# #             self.last_publish_time = now

# #     def publish_to_redis(self):
# #         try:
# #             pipe = self.redis_client.pipeline()
# #             live_ohlc = {}
# #             stocks = []
            
# #             for token, data in self.instrument_map.items():
# #                 sym = data.get('symbol')
# #                 if not sym or data.get('ltp', 0.0) <= 0.0: continue
                
# #                 # --- STRICT UI FILTER ---
# #                 # Check if symbol is in the Qualified Set or Whitelist
# #                 # Index Symbols are ALWAYS allowed
# #                 is_index = sym in settings.INDEX_SYMBOLS
# #                 if not is_index and sym not in self.qualified_symbols and sym not in self.whitelist:
# #                     continue
# #                 # ------------------------

# #                 live_ohlc[sym] = {
# #                     'ltp': data.get('ltp', 0.0), 'open': data.get('open', 0.0), 'high': data.get('high', 0.0),
# #                     'low': data.get('low', 0.0), 'close': data.get('close', 0.0), 'change': data.get('change', 0.0),
# #                     'sector': settings.STOCK_INDEX_MAPPING.get(sym, 'N/A'), 'volume': int(data.get('volume', 0)),
# #                 }
                
# #                 if sym in settings.STOCK_INDEX_MAPPING and data.get('exchange') == 'NSE':
# #                     stocks.append({
# #                         'symbol': sym, 'ltp': data['ltp'], 'change': data['change'],
# #                         'day_open': data['open'], 'day_high': data['high'], 'day_low': data['low'],
# #                         'sector': settings.STOCK_INDEX_MAPPING.get(sym, 'N/A'), 'volume': int(data.get('volume', 0)),
# #                     })

# #             if live_ohlc:
# #                 pipe.set('live_ohlc_data', json.dumps(live_ohlc, default=_default_json_serializer))
# #                 # Filter PubSub too
# #                 # Note: We reconstruct list based on `live_ohlc` keys to ensure consistency
# #                 ticks_list = [{'instrument_token': t, 'last_price': d.get('ltp',0), 'volume': int(d.get('volume',0))} 
# #                               for t, d in self.instrument_map.items() if d.get('symbol') in live_ohlc]
# #                 pipe.publish('ticks_channel', json.dumps(ticks_list))

# #             stocks.sort(key=lambda x: x['change'], reverse=True)
# #             indices = [v for v in self.instrument_map.values() if v.get('symbol') in settings.INDEX_SYMBOLS and v.get('ltp', 0) > 0]
# #             indices.sort(key=lambda x: x['change'], reverse=True)
            
# #             pipe.set('top_gainers', json.dumps(stocks[:30], default=_default_json_serializer))
# #             pipe.set('top_losers', json.dumps(stocks[-30:][::-1], default=_default_json_serializer))
# #             pipe.set('top_sectors', json.dumps(indices[:10], default=_default_json_serializer))
# #             pipe.set('bottom_sectors', json.dumps(indices[-10:][::-1], default=_default_json_serializer))
# #             pipe.execute()
# #         except Exception: pass

# #     # ... (Keep on_connect, on_close, on_error, start/stop_connection) ...
# #     def on_connect(self, ws, response):
# #         logger.info("WEBSOCKET: Connected.")
# #         self.redis_client.set('websocket_status', 'connected')
# #         st = {safe_int(t) for t, d in self.instrument_map.items() if d.get('symbol') in settings.STOCK_INDEX_MAPPING and d.get('exchange') == 'NSE'}
# #         it = {safe_int(t) for t, d in self.instrument_map.items() if d.get('symbol') in settings.INDEX_SYMBOLS}
# #         try:
# #             target = self.redis_client.hkeys('prev_day_ohlc')
# #             if target:
# #                 decoded = {s.decode('utf-8') if isinstance(s, bytes) else str(s) for s in target}
# #                 for t, d in self.instrument_map.items():
# #                     if d.get('symbol') in decoded and d.get('exchange') == 'NSE': st.add(safe_int(t))
# #         except Exception: pass
# #         subs = list((st | it) - {None})
# #         self.subscribed_tokens = set(subs)
# #         if subs:
# #             try: ws.subscribe(subs); ws.set_mode(ws.MODE_FULL, subs); logger.info(f"WEBSOCKET: Subscribed to {len(subs)} tokens.")
# #             except Exception: pass

# #     def on_close(self, ws, code, reason):
# #         logger.warning(f"WEBSOCKET: Closed. Code: {code}, Reason: {reason}")
# #         self.redis_client.set('websocket_status', 'disconnected')

# #     def on_error(self, ws, code, reason):
# #         logger.error(f"WEBSOCKET: Error. Code: {code}, Reason: {reason}")

# #     @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
# #     def start_connection(self):
# #         logger.info("WEBSOCKET: Starting connection...")
# #         self.kws.connect(threaded=True)

# #     def stop_connection(self):
# #         try:
# #             if getattr(self.kws, 'is_connected', lambda: False)() or getattr(self.kws, 'is_connecting', lambda: False)():
# #                 self.kws.close(1000, "Manual stop")
# #         except Exception: pass

# #     def run(self):
# #         if not self._load_instruments_from_cache():
# #             logger.warning("WEBSOCKET: Missing instrument cache. Rebuilding...")
# #             try: cache_instruments_for_day(); self._load_instruments_from_cache()
# #             except Exception: return
        
# #         if not self.redis_client.exists('prev_day_ohlc'):
# #             try: cache_previous_day_hl()
# #             except Exception: pass
        
# #         try: calculate_and_cache_fixed_volume_sma(); self.candle_agg.load_fixed_sma()
# #         except Exception as e: logger.error(f"WEBSOCKET: Fixed SMA Calc Failed: {e}")

# #         while self.running:
# #             connect_time = datetime_time(9, 10); disconnect_time = datetime_time(15, 35)
# #             now = dt.now(IST); today = now.date()
# #             if today.weekday() >= 5: time.sleep(3600); continue
# #             target = IST.localize(dt.combine(today, connect_time))
# #             if now < target:
# #                 sleep_sec = (target - now).total_seconds()
# #                 if sleep_sec > 0: time.sleep(sleep_sec)
            
# #             if dt.now(IST).time() < disconnect_time:
# #                 if not getattr(self.kws, 'is_connected', lambda: False)():
# #                     try: self.start_connection()
# #                     except Exception: pass
# #                 while dt.now(IST).time() < disconnect_time and self.running:
# #                     if not getattr(self.kws, 'is_connected', lambda: False)():
# #                         try: self.start_connection()
# #                         except Exception: pass
# #                     time.sleep(30)
            
# #             try: self.candle_agg.flush_all()
# #             except Exception: pass
# #             if getattr(self.kws, 'is_connected', lambda: False)(): self.stop_connection()
# #             time.sleep(60)

# #     def stop(self):
# #         self.running = False
# #         try: self.stop_connection()
# #         except Exception: pass

# # def start_websocket_thread(master_account):
# #     if not redis_client: return
# #     tm = LiveTickerManager(master_account.api_key, master_account.access_token, master_account.id)
# #     t = threading.Thread(target=tm.run, daemon=True)
# #     t.start()
# #     return tm

# """
# Refactored Websocket - STRICT FILTERING (ALGO ONLY)
# - LOGIC: ALL Data (Streams, PubSub, Snapshots) is filtered by Volume SMA.
# - SMA SOURCE: Externally seeded via 'seed_daily_volume' command.
# - RESULT: Redis contains ONLY relevant stocks for Algo Trading. No Dashboard fluff.
# """

import json
import logging
import time
import math
import threading
from datetime import datetime as dt, date, time as datetime_time, timedelta
from typing import Optional, Any, List, Set

import pytz
import redis
from kiteconnect import KiteTicker
from tenacity import retry, stop_after_attempt, wait_exponential

from django.conf import settings
from trading.models import Account
from trading.utils import get_kite, get_redis_connection

logger = logging.getLogger(__name__)
IST = pytz.timezone('Asia/Kolkata')
redis_client = get_redis_connection()

# --- REDIS KEYS ---
CANDLE_STREAM_KEY = 'candle_1m'       # Filtered
TICK_STREAM_KEY = 'tick_stream'       # Filtered
DATA_HEARTBEAT_KEY = "algo:data:heartbeat"
FIXED_SMA_KEY = "algo:fixed_vol_sma"  # Populated by management command

KEY_BULL_SETTINGS = "algo:settings:bull"
KEY_BEAR_SETTINGS = "algo:settings:bear"
FIRST_CANDLE_PREFIX = "first_candle_915"

# --- HELPER FUNCTIONS ---

def parse_date_safe(expiry_raw) -> Optional[date]:
    if not expiry_raw: return None
    if isinstance(expiry_raw, date) and not isinstance(expiry_raw, dt): return expiry_raw
    if isinstance(expiry_raw, dt): return expiry_raw.date()
    s = str(expiry_raw).strip()
    fmts = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d-%b-%Y", "%d%b%Y", "%d-%b-%y"]
    for f in fmts:
        try: return dt.strptime(s, f).date()
        except Exception: continue
    try: return dt.fromisoformat(s).date()
    except Exception: return None

def safe_int(x, default=None):
    try: return int(x)
    except Exception:
        try: return int(float(x))
        except Exception: return default

def _default_json_serializer(obj):
    try:
        if isinstance(obj, (date, dt)): return obj.isoformat()
        if isinstance(obj, float):
            if math.isinf(obj) or math.isnan(obj): return None
            return obj
        return str(obj)
    except Exception: return None

# --- PREVIOUS DAY CACHE ---

def cache_previous_day_hl(master_account=None):
    if not redis_client: return
    try:
        if not master_account:
            master_account = Account.objects.filter(is_master=True, user__is_superuser=True).first()
        if not master_account: return

        kite = get_kite(master_account)
        instrument_map_json = redis_client.get('instrument_map')
        if not instrument_map_json: return

        if isinstance(instrument_map_json, (bytes, bytearray)):
            instrument_map_json = instrument_map_json.decode('utf-8')

        instr_map = json.loads(instrument_map_json)
        today = dt.now(IST).date()
        prev_day = today - timedelta(days=1)
        while prev_day.weekday() >= 5: prev_day -= timedelta(days=1)

        symbols = list(settings.STOCK_INDEX_MAPPING.keys())
        pipe = redis_client.pipeline()
        stored = 0

        for symbol in symbols:
            token = None
            for t, data in instr_map.items():
                if (data.get('symbol') == symbol and data.get('exchange') == 'NSE' and data.get('instrument_type') in ('EQ', 'EQUITY')):
                    token = safe_int(t)
                    break
            if not token: continue

            try:
                hist = kite.historical_data(instrument_token=token, from_date=prev_day, to_date=prev_day, interval='day')
                if not hist: continue
                row = hist[-1]
                high = float(row.get('high', 0.0) or 0.0)
                low = float(row.get('low', 0.0) or 0.0)
                close = float(row.get('close', 0.0) or 0.0)
                payload = json.dumps({'date': prev_day.isoformat(), 'high': high, 'low': low, 'close': close}, default=_default_json_serializer)
                pipe.hset('prev_day_ohlc', symbol, payload)
                stored += 1
            except Exception: continue

        if stored: pipe.execute()
    except Exception as e: logger.error(f"PREV_HL: Overall failure: {e}")

def get_instrument_map_from_cache():
    """Helper to retrieve the instrument map from Redis."""
    if not redis_client: return {}
    instrument_map_json = redis_client.get('instrument_map')
    if instrument_map_json:
        try:
            if isinstance(instrument_map_json, (bytes, bytearray)):
                instrument_map_json = instrument_map_json.decode('utf-8')
            return json.loads(instrument_map_json)
        except Exception: return {}
    return {}
# --- INSTRUMENT CACHING ---
def cache_instruments_for_day():
    """Fetches NSE instruments from Kite and caches them in Redis."""
    logger.info("WEBSOCKET_CACHE: Starting daily unified instrument caching process (EQUITY ONLY)...")
    try:
        master_account = Account.objects.filter(is_master=True, user__is_superuser=True).first()
        if not master_account:
            logger.error("WEBSOCKET_CACHE: No master account found.")
            return

        kite = get_kite(master_account)

        logger.info("WEBSOCKET_CACHE: Fetching NSE instruments...")
        nse_instruments = kite.instruments("NSE") or []
        all_instruments = nse_instruments 

        symbols_to_fetch = list(settings.STOCK_INDEX_MAPPING.keys()) + list(settings.INDEX_SYMBOLS)
        full_instrument_symbols = [f"NSE:{s}" for s in symbols_to_fetch]

        initial_quotes = {}
        try:
            if full_instrument_symbols:
                initial_quotes = kite.quote(full_instrument_symbols) or {}
        except Exception: pass

        instrument_map = {}
        circuit_limits = {}

        for inst in all_instruments:
            token_val = inst.get('instrument_token') or inst.get('token')
            if not token_val: continue
            token_str = str(token_val)

            symbol = (inst.get('tradingsymbol') or inst.get('symbol') or "").strip()
            exchange = inst.get('exchange') or inst.get('segment') or ""
            instrument_type = 'EQ'

            expiry = parse_date_safe(inst.get('expiry'))
            strike = safe_int(inst.get('strike'))
            name = inst.get('name', '')
            full_symbol = f"{exchange}:{symbol}"

            instrument_data = {
                'symbol': symbol, 'exchange': exchange, 'segment': inst.get('segment', exchange),
                'instrument_type': instrument_type, 'lot_size': inst.get('lot_size', 1), 'tick_size': inst.get('tick_size', 0.05),
                'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0, 'ltp': 0.0, 'change': 0.0, 'volume': 0,
                'expiry': expiry, 'strike': strike, 'name': name,
            }
            if inst.get('instrument_type') in ('EQ', 'EQUITY'):
                circuit_limits[symbol] = {
                    'upper': inst.get('upper_circuit_limit', float('inf')),
                    'lower': inst.get('lower_circuit_limit', 0)
                }

            if full_symbol in initial_quotes:
                quote = initial_quotes[full_symbol] or {}
                ohlc = quote.get('ohlc') or {}
                ltp = quote.get('last_price') or 0.0
                prev_close = ohlc.get('close', 0.0)
                seeded_volume = ohlc.get('volume') or quote.get('volume') or 0

                instrument_data.update({
                    'close': prev_close, 'ltp': ltp,
                    'change': ((ltp - prev_close) / prev_close * 100) if prev_close else 0.0,
                    'volume': int(seeded_volume or 0), 'open': ohlc.get('open', 0.0),
                    'high': max(ohlc.get('high', 0.0), ltp),
                    'low': min(ohlc.get('low', float('inf')), ltp) if ltp > 0 else 0.0,
                })
                if instrument_data['low'] == 0.0 and ltp > 0: instrument_data['low'] = ltp

            instrument_map[token_str] = instrument_data

        pipe = redis_client.pipeline()
        pipe.set('instrument_map', json.dumps(instrument_map, default=_default_json_serializer))
        pipe.set('circuit_limits', json.dumps(circuit_limits, default=_default_json_serializer))
        pipe.execute()
        logger.info(f"WEBSOCKET_CACHE: Successfully cached {len(instrument_map)} total instruments.")
    except Exception as e:
        logger.error(f"WEBSOCKET_CACHE: Failed to cache instruments: {e}", exc_info=True)

# --- CANDLE AGGREGATOR ---

class CandleAggregator:
    def __init__(self, redis_client, filter_criteria, qualified_set_ref, whitelist_ref, max_candles=10000):
        self.redis = redis_client
        self.max_candles = max_candles
        self.active_candle = {}
        self.last_total_volume = {}
        self.lock = threading.Lock()
        
        self.fixed_sma_map = {}
        self.filter_criteria = filter_criteria 
        
        # References to Shared Sets
        self.qualified_symbols = qualified_set_ref
        self.whitelist_ref = whitelist_ref
        
        self.load_fixed_sma()

    def load_fixed_sma(self):
        """Loads externally seeded SMA from Redis."""
        try:
            raw_map = self.redis.hgetall(FIXED_SMA_KEY)
            if raw_map:
                self.fixed_sma_map = {
                    (k.decode('utf-8') if isinstance(k, bytes) else k): int(safe_int(v, 0)) 
                    for k, v in raw_map.items()
                }
                logger.info(f"AGG: Loaded {len(self.fixed_sma_map)} Fixed SMA values.")
            else:
                logger.warning("AGG: Fixed SMA map empty! Did you run 'seed_daily_volume'?")
        except Exception: pass

    def _is_trading_minute(self, ts: dt) -> bool:
        t = ts.astimezone(IST).time()
        start = datetime_time(9, 15)
        end = datetime_time(15, 30)
        return (t >= start) and (t < end)

    def _bucket_for(self, ts: dt) -> dt:
        if ts.tzinfo is None: ts = pytz.utc.localize(ts)
        ts_ist = ts.astimezone(IST)
        return ts_ist.replace(second=0, microsecond=0)

    # --- THE QUALIFICATION LOGIC ---
    # --- THE QUALIFICATION LOGIC (OPTIMIZED) ---
    def is_qualified(self, symbol, vol, close):
        """
        Determines if a stock should be processed (UI + Algo).
        OPTIMIZATION: Checks static SMA first to fail-fast on illiquid stocks 
        before checking turnover or current volume spikes.
        """
        # 1. Fast Path (Latch)
        # If already qualified or whitelisted, return immediately.
        if symbol in self.qualified_symbols: return True
        if symbol in self.whitelist_ref: 
            self.qualified_symbols.add(symbol)
            return True

        # 2. Check Criteria
        if not self.filter_criteria: return True
        
        # Fast Lookup (RAM)
        sma = self.fixed_sma_map.get(symbol, 0)
        if close <= 0: return False

        # Pre-calculate turnover only if needed (Optimization: Delayed calculation if possible)
        # However, since 'vol' and 'close' are passed in, calculating this float is cheap enough 
        # to do once rather than inside the loop if there are multiple criteria.
        vol_price_cr = round((vol * close) / 10000000.0, 4)

        for criteria in self.filter_criteria:
            try:
                min_sma = criteria.get('min_sma', 0)
                
                # OPTIMIZATION 1: Filter by Liquidity (Integer Comparison)
                # If the stock's average daily volume is too low, skip it instantly.
                if sma < min_sma:
                    continue

                # OPTIMIZATION 2: Filter by Volume Spike (Integer Math)
                # Is the current volume significantly higher than average?
                mult = criteria.get('mult', 0)
                if vol < (sma * mult):
                    continue

                # OPTIMIZATION 3: Filter by Turnover (Float Comparison)
                # Only check value if liquidity and spike conditions are met.
                min_vp = criteria.get('min_vp', 0)
                if vol_price_cr >= min_vp:
                    self.qualified_symbols.add(symbol) # LATCH
                    return True
            except: continue
            
        return False

    def process_tick(self, symbol, ltp, ts, cumulative_volume):
        if not symbol or ltp is None: return
        try: self.redis.set(DATA_HEARTBEAT_KEY, int(time.time()), ex=10)
        except: pass

        if not self._is_trading_minute(ts): return
        bucket = self._bucket_for(ts)
        
        with self.lock:
            active = self.active_candle.get(symbol)
            delta_vol = 0
            
            try:
                if cumulative_volume is not None:
                    cur_total = int(cumulative_volume)
                    prev_total = self.last_total_volume.get(symbol)
                    delta_vol = max(0, cur_total - prev_total) if prev_total is not None else 0
                    self.last_total_volume[symbol] = cur_total
                else: delta_vol = 0
            except: delta_vol = 0

            if active and active['bucket'] != bucket:
                self._publish_and_store(symbol, active)
                self.active_candle[symbol] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': int(delta_vol)}
            elif not active:
                self.active_candle[symbol] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': int(delta_vol)}
            else:
                c = active
                c['high'] = max(c['high'], ltp)
                c['low'] = min(c['low'], ltp)
                c['close'] = ltp
                c['volume'] = int(c.get('volume', 0) + delta_vol)
            
            # --- LIVE SNAPSHOT UPDATE (FILTERED) ---
            try:
                cur = self.active_candle[symbol]
                # Check qualification using CURRENT total volume (approx)
                is_valid = self.is_qualified(symbol, int(cur.get('volume', 0)), ltp)
                
                if is_valid:
                    payload = {'ts': cur['bucket'].isoformat(), 'open': cur['open'], 'high': cur['high'], 'low': cur['low'], 'close': cur['close'], 'volume': cur['volume']}
                    self.redis.set(f'current_1m_candle:{symbol}', json.dumps(payload, default=_default_json_serializer))
            except: pass

    def _publish_and_store(self, symbol, candle):
        vol = int(candle.get('volume', 0))
        close = float(candle['close'])
        
        # --- STRICT FILTERING (ALGO) ---
        if not self.is_qualified(symbol, vol, close):
            return 
        # -------------------------------

        vol_sma = self.fixed_sma_map.get(symbol, 0)
        vol_price_cr = round((vol * close) / 10000000.0, 4)
        
        payload = {
            'symbol': symbol, 'interval': '1m', 'ts': candle['bucket'].isoformat(),
            'open': candle['open'], 'high': candle['high'], 'low': candle['low'], 
            'close': candle['close'], 'volume': vol,
            'vol_sma_375': vol_sma, 'vol_price_cr': vol_price_cr,
        }
        
        try:
            key = f'candles:1m:{symbol}'
            self.redis.rpush(key, json.dumps(payload, default=_default_json_serializer))
            self.redis.ltrim(key, -self.max_candles, -1)
            
            if candle['bucket'].time() == datetime_time(9, 15):
                self.redis.set(f"{FIRST_CANDLE_PREFIX}:{symbol}", json.dumps(payload, default=_default_json_serializer), ex=86400)

            try: self.redis.xadd(CANDLE_STREAM_KEY, {'data': json.dumps(payload, default=_default_json_serializer)}, maxlen=self.max_candles, approximate=True)
            except TypeError: self.redis.xadd(CANDLE_STREAM_KEY, {'data': json.dumps(payload, default=_default_json_serializer)})

        except Exception as e: logger.error(f"AGG: Finalize Error {symbol}: {e}")

    def flush_all(self):
        with self.lock:
            keys = list(self.active_candle.keys())
            for k in keys:
                c = self.active_candle.pop(k, None)
                if c: self._publish_and_store(k, c)

# --- LIVE TICKER ---

class LiveTickerManager:
    def __init__(self, api_key, access_token, account_id):
        self.kws = KiteTicker(api_key, access_token)
        self.account_id = account_id
        self.redis_client = get_redis_connection()
        self.instrument_map = {}
        self.subscribed_tokens = set()
        self.last_publish_time = 0
        self.running = True
        
        # --- QUALIFIED SETS ---
        self.qualified_symbols = set() 
        self.whitelist = set()
        self.last_whitelist_update = 0
        
        self.filter_criteria = self._load_strategy_criteria()
        
        self.candle_agg = CandleAggregator(self.redis_client, self.filter_criteria, self.qualified_symbols, self.whitelist, max_candles=5000)

        self.kws.on_ticks = self.on_ticks
        self.kws.on_connect = self.on_connect
        self.kws.on_close = self.on_close
        self.kws.on_error = self.on_error
        self.kws.on_order_update = self.on_order_update

    def _load_strategy_criteria(self):
        """Fetches Settings at Startup."""
        criteria = []
        try:
            bull_raw = self.redis_client.get(KEY_BULL_SETTINGS)
            if bull_raw:
                bull = json.loads(bull_raw).get('volume_criteria', [])
                for c in bull: criteria.append({'min_vp': float(c.get('min_vol_price_cr', 0)), 'min_sma': float(c.get('min_sma_avg', 0)), 'mult': float(c.get('sma_multiplier', 0))})
            
            bear_raw = self.redis_client.get(KEY_BEAR_SETTINGS)
            if bear_raw:
                bear = json.loads(bear_raw).get('volume_criteria', [])
                for c in bear: criteria.append({'min_vp': float(c.get('min_vol_price_cr', 0)), 'min_sma': float(c.get('min_sma_avg', 0)), 'mult': float(c.get('sma_multiplier', 0))})
            
            logger.info(f"WS_INIT: Loaded {len(criteria)} volume criteria sets.")
        except: pass
        return criteria

    def _refresh_whitelist(self):
        """Syncs active trade list from Redis."""
        if time.time() - self.last_whitelist_update < 2: return 
        try:
            keys = [
                f"breakout_active_entries:{self.account_id}",
                f"breakdown_active_entries:{self.account_id}",
                f"mom_bull_active_entries:{self.account_id}",
                f"mom_bear_active_entries:{self.account_id}"
            ]
            active_symbols = self.redis_client.sunion(keys)
            new_whitelist = {s.decode('utf-8') if isinstance(s, bytes) else s for s in active_symbols} if active_symbols else set()
            self.whitelist.clear()
            self.whitelist.update(new_whitelist)
            self.last_whitelist_update = time.time()
        except: pass

    def _load_instruments_from_cache(self) -> bool:
        try:
            instrument_map_json = self.redis_client.get('instrument_map')
            if instrument_map_json:
                if isinstance(instrument_map_json, (bytes, bytearray)):
                    instrument_map_json = instrument_map_json.decode('utf-8')
                full_map = json.loads(instrument_map_json)
                self.instrument_map = {k: v for k, v in full_map.items() if v.get('exchange') == 'NSE' or v.get('symbol') in settings.INDEX_SYMBOLS}
                logger.info(f"WEBSOCKET: Loaded {len(self.instrument_map)} Cash/Index instruments.")
                return True
            return False
        except Exception: return False

    def on_order_update(self, ws, order):
        try:
            user_id = order.get('user_id')
            if not user_id: return
            account = Account.objects.filter(user__username=user_id).first()
            if not account: return
            channel = f"order_updates:{account.id}"
            message = json.dumps({
                'order_id': order.get('order_id'), 'status': order.get('status'), 'status_message': order.get('status_message'),
                'filled_quantity': order.get('filled_quantity', 0), 'average_price': order.get('average_price', 0.0),
            })
            self.redis_client.publish(channel, message)
        except Exception: pass

    def on_ticks(self, ws, ticks):
        self._refresh_whitelist()
        
        pipe = self.redis_client.pipeline()
        has_stream_data = False

        for tick in ticks:
            token = tick.get('instrument_token') or tick.get('token')
            if token is None: continue
            token_str = str(token)
            if token_str not in self.instrument_map: continue
            instrument = self.instrument_map[token_str]

            ltp_raw = tick.get('last_price') or tick.get('ltp') or instrument.get('ltp', 0.0)
            try: instrument['ltp'] = float(ltp_raw)
            except Exception: instrument['ltp'] = float(instrument.get('ltp', 0.0) or 0.0)

            # (OHLC Updates)
            if 'ohlc' in tick and isinstance(tick['ohlc'], dict):
                ohlc = tick['ohlc']
                instrument['open'] = ohlc.get('open', instrument.get('open', 0.0))
                instrument['high'] = ohlc.get('high', instrument.get('high', 0.0))
                instrument['low'] = ohlc.get('low', instrument.get('low', 0.0))
                instrument['close'] = ohlc.get('close', instrument.get('close', 0.0))
            else:
                instrument['open'] = tick.get('open') or instrument.get('open', 0.0)
                instrument['high'] = tick.get('high') or instrument.get('high', 0.0)
                instrument['low'] = tick.get('low') or instrument.get('low', 0.0)
                instrument['close'] = tick.get('close') or instrument.get('close', 0.0)

            if instrument['ltp'] > 0.0:
                instrument['high'] = max(instrument.get('high', 0.0), instrument['ltp'])
                cur_low = instrument.get('low', float('inf'))
                if cur_low == 0.0 or cur_low == float('inf') or instrument['ltp'] < cur_low: instrument['low'] = instrument['ltp']

            raw_vol = tick.get('volume_traded') or tick.get('total_traded_quantity')
            if raw_vol is not None:
                try: instrument['volume'] = int(raw_vol)
                except Exception: pass

            prev = instrument.get('close', 0.0)
            if prev:
                try: instrument['change'] = ((instrument['ltp'] - prev) / prev) * 100
                except Exception: pass

            try:
                sym = instrument.get('symbol')
                if sym and sym in settings.STOCK_INDEX_MAPPING and instrument.get('exchange') == 'NSE':
                    ts_raw = tick.get('timestamp') or tick.get('tradable_at')
                    if ts_raw:
                        try:
                            if isinstance(ts_raw, dt): ts = ts_raw
                            else: ts = dt.fromisoformat(str(ts_raw).replace('Z', '+00:00'))
                            if ts.tzinfo is None: ts = pytz.utc.localize(ts).astimezone(IST)
                        except Exception: ts = dt.now(IST)
                    else: ts = dt.now(IST)

                    cum_vol = int(raw_vol) if raw_vol is not None else None
                    
                    # 1. Update Candle (Takes care of Candle Stream Filtering inside)
                    self.candle_agg.process_tick(sym, float(instrument['ltp'] or 0.0), ts, cum_vol)
                    
                    # 2. TICK STREAM FILTER (SAFE)
                    # Use CandleAggregator logic to check if Qualified
                    is_valid = self.candle_agg.is_qualified(sym, instrument['volume'], instrument['ltp'])
                    
                    if is_valid:
                        stream_data = {
                            'symbol': sym,
                            'ltp': str(instrument['ltp']),
                            'volume': str(instrument['volume']),
                            'ts': ts.isoformat()
                        }
                        pipe.xadd(TICK_STREAM_KEY, stream_data, maxlen=100000, approximate=True)
                        has_stream_data = True
            except Exception: pass

        if has_stream_data:
            pipe.execute()

        # UI Updates (Filtered)
        now = time.time()
        if now - self.last_publish_time > 0.1:
            self.publish_to_redis()
            self.last_publish_time = now

    def publish_to_redis(self):
        try:
            pipe = self.redis_client.pipeline()
            live_ohlc = {}
            
            for token, data in self.instrument_map.items():
                sym = data.get('symbol')
                if not sym or data.get('ltp', 0.0) <= 0.0: continue
                
                # --- STRICT UI FILTER ---
                # Check if symbol is in the Qualified Set or Whitelist
                # Index Symbols are ALWAYS allowed
                is_index = sym in settings.INDEX_SYMBOLS
                if not is_index and sym not in self.qualified_symbols and sym not in self.whitelist:
                    continue
                # ------------------------

                live_ohlc[sym] = {
                    'ltp': data.get('ltp', 0.0), 'open': data.get('open', 0.0), 'high': data.get('high', 0.0),
                    'low': data.get('low', 0.0), 'close': data.get('close', 0.0), 'change': data.get('change', 0.0),
                    'sector': settings.STOCK_INDEX_MAPPING.get(sym, 'N/A'), 'volume': int(data.get('volume', 0)),
                }
                
            if live_ohlc:
                pipe.set('live_ohlc_data', json.dumps(live_ohlc, default=_default_json_serializer))
                # Filter PubSub too
                ticks_list = [{'instrument_token': t, 'last_price': d.get('ltp',0), 'volume': int(d.get('volume',0))} 
                              for t, d in self.instrument_map.items() if d.get('symbol') in live_ohlc]
                pipe.publish('ticks_channel', json.dumps(ticks_list))

            pipe.execute()
        except Exception: pass

    # ... (Keep on_connect, on_close, on_error, start/stop_connection) ...
    def on_connect(self, ws, response):
        logger.info("WEBSOCKET: Connected.")
        self.redis_client.set('websocket_status', 'connected')
        st = {safe_int(t) for t, d in self.instrument_map.items() if d.get('symbol') in settings.STOCK_INDEX_MAPPING and d.get('exchange') == 'NSE'}
        it = {safe_int(t) for t, d in self.instrument_map.items() if d.get('symbol') in settings.INDEX_SYMBOLS}
        try:
            target = self.redis_client.hkeys('prev_day_ohlc')
            if target:
                decoded = {s.decode('utf-8') if isinstance(s, bytes) else str(s) for s in target}
                for t, d in self.instrument_map.items():
                    if d.get('symbol') in decoded and d.get('exchange') == 'NSE': st.add(safe_int(t))
        except Exception: pass
        subs = list((st | it) - {None})
        self.subscribed_tokens = set(subs)
        if subs:
            try: ws.subscribe(subs); ws.set_mode(ws.MODE_FULL, subs); logger.info(f"WEBSOCKET: Subscribed to {len(subs)} tokens.")
            except Exception: pass

    def on_close(self, ws, code, reason):
        logger.warning(f"WEBSOCKET: Closed. Code: {code}, Reason: {reason}")
        self.redis_client.set('websocket_status', 'disconnected')

    def on_error(self, ws, code, reason):
        logger.error(f"WEBSOCKET: Error. Code: {code}, Reason: {reason}")

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
    def start_connection(self):
        logger.info("WEBSOCKET: Starting connection...")
        self.kws.connect(threaded=True)

    def stop_connection(self):
        try:
            if getattr(self.kws, 'is_connected', lambda: False)() or getattr(self.kws, 'is_connecting', lambda: False)():
                self.kws.close(1000, "Manual stop")
        except Exception: pass

    def run(self):
        if not self._load_instruments_from_cache():
            logger.warning("WEBSOCKET: Missing instrument cache. Rebuilding...")
            try: cache_instruments_for_day(); self._load_instruments_from_cache()
            except Exception: return
        
        if not self.redis_client.exists('prev_day_ohlc'):
            try: cache_previous_day_hl()
            except Exception: pass
        
        # --- NO CALCULATION, JUST LOAD ---
        self.candle_agg.load_fixed_sma()

        while self.running:
            connect_time = datetime_time(9, 10); disconnect_time = datetime_time(15, 35)
            now = dt.now(IST); today = now.date()
            if today.weekday() >= 5: time.sleep(3600); continue
            target = IST.localize(dt.combine(today, connect_time))
            if now < target:
                sleep_sec = (target - now).total_seconds()
                if sleep_sec > 0: time.sleep(sleep_sec)
            
            if dt.now(IST).time() < disconnect_time:
                if not getattr(self.kws, 'is_connected', lambda: False)():
                    try: self.start_connection()
                    except Exception: pass
                while dt.now(IST).time() < disconnect_time and self.running:
                    if not getattr(self.kws, 'is_connected', lambda: False)():
                        try: self.start_connection()
                        except Exception: pass
                    time.sleep(30)
            
            try: self.candle_agg.flush_all()
            except Exception: pass
            if getattr(self.kws, 'is_connected', lambda: False)(): self.stop_connection()
            time.sleep(60)

    def stop(self):
        self.running = False
        try: self.stop_connection()
        except Exception: pass

def start_websocket_thread(master_account):
    if not redis_client: return
    tm = LiveTickerManager(master_account.api_key, master_account.access_token, master_account.id)
    t = threading.Thread(target=tm.run, daemon=True)
    t.start()
    return tm

