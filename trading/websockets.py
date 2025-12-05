"""
Refactored Websocket / Instrument / Candle manager for KiteConnect
- FIXED: Volume SMA (375) now EXCLUDES the current candle (Historical Baseline only)
- Integrated Dashboard Metric Calculations (Volume * Price)
- Live synchronization with Redis for Algo Settings
"""

import json
import logging
import time
import re
import math
from datetime import datetime as dt, date, time as datetime_time, timedelta
from collections import defaultdict
from typing import Optional, Any, List
import threading

import pytz
from kiteconnect import KiteTicker
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_not_exception_type
import pandas as pd
import numpy as np

from django.conf import settings
from trading.models import Account
from trading.utils import get_kite, get_redis_connection, is_market_open

logger = logging.getLogger(__name__)
IST = pytz.timezone('Asia/Kolkata')
redis_client = get_redis_connection()

# A set of F&O eligible underlying symbols - from settings or empty
FNO_STOCKS = set(getattr(settings, 'SIMULATION_FNO_STOCKS', []))

# The key used for the Redis Stream
CANDLE_STREAM_KEY = 'candle_1m'

# --- REDIS KEYS FOR SETTINGS SYNC (MATCHING FRONTEND) ---
# These keys should be updated by your Django Views when Frontend saves settings
KEY_GLOBAL_SETTINGS = "algo:settings:global"
KEY_BULL_SETTINGS = "algo:settings:bull"
KEY_BEAR_SETTINGS = "algo:settings:bear"


# --- Utility helpers ---

def parse_date_safe(expiry_raw) -> Optional[date]:
    """Try multiple common expiry formats; return a date or None."""
    if not expiry_raw:
        return None
    if isinstance(expiry_raw, date) and not isinstance(expiry_raw, dt):
        return expiry_raw
    if isinstance(expiry_raw, dt):
        return expiry_raw.date()
    s = str(expiry_raw).strip()
    fmts = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d-%b-%Y", "%d%b%Y", "%d-%b-%y"]
    for f in fmts:
        try:
            return dt.strptime(s, f).date()
        except Exception:
            continue
    try:
        return dt.fromisoformat(s).date()
    except Exception:
        return None


def safe_int(x, default=None):
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return default


def _default_json_serializer(obj):
    try:
        if isinstance(obj, (date, dt)):
            return obj.isoformat()
        if isinstance(obj, float):
            if math.isinf(obj) or math.isnan(obj):
                return None
            return obj
        return str(obj)
    except Exception:
        return None


# --- Previous day high/low caching ---

def cache_previous_day_hl(master_account=None):
    """
    Caches previous trading day's OHLC (high, low, date) for symbols present in
    settings.STOCK_INDEX_MAPPING into Redis hash 'prev_day_ohlc'.
    """
    if not redis_client:
        logger.error("PREV_HL: Redis unavailable. Skipping previous day high/low caching.")
        return

    try:
        if not master_account:
            master_account = Account.objects.filter(is_master=True, user__is_superuser=True).first()
        if not master_account:
            logger.error("PREV_HL: No master account found. Skipping.")
            return

        kite = get_kite(master_account)

        # Load instrument map and map symbols->token
        instrument_map_json = redis_client.get('instrument_map')
        if not instrument_map_json:
            logger.warning("PREV_HL: instrument_map not found in Redis. Please run cache_instruments_for_day() first.")
            return

        # Redis may return bytes
        if isinstance(instrument_map_json, (bytes, bytearray)):
            instrument_map_json = instrument_map_json.decode('utf-8')

        instr_map = json.loads(instrument_map_json)

        today = dt.now(IST).date()
        # Find previous trading day
        prev_day = today - timedelta(days=1)
        while prev_day.weekday() >= 5:  # if weekend, go back
            prev_day -= timedelta(days=1)

        symbols = list(settings.STOCK_INDEX_MAPPING.keys())
        pipe = redis_client.pipeline()
        stored = 0

        for symbol in symbols:
            # find NSE EQ instrument token
            token = None
            for t, data in instr_map.items():
                if (data.get('symbol') == symbol and data.get('exchange') == 'NSE' and data.get('instrument_type') in ('EQ', 'EQUITY')):
                    token = safe_int(t)
                    break
            if not token:
                logger.debug(f"PREV_HL: No token for {symbol}; skipping")
                continue

            try:
                hist = kite.historical_data(instrument_token=token, from_date=prev_day, to_date=prev_day, interval='day')
                if not hist:
                    logger.warning(f"PREV_HL: No historical day data for {symbol} on {prev_day}")
                    continue
                row = hist[-1]
                high = float(row.get('high', 0.0) or 0.0)
                low = float(row.get('low', 0.0) or 0.0)
                payload = json.dumps({'date': prev_day.isoformat(), 'high': high, 'low': low}, default=_default_json_serializer)
                pipe.hset('prev_day_ohlc', symbol, payload)
                stored += 1
            except Exception as e:
                logger.error(f"PREV_HL: Error fetching historical for {symbol}: {e}", exc_info=True)
                continue

        if stored:
            pipe.execute()
            logger.info(f"PREV_HL: Stored previous day OHLC for {stored} symbols in Redis 'prev_day_ohlc'.")
        else:
            logger.warning("PREV_HL: No previous day OHLC stored.")

    except Exception as e:
        logger.error(f"PREV_HL: Overall failure: {e}", exc_info=True)


# --- Candle Aggregator for 1-minute candles (NOW USING REDIS STREAMS) ---
class CandleAggregator:
    """Aggregates ticks into 1-minute candles and adds them to a Redis Stream.
    
    Integrated Capabilities:
    1. 1-Min Candle Construction
    2. Volume SMA Calculation (Rolling 375 Minutes) - HISTORICAL ONLY
    3. Volume * Price Calculation for Dashboard
    """

    def __init__(self, redis_client, max_candles=5000):
        self.redis = redis_client
        self.max_candles = max_candles
        self.sma_period = 375  # As requested: 375 candles for Volume SMA
        
        # active_candle[symbol] = {'bucket': datetime, 'open':..., 'high':..., 'low':..., 'close':..., 'volume':...}
        self.active_candle = {}
        # last_total_volume used to compute delta
        self.last_total_volume = {}
        self.lock = threading.Lock()
        
        # Temporary storage for 9:15 candles to check against 9:16 candles for strategy logic
        self.first_candles_cache = {} 

    def _is_trading_minute(self, ts: dt) -> bool:
        # trading minutes: 09:15 <= minute < 15:30
        t = ts.astimezone(IST).time()
        start = datetime_time(9, 15)
        end = datetime_time(15, 30)
        return (t >= start) and (t < end)

    def _bucket_for(self, ts: dt) -> dt:
        # Ensure ts is timezone-aware; assume UTC if naive
        if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
            ts = pytz.utc.localize(ts)
        ts_ist = ts.astimezone(IST)
        return ts_ist.replace(second=0, microsecond=0)

    def process_tick(self, symbol: str, ltp: float, ts: dt, cumulative_volume: Optional[int]):
        if not symbol or ltp is None:
            return
        try:
            if not self._is_trading_minute(ts):
                return
        except Exception:
            # If timezone ops fail, drop tick (defensive)
            return

        bucket = self._bucket_for(ts)
        key_active = symbol
        with self.lock:
            active = self.active_candle.get(key_active)

            # compute delta volume using cumulative volume
            delta_vol = 0
            try:
                if cumulative_volume is not None:
                    cur_total = int(cumulative_volume)
                    prev_total = self.last_total_volume.get(symbol)
                    if prev_total is None:
                        delta_vol = cur_total
                    else:
                        delta_vol = max(0, cur_total - prev_total)
                    self.last_total_volume[symbol] = cur_total
                else:
                    delta_vol = 0
            except Exception:
                delta_vol = 0

            # if no active candle or bucket changed, finalize old candle
            if not active or active['bucket'] != bucket:
                if active:
                    # finalize and persist
                    try:
                        self._publish_and_store(symbol, active)
                    except Exception:
                        logger.exception("CANDLE_AGG: Error finalizing candle during bucket rollover")
                # start new candle
                self.active_candle[key_active] = {
                    'bucket': bucket,
                    'open': ltp,
                    'high': ltp,
                    'low': ltp,
                    'close': ltp,
                    'volume': int(delta_vol or 0),
                }
            else:
                # update existing
                c = active
                c['high'] = max(c['high'], ltp)
                c['low'] = min(c['low'], ltp)
                c['close'] = ltp
                c['volume'] = int(c.get('volume', 0) + (delta_vol or 0))

            # Also update current_1m_candle slot for consumers (Live Feed)
            try:
                cur = self.active_candle[key_active]
                payload = {
                    'ts': cur['bucket'].isoformat(),
                    'open': cur['open'],
                    'high': cur['high'],
                    'low': cur['low'],
                    'close': cur['close'],
                    'volume': cur['volume'],
                }
                self.redis.set(f'current_1m_candle:{symbol}', json.dumps(payload, default=_default_json_serializer))
            except Exception:
                logger.exception(f"CANDLE_AGG: Failed to set current_1m_candle for {symbol}")

    def _calculate_sma_and_vol_stats(self, symbol: str, current_volume: int, current_close: float):
        """
        Calculates the Volume SMA based on the last 375 candles stored in Redis.
        CRITICAL FIX: This now EXCLUDES the current candle from the SMA calculation.
        We want to compare Current Spike vs Historical Average.
        """
        key = f'candles:1m:{symbol}'
        sma_val = 0
        
        try:
            # Fetch last N candles from Redis (indices are inclusive)
            # We want the PREVIOUS 375 candles.
            # We do NOT add the current candle to this list.
            
            # lrange(key, -375, -1) gets the last 375 completed candles from history
            prev_candles_raw = self.redis.lrange(key, -self.sma_period, -1)
            
            volumes = []
            for raw in prev_candles_raw:
                try:
                    c_data = json.loads(raw)
                    volumes.append(c_data.get('volume', 0))
                except:
                    pass
            
            # NOTE: We DO NOT append current_volume here.
            
            if volumes:
                sma_val = sum(volumes) / len(volumes)
            else:
                # Fallback if no history exists (start of day/first candle)
                # In this case, we can't really compare, so we use current volume as baseline (SMA=Vol)
                # or 0 if you prefer strictly no trade on first candle.
                # Using current_volume prevents division by zero later, but signals will match 1.0x exactly.
                sma_val = current_volume 
                
        except Exception as e:
            logger.error(f"SMA_CALC: Error calculating volume SMA for {symbol}: {e}")
            sma_val = current_volume

        # Calculate Vol * Price (in Crores)
        # Formula: (Volume * Close Price) / 1,00,00,000
        vol_price_cr = 0
        try:
            vol_price_cr = round((current_volume * current_close) / 10000000.0, 4)
        except:
            pass

        return sma_val, vol_price_cr

    def _check_first_candles_logic(self, symbol: str, current_candle: dict):
        """
        Implements the 2-candle setup check at 9:16 AM.
        Current candle passed here is the 9:16-9:17 candle (bucket 09:16).
        """
        try:
            # Retrieve the stored 9:15 candle
            candle_1 = self.first_candles_cache.get(symbol)
            if not candle_1:
                return

            candle_2 = current_candle

            c1_open, c1_close, c1_high = candle_1['open'], candle_1['close'], candle_1['high']
            c2_open, c2_close, c2_high = candle_2['open'], candle_2['close'], candle_2['high']

            is_c1_red = c1_close < c1_open
            is_c2_red = c2_close < c2_open
            is_c2_green = c2_close > c2_open

            valid_setup = False

            # Condition 4 & 5: If both are RED
            if is_c1_red and is_c2_red:
                valid_setup = True
            
            # Condition 6: C1 Red, C2 Green (With specific constraints)
            elif is_c1_red and is_c2_green:
                # 2nd candle close must be less than open of first candle
                # AND high of second candle must be less than high of first candle
                if c2_close < c1_open and c2_high < c1_high:
                    valid_setup = True
            
            if valid_setup:
                # Store valid setup in Redis for the trading engine
                # Key: algo:setup_valid:<symbol>
                # Value: JSON with Reference Level (High of First Candle)
                payload = {
                    'symbol': symbol,
                    'reference_level': c1_high,
                    'setup_time': dt.now(IST).isoformat(),
                    'c1_high': c1_high,
                    'c1_close': c1_close,
                    'c2_close': c2_close
                }
                self.redis.set(f"algo:setup_valid:{symbol}", json.dumps(payload, default=_default_json_serializer), ex=86400) # Expire in 24h
                logger.info(f"STRATEGY_SETUP: Valid Setup found for {symbol}. Ref Level: {c1_high}")

            # Cleanup cache to save memory
            del self.first_candles_cache[symbol]

        except Exception as e:
            logger.error(f"STRATEGY_SETUP: Error checking logic for {symbol}: {e}")

    def _publish_and_store(self, symbol: str, candle: dict):
        # 1. Calculate SMA (Historical) and Vol Price Stats BEFORE pushing current candle to history list
        vol_sma, vol_price_cr = self._calculate_sma_and_vol_stats(symbol, int(candle.get('volume', 0)), candle['close'])

        payload = {
            'symbol': symbol,
            'interval': '1m',
            'ts': candle['bucket'].isoformat(),
            'open': candle['open'],
            'high': candle['high'],
            'low': candle['low'],
            'close': candle['close'],
            'volume': int(candle.get('volume', 0)),
            # --- New Fields for Strategy Engine & Dashboard ---
            'vol_sma_375': vol_sma,
            'vol_price_cr': vol_price_cr,
            # ------------------------------------------------
        }
        
        # --- Strategy Logic: Capture 9:15 and 9:16 Candles ---
        candle_time = candle['bucket'].time()
        
        # Capture 9:15 Candle (09:15 to 09:16)
        if candle_time.hour == 9 and candle_time.minute == 15:
            self.first_candles_cache[symbol] = candle.copy()
            
        # Capture 9:16 Candle (09:16 to 09:17) and Run Logic
        elif candle_time.hour == 9 and candle_time.minute == 16:
            self._check_first_candles_logic(symbol, candle)
        # -----------------------------------------------------

        try:
            key = f'candles:1m:{symbol}'
            # store as JSON string into a list for backward compatibility
            self.redis.rpush(key, json.dumps(payload, default=_default_json_serializer))
            # keep only last max_candles
            self.redis.ltrim(key, -self.max_candles, -1)
            
            # set latest snapshot with enriched data
            self.redis.set(f'current_1m_candle:{symbol}', json.dumps(payload, default=_default_json_serializer))
            
            # Store technicals separately for easy dashboard access (optional but recommended)
            tech_payload = {'symbol': symbol, 'vol_sma_375': vol_sma, 'vol_price_cr': vol_price_cr}
            self.redis.set(f'technical:1m:{symbol}', json.dumps(tech_payload))

            # Publish using Redis Stream (XADD).
            try:
                # redis-py allows xadd(name, fields, maxlen=..., approximate=True)
                self.redis.xadd(CANDLE_STREAM_KEY, {'data': json.dumps(payload, default=_default_json_serializer)}, maxlen=self.max_candles, approximate=True)
            except TypeError:
                # Older redis client: fallback to xadd without maxlen
                self.redis.xadd(CANDLE_STREAM_KEY, {'data': json.dumps(payload, default=_default_json_serializer)})

            logger.debug(f"CANDLE_AGG: Added 1m candle for {symbol} (SMA={vol_sma:.2f}) to Stream.")
        except Exception as e:
            logger.error(f"CANDLE_AGG: Failed to XADD/store candle for {symbol}: {e}", exc_info=True)

    def flush_all(self):
        """Force finalize all active candles (e.g., at market close)."""
        with self.lock:
            keys = list(self.active_candle.keys())
            for k in keys:
                c = self.active_candle.pop(k, None)
                if c:
                    try:
                        self._publish_and_store(k, c)
                    except Exception:
                        logger.exception(f"CANDLE_AGG: Error flushing candle for {k}")


# --- Daily Volatility Computation Function (kept similar but defensive) ---
def compute_and_store_daily_volatilities(master_account=None):
    if not redis_client:
        logger.error("VOLATILITY: Redis client unavailable. Skipping computation.")
        return

    try:
        if not master_account:
            master_account = Account.objects.filter(is_master=True, user__is_superuser=True).first()
        if not master_account:
            logger.error("VOLATILITY: No master account found. Skipping computation.")
            return
        kite = get_kite(master_account)

        instrument_map_json = redis_client.get('instrument_map')
        if not instrument_map_json:
            logger.warning("VOLATILITY: instrument_map not found in Redis. Run cache_instruments_for_day() first.")
            return
        if isinstance(instrument_map_json, (bytes, bytearray)):
            instrument_map_json = instrument_map_json.decode('utf-8')
        instrument_map = json.loads(instrument_map_json)

        stocks = list(settings.STOCK_INDEX_MAPPING.keys())
        logger.info(f"VOLATILITY: Starting computation for {len(stocks)} stocks over 1 year.")

        volatilities = {}
        today = dt.now(IST).date()
        from_date = today - timedelta(days=365)

        for stock in stocks:
            token = None
            for t, data in instrument_map.items():
                if (data.get('symbol') == stock and
                        data.get('exchange') == 'NSE' and
                        data.get('instrument_type') == 'EQ'):
                    token = safe_int(t)
                    break
            if not token:
                logger.warning(f"VOLATILITY: No NSE EQ instrument found for {stock}. Skipping.")
                continue

            try:
                hist_data = kite.historical_data(
                    instrument_token=token,
                    from_date=from_date,
                    to_date=today,
                    interval='day'
                )
                if not hist_data:
                    logger.warning(f"VOLATILITY: No historical data for {stock}.")
                    continue

                df = pd.DataFrame(hist_data)
                if df.empty:
                    continue

                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date').reset_index(drop=True)

                df['LogReturn'] = np.log(df['close'] / df['close'].shift(1))
                lam = 0.94
                alpha = 1 - lam
                df['EWMA_Var'] = df['LogReturn'].pow(2).ewm(alpha=alpha, adjust=False).mean()
                df['EWMA_DailyVol'] = np.sqrt(df['EWMA_Var'])

                last_vol = df['EWMA_DailyVol'].iloc[-1]
                if pd.notna(last_vol):
                    volatilities[stock] = round(float(last_vol * 100), 2)
                else:
                    logger.warning(f"VOLATILITY: NaN volatility for {stock}.")

            except Exception as e:
                logger.error(f"VOLATILITY: Error processing {stock}: {e}", exc_info=True)
                continue

        if volatilities:
            pipe = redis_client.pipeline()
            for stock, vol in volatilities.items():
                pipe.hset('daily_volatilities', stock, vol)
            pipe.execute()
            logger.info(f"VOLATILITY: Stored daily volatilities for {len(volatilities)} stocks in Redis 'daily_volatilities'.")
        else:
            logger.warning("VOLATILITY: No volatilities computed.")

    except Exception as e:
        logger.error(f"VOLATILITY: Overall computation failed: {e}", exc_info=True)


# --- Cache instruments ---
def get_instrument_map_from_cache():
    if not redis_client:
        return {}
    instrument_map_json = redis_client.get('instrument_map')
    if instrument_map_json:
        try:
            if isinstance(instrument_map_json, (bytes, bytearray)):
                instrument_map_json = instrument_map_json.decode('utf-8')
            return json.loads(instrument_map_json)
        except Exception:
            return {}
    return {}


def cache_instruments_for_day():
    logger.info("WEBSOCKET_CACHE: Starting daily unified instrument caching process...")
    try:
        master_account = Account.objects.filter(is_master=True, user__is_superuser=True).first()
        if not master_account:
            logger.error("WEBSOCKET_CACHE: No master account found.")
            return

        kite = get_kite(master_account)

        logger.info("WEBSOCKET_CACHE: Fetching NSE & NFO instruments...")
        nse_instruments = kite.instruments("NSE") or []
        nfo_instruments = kite.instruments("NFO") or []
        all_instruments = nse_instruments + nfo_instruments

        symbols_to_fetch = list(settings.STOCK_INDEX_MAPPING.keys()) + list(settings.INDEX_SYMBOLS)
        full_instrument_symbols = [f"NSE:{s}" for s in symbols_to_fetch]

        initial_quotes = {}
        try:
            if full_instrument_symbols:
                initial_quotes = kite.quote(full_instrument_symbols) or {}
        except Exception:
            logger.warning("WEBSOCKET_CACHE: kite.quote() failed for initial symbols; data will start at zero/default.")

        instrument_map = {}
        circuit_limits = {}

        for inst in all_instruments:
            token_val = inst.get('instrument_token') or inst.get('token') or inst.get('instrumentToken')
            if not token_val:
                continue
            token_str = str(token_val)

            symbol = (inst.get('tradingsymbol') or inst.get('symbol') or inst.get('name') or "").strip()
            exchange = inst.get('exchange') or inst.get('segment') or ""
            raw_it = inst.get('instrument_type') or inst.get('instrumentType') or ""
            itype_field = str(raw_it).upper() if raw_it is not None else ""
            sym_upper = symbol.upper()

            if 'OPT' in itype_field or 'OPTION' in itype_field or sym_upper.endswith('CE') or sym_upper.endswith('PE'):
                instrument_type = 'OPT'
            elif 'FUT' in itype_field or 'FUTURE' in itype_field:
                instrument_type = 'FUT'
            elif itype_field in ('EQUITY', 'EQ'):
                instrument_type = 'EQ'
            else:
                seg = (inst.get('segment') or exchange or "").upper()
                if 'NFO' in seg and (sym_upper.endswith('CE') or sym_upper.endswith('PE') or 'FUT' in sym_upper):
                    instrument_type = 'OPT' if (sym_upper.endswith('CE') or sym_upper.endswith('PE')) else 'FUT'
                else:
                    instrument_type = 'EQ'

            expiry = inst.get('expiry') or inst.get('expiry_date') or None
            strike_raw = inst.get('strike') or inst.get('strike_price') or None
            name = inst.get('name', '')

            try:
                if isinstance(expiry, dt): expiry = expiry.date()
                if isinstance(expiry, date): expiry = expiry.isoformat()
            except Exception:
                pass

            strike = None
            try:
                if strike_raw not in (None, '', 'None'):
                    strike = float(strike_raw)
            except Exception:
                strike = None

            full_symbol = f"{exchange}:{symbol}"

            instrument_data = {
                'symbol': symbol, 'exchange': exchange, 'segment': inst.get('segment', exchange),
                'instrument_type': instrument_type, 'lot_size': inst.get('lot_size', 1), 'tick_size': inst.get('tick_size', 0.05),
                'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0, 'ltp': 0.0, 'change': 0.0, 'volume': 0,
                'expiry': expiry, 'strike': strike, 'name': name,
            }
            if inst.get('instrument_type') == 'EQ':
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
        raise


# --- FNO Manager ---
class FnoTickerManager:
    def __init__(self, api_key, access_token, subscription_strategy='nearest_expiry', atm_strike_count=8):
        self.kws = KiteTicker(api_key, access_token)
        self.redis_client = get_redis_connection()
        self.instrument_map = {}
        self.fno_structured_map = {}
        self.subscribed_tokens = set()
        self.last_publish_time = 0
        self.running = True
        self.subscription_strategy = subscription_strategy
        self.atm_strike_count = int(atm_strike_count)
        self.last_underlying_movers = set()
        # Timer for dynamic subscription updates
        self.DYNAMIC_UPDATE_INTERVAL = 0.3

        # pubsub resources
        self._pubsub = None

        # event handlers
        self.kws.on_ticks = self.on_ticks
        self.kws.on_connect = self.on_connect
        self.kws.on_close = self.on_close
        self.kws.on_error = self.on_error
        self.kws.on_order_update = self.on_order_update

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
            logger.debug(f"FNO_WEBSOCKET_ORDER_UPDATE: Published update for order {order.get('order_id')} to channel {channel}")
        except Exception as e:
            logger.error(f"FNO_WEBSOCKET_ORDER_UPDATE: Error processing order update: {e}", exc_info=True)

    def _load_instruments_from_cache(self) -> bool:
        try:
            instrument_map_json = self.redis_client.get('instrument_map')
            if instrument_map_json:
                if isinstance(instrument_map_json, (bytes, bytearray)):
                    instrument_map_json = instrument_map_json.decode('utf-8')
                full_map = json.loads(instrument_map_json)
                filtered = {}
                structured = defaultdict(lambda: defaultdict(list))
                today_date = dt.now(IST).date()

                for k, v in full_map.items():
                    sym = (v.get('symbol') or '').upper()
                    itype = (v.get('instrument_type') or '').upper()
                    seg = (v.get('segment') or v.get('exchange') or '').upper()

                    if (itype in ('FUT', 'OPT')) or ('NFO' in seg) or (sym.endswith('CE') or sym.endswith('PE') or 'FUT' in sym):
                        filtered[k] = v

                        expiry_date = parse_date_safe(v.get('expiry'))
                        strike = None
                        try:
                            strike = float(v.get('strike')) if v.get('strike') not in (None, '', 'None') else None
                        except Exception:
                            strike = None

                        m = re.match(r'^([A-Z]+)', sym)
                        underlying_prefix = m.group(1) if m else ''

                        if not underlying_prefix or (expiry_date and expiry_date < today_date):
                            continue

                        structured[underlying_prefix][expiry_date].append({
                            'token': safe_int(k), 'symbol': sym, 'type': itype, 'expiry': expiry_date, 'strike': strike, 'raw': v,
                        })

                self.instrument_map = filtered
                self.fno_structured_map = structured
                logger.info(f"FNO_WEBSOCKET: Loaded {len(self.instrument_map)} NFO instruments and structured map.")
                return True
            return False
        except Exception as e:
            logger.error(f"FNO_WEBSOCKET: Failed to load NFO instruments from Redis cache: {e}", exc_info=True)
            return False

    def _get_target_tokens_from_movers(self, target_symbols: set) -> list:
        initial_tokens = []
        structured = self.fno_structured_map

        live_ohlc = {}
        try:
            live_ohlc_raw = self.redis_client.get('live_ohlc_data')
            if live_ohlc_raw:
                if isinstance(live_ohlc_raw, (bytes, bytearray)):
                    live_ohlc_raw = live_ohlc_raw.decode('utf-8')
                live_ohlc = json.loads(live_ohlc_raw)
        except Exception:
            pass

        for target in target_symbols:
            target_key = target.upper()
            if target_key not in structured:
                matches = [k for k in structured.keys() if k.startswith(target_key) or target_key.startswith(k)]
                if matches:
                    target_key = matches[0]
                else:
                    continue

            expiries = sorted([e for e in structured[target_key].keys() if e is not None])
            chosen_expiry = expiries[0] if expiries else None
            if chosen_expiry not in structured[target_key]:
                chosen_expiry = next(iter(structured[target_key].keys())) if structured[target_key] else None

            instruments_for_expiry = structured[target_key].get(chosen_expiry, [])
            if not instruments_for_expiry:
                continue

            for inst in instruments_for_expiry:
                if inst['type'] == 'FUT' and inst.get('token'):
                    initial_tokens.append(inst['token'])

            underlying_ltp = None
            underlying_ltp_data = live_ohlc.get(target_key) or live_ohlc.get(target)
            if isinstance(underlying_ltp_data, dict):
                try:
                    underlying_ltp = float(underlying_ltp_data.get('ltp') or underlying_ltp_data.get('last_price') or 0.0)
                except Exception:
                    underlying_ltp = None

            opt_candidates = [x for x in instruments_for_expiry if (x['type'] == 'OPT' or x['symbol'].endswith('CE') or x['symbol'].endswith('PE')) and x.get('strike') is not None]

            if opt_candidates:
                max_pick = max(12, self.atm_strike_count * 2)
                if underlying_ltp and len(opt_candidates) > 0:
                    opt_candidates.sort(key=lambda x: abs((x['strike'] or 0.0) - underlying_ltp))
                    selected_opts = opt_candidates[:max_pick]
                else:
                    selected_opts = opt_candidates[:max_pick]

                for s in selected_opts:
                    if s.get('token'):
                        initial_tokens.append(s['token'])

        # dedupe while preserving order
        seen = set()
        result = []
        for t in initial_tokens:
            if t not in seen and t:
                seen.add(t)
                result.append(t)
        return result

    def _periodic_mover_update_thread(self):
        logger.info(f"FNO_WEBSOCKET: Starting periodic mover update thread (interval={self.DYNAMIC_UPDATE_INTERVAL}s).")
        while self.running:
            time.sleep(self.DYNAMIC_UPDATE_INTERVAL)

            if not getattr(self.kws, 'is_connected', lambda: False)() or not is_market_open():
                continue

            try:
                fno_gainers_raw = self.redis_client.get('fno_gainers')
                fno_losers_raw = self.redis_client.get('fno_losers')

                gainers = []
                if fno_gainers_raw:
                    if isinstance(fno_gainers_raw, (bytes, bytearray)):
                        fno_gainers_raw = fno_gainers_raw.decode('utf-8')
                    gainers = json.loads(fno_gainers_raw)
                    
                losers = []
                if fno_losers_raw:
                    if isinstance(fno_losers_raw, (bytes, bytearray)):
                        fno_losers_raw = fno_losers_raw.decode('utf-8')
                    losers = json.loads(fno_losers_raw)

                target_symbols = set()
                for stock in (gainers[:10] + losers[:10]):
                    sym = stock.get('symbol') if isinstance(stock, dict) else stock
                    if sym:
                        target_symbols.add(str(sym).upper())

                if not target_symbols:
                    logger.debug("FNO_WEBSOCKET: Movers list is empty during update check.")
                    continue

                if target_symbols == self.last_underlying_movers:
                    logger.debug("FNO_WEBSOCKET: Underlying movers are unchanged. Skipping token recalculation.")
                    continue

                new_required_tokens = set(self._get_target_tokens_from_movers(target_symbols))

                tokens_to_subscribe = list(new_required_tokens - self.subscribed_tokens)
                tokens_to_unsubscribe = list(self.subscribed_tokens - new_required_tokens)

                if tokens_to_unsubscribe and getattr(self.kws, 'unsubscribe', None):
                    try:
                        self.kws.unsubscribe(tokens_to_unsubscribe)
                        logger.info(f"FNO_WEBSOCKET: Unsubscribed from {len(tokens_to_unsubscribe)} old tokens.")
                    except Exception:
                        logger.exception("FNO_WEBSOCKET: Error while unsubscribing tokens")

                if tokens_to_subscribe and getattr(self.kws, 'subscribe', None):
                    try:
                        self.kws.subscribe(tokens_to_subscribe)
                        if getattr(self.kws, 'set_mode', None):
                            self.kws.set_mode(self.kws.MODE_FULL, tokens_to_subscribe)
                        logger.info(f"FNO_WEBSOCKET: Dynamically subscribed to {len(tokens_to_subscribe)} new tokens.")
                    except Exception:
                        logger.exception("FNO_WEBSOCKET: Error while subscribing tokens")

                self.subscribed_tokens = new_required_tokens
                self.last_underlying_movers = target_symbols

            except Exception as e:
                logger.error(f"FNO_WEBSOCKET: Error during periodic subscription update: {e}", exc_info=True)

    def on_ticks(self, ws, ticks):
        for tick in ticks:
            token = tick.get('instrument_token') or tick.get('token') or tick.get('instrumentToken')
            if token is None: continue
            token_str = str(token)

            if token_str not in self.instrument_map: continue

            instrument = self.instrument_map[token_str]
            ltp_raw = tick.get('last_price') or tick.get('ltp') or instrument.get('ltp', 0.0)
            try: instrument['ltp'] = float(ltp_raw)
            except Exception: instrument['ltp'] = float(instrument.get('ltp', 0.0) or 0.0)

            current_ltp = instrument['ltp']

            if current_ltp > 0.0:
                if instrument.get('open', 0.0) == 0.0: instrument['open'] = current_ltp
                instrument['high'] = max(instrument.get('high', 0.0), current_ltp)
                current_low = instrument.get('low', float('inf'))
                if current_low == 0.0 or current_low == float('inf') or current_ltp < current_low: instrument['low'] = current_ltp

            instrument['depth'] = tick.get('depth', instrument.get('depth', {}))

        now = time.time()
        if now - self.last_publish_time > 0.1:
            self.publish_to_redis()
            self.last_publish_time = now

    def publish_to_redis(self):
        try:
            live_nfo_data = {}
            for data in self.instrument_map.values():
                if data.get('ltp', 0.0) > 0.0:
                    live_nfo_data[data['symbol']] = {
                        'ltp': data.get('ltp', 0.0), 'depth': data.get('depth', {}),
                        'open': data.get('open', 0.0), 'high': data.get('high', 0.0),
                        'low': data.get('low', 0.0), 'close': data.get('close', 0.0),
                    }
            if live_nfo_data:
                self.redis_client.set('live_nfo_ohlc_data', json.dumps(live_nfo_data, default=_default_json_serializer))
                logger.debug(f"FNO_WEBSOCKET: Published {len(live_nfo_data)} NFO instruments with OHLC/Depth.")
            else:
                logger.debug("FNO_WEBSOCKET: No live NFO data received or to publish yet.")
        except Exception as e:
            logger.error(f"FNO_WEBSOCKET: Error publishing NFO data to Redis: {e}", exc_info=True)

    def on_connect(self, ws, response):
        logger.info("FNO_WEBSOCKET: Connection to Kite Ticker successful.")
        self.redis_client.set('fno_websocket_status', 'connected')
        try:
            self._run_initial_subscription()
        except Exception as e:
            logger.error(f"FNO_WEBSOCKET: Initial subscription failed: {e}", exc_info=True)

    def _run_initial_subscription(self):
        max_attempts = 5; wait_delay = 5; target_symbols = set()

        for attempt in range(max_attempts):
            try:
                fno_gainers_raw = self.redis_client.get('fno_gainers')
                fno_losers_raw = self.redis_client.get('fno_losers')

                # FIX: Defensive decoding of Redis bytes before JSON parsing
                gainers_data = fno_gainers_raw
                if isinstance(gainers_data, (bytes, bytearray)):
                    gainers_data = gainers_data.decode('utf-8')
                gainers = json.loads(gainers_data) if gainers_data else []

                losers_data = fno_losers_raw
                if isinstance(losers_data, (bytes, bytearray)):
                    losers_data = losers_data.decode('utf-8')
                losers = json.loads(losers_data) if losers_data else []

                for stock in (gainers[:10] + losers[:10]):
                    if isinstance(stock, dict): sym = stock.get('symbol')
                    else: sym = stock
                    if sym: target_symbols.add(str(sym).upper())

                if target_symbols:
                    logger.info(f"FNO_WEBSOCKET: Loaded movers on attempt {attempt + 1}. targets={list(target_symbols)}")
                    break
                logger.warning(f"FNO_WEBSOCKET: F&O movers empty (Attempt {attempt + 1}/{max_attempts}). Sleeping {wait_delay}s")
            except Exception as e:
                logger.error(f"FNO_WEBSOCKET: Error reading movers (Attempt {attempt + 1}): {e}", exc_info=True)

            if attempt < max_attempts - 1: time.sleep(wait_delay)
            else: logger.critical("FNO_WEBSOCKET: Failed to load F&O movers after retries. Cannot subscribe."); return

        if not target_symbols: logger.warning("FNO_WEBSOCKET: No target underlying symbols found. Aborting NFO subscription."); return

        initial_tokens = self._get_target_tokens_from_movers(target_symbols)

        self.subscribed_tokens = set(initial_tokens)
        self.last_underlying_movers = target_symbols

        if initial_tokens:
            try:
                self.kws.subscribe(initial_tokens)
                self.kws.set_mode(self.kws.MODE_FULL, initial_tokens)
                logger.info(f"FNO_WEBSOCKET: Subscribed to {len(initial_tokens)} tokens for {len(target_symbols)} targets. sample={initial_tokens[:24]}")
            except Exception as e: logger.error(f"FNO_WEBSOCKET: Failed to subscribe: {e}", exc_info=True)
        else:
            sample = []
            for k, v in list(self.instrument_map.items())[:20]: sample.append({'token': k, 'symbol': v.get('symbol'), 'instrument_type': v.get('instrument_type'), 'expiry': v.get('expiry')})
            logger.warning(f"FNO_WEBSOCKET: No tokens matched movers. total_nfo_instruments={len(self.instrument_map)} sample_instruments={sample[:10]}")

    def on_close(self, ws, code, reason):
        logger.warning(f"FNO_WEBSOCKET: Connection closed - Code: {code}, Reason: {reason}")
        self.redis_client.set('fno_websocket_status', 'disconnected')

    def on_error(self, ws, code, reason):
        logger.error(f"FNO_WEBSOCKET: Connection error - Code: {code}, Reason: {reason}")

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
    def start_connection(self):
        logger.info("FNO_WEBSOCKET: Starting Kite Ticker connection...")
        self.kws.connect(threaded=True)

    def stop_connection(self):
        logger.info("FNO_WEBSOCKET: Stopping Kite Ticker connection...")
        try:
            if getattr(self.kws, 'is_connected', lambda: False)() or getattr(self.kws, 'is_connecting', lambda: False)():
                self.kws.close(1000, "Manual stop")
        except Exception:
            logger.exception("FNO_WEBSOCKET: Error while closing kws")

    def run(self):
        if not self._load_instruments_from_cache():
            logger.critical("FNO_WEBSOCKET: CRITICAL FAILURE. NFO instruments not loaded. Thread cannot start.")
            return

        threading.Thread(target=self._periodic_mover_update_thread, daemon=True).start()
        threading.Thread(target=self._external_subscription_listener, daemon=True).start()

        while self.running:
            connect_time = datetime_time(9, 10); disconnect_time = datetime_time(15, 35)
            now_dt_ist = dt.now(IST); today_ist = now_dt_ist.date()

            if today_ist.weekday() >= 5:
                time.sleep(3600)
                continue

            target_connect_dt_ist = IST.localize(dt.combine(today_ist, connect_time))
            if now_dt_ist < target_connect_dt_ist:
                sleep_seconds = (target_connect_dt_ist - now_dt_ist).total_seconds()
                if sleep_seconds > 0: time.sleep(sleep_seconds)

            if dt.now(IST).time() < disconnect_time:
                if not getattr(self.kws, 'is_connected', lambda: False)():
                    try: self.start_connection()
                    except Exception as e: logger.error(f"FNO_WEBSOCKET: Initial connection failed: {e}", exc_info=True)

                while dt.now(IST).time() < disconnect_time and self.running:
                    if not getattr(self.kws, 'is_connected', lambda: False)():
                        logger.warning("FNO_WEBSOCKET: Disconnected during trading hours. Reconnecting...")
                        try: self.start_connection()
                        except Exception as e: logger.error(f"FNO_WEBSOCKET: Reconnect failed: {e}", exc_info=True)
                    time.sleep(10)

            if getattr(self.kws, 'is_connected', lambda: False)():
                self.stop_connection()

            tomorrow_ist = today_ist + timedelta(days=1)
            next_target_connect_dt = IST.localize(dt.combine(tomorrow_ist, connect_time))
            wake_up_dt = next_target_connect_dt - timedelta(minutes=1)
            sleep_until_tomorrow_seconds = (wake_up_dt - dt.now(IST)).total_seconds()
            if sleep_until_tomorrow_seconds > 0:
                time.sleep(sleep_until_tomorrow_seconds)

    def _external_subscription_listener(self):
        """Handles external subscription requests via Redis PubSub. Non-blocking-friendly."""
        try:
            self._pubsub = self.redis_client.pubsub()
            self._pubsub.subscribe('subscribe_tokens')
            logger.info("FNO_WEBSOCKET: Listening for dynamic token subscriptions on 'subscribe_tokens' channel.")

            for message in self._pubsub.listen():
                if not self.running:
                    break
                if message['type'] == 'message':
                    try:
                        # Defensive decoding of message['data'] which is bytes
                        raw_data = message['data']
                        if isinstance(raw_data, (bytes, bytearray)):
                            new_tokens = json.loads(raw_data.decode('utf-8'))
                        else:
                            new_tokens = json.loads(raw_data)
                            
                        # Ensure tokens are integers/safe for KiteTicker
                        new_tokens_to_add = [safe_int(t) for t in new_tokens if safe_int(t) and safe_int(t) not in self.subscribed_tokens]
                        
                        if new_tokens_to_add and getattr(self.kws, 'is_connected', lambda: False)():
                            self.kws.subscribe(new_tokens_to_add)
                            if getattr(self.kws, 'set_mode', None):
                                self.kws.set_mode(self.kws.MODE_FULL, new_tokens_to_add)
                            self.subscribed_tokens.update(new_tokens_to_add)
                            logger.info(f"FNO_WEBSOCKET: Dynamically subscribed to {len(new_tokens_to_add)} new tokens via external channel.")
                    except Exception as e:
                        logger.error(f"FNO_WEBSOCKET: Error processing dynamic subscription message: {e}")
        except Exception:
            logger.exception("FNO_WEBSOCKET: External subscription listener failed")
        finally:
            try:
                if self._pubsub:
                    # Attempt to close the connection to unblock listener/thread gracefully
                    if hasattr(self._pubsub, 'close'):
                        self._pubsub.close()
            except Exception:
                pass

    def stop(self):
        """Signal manager to stop and cleanup resources."""
        self.running = False
        try:
            if self._pubsub:
                # Use the close() method if available for graceful shutdown
                if hasattr(self._pubsub, 'close'):
                    self._pubsub.close()
        except Exception:
            pass
        try:
            self.stop_connection()
        except Exception:
            pass


# --- Live Ticker Manager (Cash/Index) with candle aggregation and prev-day HL caching ---
class LiveTickerManager:
    """WebSocket manager for Cash and Index instruments. Produces top-gainers/losers and F&O movers."""

    def __init__(self, api_key, access_token):
        self.kws = KiteTicker(api_key, access_token)
        self.redis_client = get_redis_connection()
        self.instrument_map = {}
        self.subscribed_tokens = set()
        self.last_publish_time = 0
        self.running = True

        # Candle aggregator instance
        self.candle_agg = CandleAggregator(self.redis_client, max_candles=5000)

        self.kws.on_ticks = self.on_ticks
        self.kws.on_connect = self.on_connect
        self.kws.on_close = self.on_close
        self.kws.on_error = self.on_error
        self.kws.on_order_update = self.on_order_update

    def _load_instruments_from_cache(self) -> bool:
        try:
            instrument_map_json = self.redis_client.get('instrument_map')
            if instrument_map_json:
                if isinstance(instrument_map_json, (bytes, bytearray)):
                    instrument_map_json = instrument_map_json.decode('utf-8')
                full_map = json.loads(instrument_map_json)
                self.instrument_map = {k: v for k, v in full_map.items() if v.get('exchange') == 'NSE' or v.get('symbol') in settings.INDEX_SYMBOLS}
                logger.info(f"WEBSOCKET: Loaded {len(self.instrument_map)} Cash/Index instruments from cache.")
                return True
            else:
                logger.error("WEBSOCKET: 'instrument_map' not found in Redis cache.")
                return False
        except Exception as e:
            logger.error(f"WEBSOCKET: Failed to load instruments from Redis cache: {e}", exc_info=True)
            return False

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
        except Exception as e:
            logger.error(f"WEBSOCKET_ORDER_UPDATE: Error processing order update: {e}", exc_info=True)

    def on_ticks(self, ws, ticks):
        for tick in ticks:
            token = tick.get('instrument_token') or tick.get('token') or tick.get('instrumentToken')
            if token is None: continue
            token_str = str(token)

            if token_str not in self.instrument_map: continue

            instrument = self.instrument_map[token_str]
            ltp_raw = tick.get('last_price') or tick.get('ltp') or instrument.get('ltp', 0.0)
            try: instrument['ltp'] = float(ltp_raw)
            except Exception: instrument['ltp'] = float(instrument.get('ltp', 0.0) or 0.0)

            if 'ohlc' in tick and isinstance(tick['ohlc'], dict):
                ohlc = tick['ohlc']
                # Use ohlc fields if available from the tick data (e.g., in MODE_FULL)
                instrument['open'] = ohlc.get('open', instrument.get('open', 0.0))
                instrument['high'] = ohlc.get('high', instrument.get('high', 0.0))
                instrument['low'] = ohlc.get('low', instrument.get('low', 0.0))
                instrument['close'] = ohlc.get('close', instrument.get('close', 0.0))
            else:
                # Fallback to direct fields
                instrument['open'] = tick.get('open') or instrument.get('open', 0.0)
                instrument['high'] = tick.get('high') or instrument.get('high', 0.0)
                instrument['low'] = tick.get('low') or instrument.get('low', 0.0)
                instrument['close'] = tick.get('close') or instrument.get('close', 0.0)

            current_ltp = instrument['ltp']
            if current_ltp > 0.0:
                instrument['high'] = max(instrument.get('high', 0.0), current_ltp)
                current_low = instrument.get('low', float('inf'))
                if current_low == 0.0 or current_low == float('inf') or current_ltp < current_low: instrument['low'] = current_ltp

            raw_vol = tick.get('volume_traded') or tick.get('total_traded_quantity') or None
            if raw_vol is not None:
                try: instrument['volume'] = int(raw_vol)
                except Exception: instrument['volume'] = int(instrument.get('volume', 0) or 0)
            else: instrument['volume'] = int(instrument.get('volume', 0) or 0)

            prev_close = instrument.get('close', 0.0)
            if prev_close:
                try: instrument['change'] = ((instrument['ltp'] - prev_close) / prev_close) * 100
                except Exception: instrument['change'] = instrument.get('change', 0.0)

            # --- Feed the candle aggregator for configured stock list only ---
            try:
                sym = instrument.get('symbol')
                if sym and sym in settings.STOCK_INDEX_MAPPING and instrument.get('exchange') == 'NSE':
                    ts_raw = tick.get('timestamp') or tick.get('tradable_at') or None
                    if ts_raw:
                        try:
                            # Parse Kite ticker timestamp which is often UTC
                            ts = dt.fromisoformat(ts_raw.replace('Z', '+00:00'))
                            if ts.tzinfo is None: ts = pytz.utc.localize(ts).astimezone(IST)
                        except Exception:
                            ts = dt.now(IST)
                    else:
                        ts = dt.now(IST)

                    cum_vol = int(raw_vol) if raw_vol is not None else None

                    self.candle_agg.process_tick(sym, float(instrument['ltp'] or 0.0), ts, cum_vol)
            except Exception:
                logger.exception("WEBSOCKET: Candle aggregator error")

        now = time.time()
        if now - self.last_publish_time > 0.1:
            self.publish_to_redis()
            self.last_publish_time = now

    def publish_to_redis(self):
        try:
            pipe = self.redis_client.pipeline()
            live_ohlc_for_engine = {}
            stocks_to_publish = []

            for token, data in self.instrument_map.items():
                if data.get('ltp', 0.0) <= 0.0 or data.get('open', 0.0) <= 0.0: continue

                live_ohlc_for_engine[data['symbol']] = {
                    'ltp': data.get('ltp', 0.0), 'open': data.get('open', 0.0), 'high': data.get('high', 0.0),
                    'low': data.get('low', 0.0), 'close': data.get('close', 0.0), 'change': data.get('change', 0.0),
                    'sector': settings.STOCK_INDEX_MAPPING.get(data['symbol'], 'N/A'), 'volume': int(data.get('volume', 0)),
                }

                if data['symbol'] in settings.STOCK_INDEX_MAPPING and data.get('exchange') == 'NSE':
                    stocks_to_publish.append({
                        'symbol': data['symbol'], 'ltp': data['ltp'], 'change': data['change'],
                        'day_open': data['open'], 'day_high': data['high'], 'day_low': data['low'],
                        'sector': settings.STOCK_INDEX_MAPPING.get(data['symbol'], 'N/A'), 'volume': int(data.get('volume', 0)),
                    })

            if live_ohlc_for_engine:
                pipe.set('live_ohlc_data', json.dumps(live_ohlc_for_engine, default=_default_json_serializer))
                # Ticks channel kept for visibility
                pipe.publish('ticks_channel', json.dumps([
                    {'instrument_token': token, 'last_price': data.get('ltp', 0), 'volume': int(data.get('volume', 0))}
                    for token, data in self.instrument_map.items() if data.get('ltp', 0) > 0 and data.get('exchange') == 'NSE'
                ]))

            fno_candidates = [s for s in stocks_to_publish if s['symbol'] in FNO_STOCKS]
            fno_candidates.sort(key=lambda x: x['change'], reverse=True)

            fno_gainers = fno_candidates[:20]; fno_losers = fno_candidates[-20:][::-1]

            pipe.set('fno_gainers', json.dumps(fno_gainers, default=_default_json_serializer))
            pipe.set('fno_losers', json.dumps(fno_losers, default=_default_json_serializer))

            stocks_to_publish.sort(key=lambda x: x['change'], reverse=True)
            indices = [inst for inst in self.instrument_map.values() if inst.get('symbol') in settings.INDEX_SYMBOLS and inst.get('ltp', 0.0) > 0.0]
            indices.sort(key=lambda x: x['change'], reverse=True)

            pipe.set('top_gainers', json.dumps(stocks_to_publish[:30], default=_default_json_serializer))
            pipe.set('top_losers', json.dumps(stocks_to_publish[-30:][::-1], default=_default_json_serializer))
            pipe.set('top_sectors', json.dumps(indices[:10], default=_default_json_serializer))
            pipe.set('bottom_sectors', json.dumps(indices[-10:][::-1], default=_default_json_serializer))

            pipe.execute()

        except Exception as e:
            logger.error(f"WEBSOCKET: Error publishing to Redis: {e}", exc_info=True)

    def on_connect(self, ws, response):
        logger.info("WEBSOCKET: Connection to Kite Ticker successful.")
        self.redis_client.set('websocket_status', 'connected')

        stock_tokens = [safe_int(token) for token, data in self.instrument_map.items() if data.get('symbol') in settings.STOCK_INDEX_MAPPING and data.get('exchange') == 'NSE']
        index_tokens = [safe_int(token) for token, data in self.instrument_map.items() if data.get('symbol') in settings.INDEX_SYMBOLS]

        # --- Ensure all stocks in prev_day_ohlc are subscribed ---
        additional_symbols_to_subscribe = set()
        try:
            target_symbols = self.redis_client.hkeys('prev_day_ohlc')
            if target_symbols:
                decoded = set()
                for s in target_symbols:
                    try:
                        if isinstance(s, (bytes, bytearray)):
                            decoded.add(s.decode('utf-8'))
                        else:
                            decoded.add(str(s))
                    except Exception:
                        continue
                target_symbols = decoded

                for token, data in self.instrument_map.items():
                    if data.get('symbol') in target_symbols and data.get('exchange') == 'NSE':
                        additional_symbols_to_subscribe.add(safe_int(token))
        except Exception:
            logger.exception("WEBSOCKET: Error determining additional symbols from prev_day_ohlc hash.")

        tokens_to_subscribe = list({t for t in (stock_tokens + index_tokens + list(additional_symbols_to_subscribe)) if t})
        self.subscribed_tokens = set(tokens_to_subscribe)

        if tokens_to_subscribe:
            try:
                ws.subscribe(tokens_to_subscribe)
                ws.set_mode(ws.MODE_FULL, tokens_to_subscribe)
                logger.info(f"WEBSOCKET: Subscribed to {len(tokens_to_subscribe)} Cash/Index tokens (including {len(additional_symbols_to_subscribe)} redundancy checks).")
            except Exception:
                logger.exception("WEBSOCKET: Failed to subscribe to tokens on connect")

    def on_close(self, ws, code, reason):
        logger.warning(f"WEBSOCKET: Connection closed - Code: {code}, Reason: {reason}")
        self.redis_client.set('websocket_status', 'disconnected')

    def on_error(self, ws, code, reason):
        logger.error(f"WEBSOCKET: Connection error - Code: {code}, Reason: {reason}")

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
    def start_connection(self):
        logger.info("WEBSOCKET: Starting Kite Ticker connection...")
        self.kws.connect(threaded=True)

    def stop_connection(self):
        logger.info("WEBSOCKET: Stopping Kite Ticker connection...")
        try:
            if getattr(self.kws, 'is_connected', lambda: False)() or getattr(self.kws, 'is_connecting', lambda: False)():
                self.kws.close(1000, "Manual stop")
        except Exception:
            logger.exception("WEBSOCKET: Error while closing kws")

    def run(self):
        if not self._load_instruments_from_cache():
            logger.warning("WEBSOCKET: Instrument map not found. Attempting to build it now...")
            try:
                cache_instruments_for_day()
                if not self._load_instruments_from_cache():
                    logger.critical("WEBSOCKET: CRITICAL FAILURE. Could not build or load instrument cache. WebSocket thread cannot start.")
                    return
            except Exception as e:
                logger.critical(f"WEBSOCKET: CRITICAL FAILURE. Exception during cache rebuild: {e}.")
                return

        try:
            master_account = Account.objects.filter(is_master=True, user__is_superuser=True).first()
            cache_previous_day_hl(master_account=master_account)
        except Exception:
            logger.exception("WEBSOCKET: Failed to cache previous day HL; continuing")

        while self.running:
            connect_time = datetime_time(9, 10); disconnect_time = datetime_time(15, 35)
            now_dt_ist = dt.now(IST); today_ist = now_dt_ist.date()

            if today_ist.weekday() >= 5:
                time.sleep(3600); continue

            target_connect_dt_ist = IST.localize(dt.combine(today_ist, connect_time))
            if now_dt_ist < target_connect_dt_ist:
                sleep_seconds = (target_connect_dt_ist - now_dt_ist).total_seconds()
                if sleep_seconds > 0: time.sleep(sleep_seconds)

            if dt.now(IST).time() < disconnect_time:
                if not getattr(self.kws, 'is_connected', lambda: False)():
                    try: self.start_connection()
                    except Exception as e: logger.error(f"WEBSOCKET: Initial connection attempt failed: {e}", exc_info=True)

                while dt.now(IST).time() < disconnect_time and self.running:
                    if not getattr(self.kws, 'is_connected', lambda: False)():
                        logger.warning("WEBSOCKET: Disconnected during trading hours! Attempting to reconnect...")
                        try: self.start_connection()
                        except Exception as e: logger.error(f"WEBSOCKET: Reconnection attempt failed: {e}", exc_info=True)
                    time.sleep(30)

            try: self.candle_agg.flush_all()
            except Exception: logger.exception("WEBSOCKET: Error flushing candles at market close")

            if getattr(self.kws, 'is_connected', lambda: False)():
                self.stop_connection()

            tomorrow_ist = today_ist + timedelta(days=1)
            next_target_connect_dt = IST.localize(dt.combine(tomorrow_ist, connect_time))
            wake_up_dt = next_target_connect_dt - timedelta(minutes=1)
            sleep_until_tomorrow_seconds = (wake_up_dt - dt.now(IST)).total_seconds()
            if sleep_until_tomorrow_seconds > 0:
                time.sleep(sleep_until_tomorrow_seconds)

    def stop(self):
        self.running = False
        try:
            self.stop_connection()
        except Exception:
            pass


# --- Thread start helpers ---

def start_websocket_thread(master_account):
    if not redis_client: logger.error("WEBSOCKET: Cannot start, Redis client is not available."); return

    ticker_manager = LiveTickerManager(master_account.api_key, master_account.access_token)
    thread = threading.Thread(target=ticker_manager.run, daemon=True)
    thread.start()
    logger.info("WEBSOCKET: LiveTickerManager (Cash/Index) thread has been started.")
    return ticker_manager


def start_fno_websocket_thread(master_account, subscription_strategy='nearest_expiry', atm_strike_count=8):
    if not redis_client: logger.error("FNO_WEBSOCKET: Cannot start, Redis client is not available."); return

    fno_ticker_manager = FnoTickerManager(master_account.api_key, master_account.access_token, subscription_strategy=subscription_strategy, atm_strike_count=atm_strike_count)
    thread = threading.Thread(target=fno_ticker_manager.run, daemon=True)
    thread.start()
    logger.info("FNO_WEBSOCKET: FnoTickerManager (NFO/Options) thread has been started.")
    return fno_ticker_manager