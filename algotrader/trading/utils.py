import math
import pytz
import requests
import json
import calendar
import ssl
import redis
import logging
import os
from datetime import datetime, date, time as dt_time, timedelta
from django.conf import settings
from kiteconnect import KiteConnect
from .models import Account

logger = logging.getLogger(__name__)
REDIS_POOL = None

def get_redis_connection():
    """
    Establishes a connection to Redis using a shared connection pool
    for efficiency and scalability.
    """
    global REDIS_POOL
    
    # The first time this is called, the pool will be created.
    # Subsequent calls will reuse the existing pool.
    if REDIS_POOL is None:
        try:
            redis_url = os.environ.get('REDIS_URL')
            
            if not redis_url:
                # This block runs ONLY on your local machine.
                redis_url = 'redis://localhost:6379'
                logger.info("Attempting to connect to local Redis...")
                REDIS_POOL = redis.ConnectionPool.from_url(redis_url, decode_responses=True)
            else:
                # This block runs ONLY on Heroku
                logger.info("Attempting to connect to Heroku Redis with SSL...")
                REDIS_POOL = redis.ConnectionPool.from_url(
                    redis_url, 
                    ssl_cert_reqs=ssl.CERT_NONE, 
                    decode_responses=True,
                    health_check_interval=30 # Keeps the connection alive
                )
            
            # Test the connection to ensure the pool is valid
            r = redis.Redis(connection_pool=REDIS_POOL)
            r.ping()
            logger.info("Successfully connected to Redis and created connection pool.")

        except redis.exceptions.ConnectionError as e:
            logger.warning(f"Could not connect to Redis. This is expected if Redis is not running locally. Error: {e}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred while creating Redis pool: {e}")
            return None

    # If the pool was successfully created, return a new client from the pool.
    if REDIS_POOL:
        return redis.Redis(connection_pool=REDIS_POOL)
    
    return None

def get_kite(account):
    """Initializes a KiteConnect object for a given account."""
    if not account or not account.access_token:
        return None
    try:
        kite = KiteConnect(api_key=account.api_key)
        kite.set_access_token(account.access_token)
        return kite
    except Exception as e:
        logger.error(f"Failed to initialize Kite connection for {account}: {e}")
        return None

def get_live_kite_client():
    """
    FIX: A new helper function for background workers to get a fresh KiteConnect client.
    This ensures that each worker process always uses the latest access token.
    """
    try:
        master_account = Account.objects.filter(user__is_superuser=True, is_master=True).first()
        if not master_account or not master_account.access_token:
            logger.error("Master account not found or access token is missing.")
            return None
        return get_kite(master_account)
    except Exception as e:
        logger.error(f"Error fetching master account for Kite client: {e}", exc_info=True)
        return None
        
def is_market_open():
    """Checks if the Indian stock market is open."""
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    market_open = dt_time(9, 15)
    market_close = dt_time(15, 30)
    return now.weekday() < 5 and market_open <= now.time() <= market_close


def round_to_tick(price, tick_size=0.05):
    """Rounds a price to the nearest valid tick size."""
    return round(price / tick_size) * tick_size


def get_last_thursday_of_month(year, month):
    """Calculates the date of the last Thursday of a given month and year."""
    last_day = calendar.monthrange(year, month)[1]
    last_date = date(year, month, last_day)
    offset = (last_date.weekday() - 3) % 7 # 3 is Thursday
    return last_date - timedelta(days=offset)


# --- Black-Scholes Implementation ---

def norm_cdf(x):
    """Standard normal cumulative distribution function."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def bs_price(S, K, t, r, sigma, flag):
    """Black-Scholes price for a call or put option."""
    if t <= 0:
        return max(0.0, (S - K) if flag == 'c' else (K - S))
    if sigma <= 0:
        return max(0.0, (S - K) if flag == 'c' else (K - S))

    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * t) / (sigma * math.sqrt(t))
    d2 = d1 - sigma * math.sqrt(t)

    if flag == 'c':
        return S * norm_cdf(d1) - K * math.exp(-r * t) * norm_cdf(d2)
    else:
        return K * math.exp(-r * t) * norm_cdf(-d2) - S * norm_cdf(-d1)


def bs_vega(S, K, t, r, sigma):
    """Vega of an option in Black-Scholes model."""
    if t <= 0 or sigma <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * t) / (sigma * math.sqrt(t))
    return S * math.sqrt(t) * (1 / math.sqrt(2 * math.pi)) * math.exp(-0.5 * d1 ** 2)


def implied_volatility(market_price, S, K, t, r, flag, tol=1e-6, max_iter=100):
    """Calculates implied volatility using Newton-Raphson method."""
    sigma = 0.5
    for i in range(max_iter):
        price = bs_price(S, K, t, r, sigma, flag)
        vega = bs_vega(S, K, t, r, sigma)

        if vega < 1e-8:
            return None

        diff = price - market_price
        if abs(diff) < tol:
            return sigma

        sigma -= diff / vega

        if sigma <= 0:
            sigma = 0.001

    return sigma


def get_strike_step(ltp):
    """Determines the strike price step based on the LTP."""
    if ltp < 100:
        return 2.5
    elif ltp < 250:
        return 5
    elif ltp < 500:
        return 10
    elif ltp < 1000:
        return 20
    elif ltp < 2000:
        return 50
    else:
        return 100


def send_telegram_message(message, bot_token=None, chat_id=None):
    """
    Sends a message to a Telegram channel.
    Uses user-specific credentials if provided, otherwise falls back to superuser defaults.
    """
    token = bot_token or settings.TELEGRAM_BOT_TOKEN
    chat_id = chat_id or settings.TELEGRAM_CHAT_ID

    if not token or not chat_id:
        logger.warning("Telegram credentials are not configured.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {'chat_id': chat_id, 'text': message, 'parse_mode': 'HTML'}
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        logger.error(f"Error sending Telegram message: {e}")

def get_option_ltp(primary_account, trade_symbol, instrument_map=None, live_ohlc_data=None):
    """
    Fetch the Last Traded Price (LTP) for an option symbol.

    Priority:
     1) Use provided live_ohlc_data snapshot (fast).
     2) Use live_nfo_ohlc_data from Redis.
     3) Fallback to Kite API (kite.ltp) and publish subscribe_tokens for websocket.

    Returns float LTP or 0.0 on failure.
    """
    ltp = 0.0

    # 1) Try the provided snapshot first (caller may already have it)
    try:
        if live_ohlc_data and isinstance(live_ohlc_data, dict):
            dd = live_ohlc_data.get(trade_symbol)
            if dd:
                l = dd.get('ltp') or dd.get('last_price') or 0
                if l and float(l) > 0:
                    logger.info(f"Found LTP for {trade_symbol} in provided snapshot: {l}")
                    return float(l)
    except Exception:
        logger.exception("Error reading provided live_ohlc_data; continuing to Redis/API fallback.")

    # 2) Try Redis live_nfo_ohlc_data (fast authoritative feed)
    redis_client = None
    try:
        redis_client = get_redis_connection()
        if redis_client:
            raw = redis_client.get('live_nfo_ohlc_data')
            if raw:
                # raw might already be decoded (if decode_responses=True) or a JSON string
                try:
                    nfo_data = json.loads(raw) if isinstance(raw, str) else raw
                except Exception:
                    nfo_data = raw  # assume it's already a dict
                if isinstance(nfo_data, dict):
                    symbol_info = nfo_data.get(trade_symbol) or {}
                    l = symbol_info.get('ltp') or symbol_info.get('last_price') or 0
                    if l and float(l) > 0:
                        logger.info(f"Found LTP for {trade_symbol} in Redis live_nfo_ohlc_data: {l}")
                        return float(l)
    except Exception:
        logger.exception("Error fetching/parsing live_nfo_ohlc_data from Redis; continuing to API fallback.")

    # 3) Last-resort: Kite API
    logger.warning(f"LTP for {trade_symbol} not found in cache. Fetching from Kite API as fallback.")
    try:
        kite = get_kite(primary_account)
        if not kite:
            logger.error("Kite client not available for API LTP fallback.")
            return 0.0

        api_instrument = f"NFO:{trade_symbol}"
        ltp_response = kite.ltp(api_instrument)

        if ltp_response and ltp_response.get(api_instrument):
            last_price = ltp_response[api_instrument].get('last_price') or ltp_response[api_instrument].get('ltp')
            if last_price is not None:
                ltp = float(last_price)
                logger.info(f"Fetched LTP for {trade_symbol} from Kite API: {ltp}")

                # publish subscribe request to websocket manager so Redis feed gets populated next
                instrument_token = None
                try:
                    # If instrument_map maps tradingsymbol->inst, prefer that
                    if isinstance(instrument_map, dict):
                        inst = instrument_map.get(trade_symbol)
                        if inst and (inst.get('instrument_token') or inst.get('token')):
                            instrument_token = inst.get('instrument_token') or inst.get('token')
                        else:
                            # instrument_map might be token->inst mapping; iterate to find tradingsymbol
                            for key, inst_data in instrument_map.items():
                                try:
                                    if inst_data and inst_data.get('tradingsymbol') == trade_symbol:
                                        instrument_token = inst_data.get('instrument_token') or inst_data.get('token') or key
                                        break
                                except Exception:
                                    continue
                except Exception:
                    logger.exception("Error while searching instrument_map for instrument token.")

                # Ensure we have a redis client (may have failed earlier)
                try:
                    if redis_client is None:
                        redis_client = get_redis_connection()
                    if instrument_token and redis_client:
                        # Try to publish an integer token if possible, else string
                        try:
                            tok = int(instrument_token)
                            payload = json.dumps([tok])
                        except Exception:
                            payload = json.dumps([instrument_token])
                        try:
                            redis_client.publish('subscribe_tokens', payload)
                            logger.info(f"Published subscribe_tokens for token: {instrument_token}")
                        except Exception:
                            logger.exception("Failed to publish subscribe_tokens after API fetch.")
                    else:
                        logger.debug("Instrument token not found or redis client unavailable; cannot request websocket subscribe.")
                except Exception:
                    logger.exception("Error preparing/publishing subscribe_tokens after API fetch.")

                return ltp
            else:
                logger.error(f"Kite API returned no last_price for {trade_symbol}: {ltp_response}")
        else:
            logger.error(f"Kite API did not return expected data for {api_instrument}: {ltp_response}")
    except Exception as e:
        logger.exception(f"Error fetching LTP from API for {trade_symbol}: {e}")

    return 0.0