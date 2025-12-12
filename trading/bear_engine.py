"""
CashBreakdown Client (Short Sell Engine) - Final Production Version
- Target RECALCULATED based on Actual Executed Price.
- Fully Integrated with React Frontend Controls (Ban, Toggle, Panic).
- Uses Tiered Risk & Global -ettings.
"""

import json
import time
import logging
import threading
from math import floor
from datetime import datetime as dt, timedelta, date, time as dt_time
from typing import Dict, Optional, Any, List

import pytz
import redis
from django.db import transaction, models
from django.utils import timezone
from django.conf import settings
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_not_exception_type
from kiteconnect.exceptions import TokenException

from trading.models import Account, CashBreakdownTrade
from trading.utils import get_redis_connection, get_kite

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")
redis_client = get_redis_connection()

# ---------------------------------------------------------------------------
# Redis Keys (Must match Django Views/Frontend)
# ---------------------------------------------------------------------------
CANDLE_STREAM_KEY = getattr(settings, "BREAKDOWN_CANDLE_STREAM", "candle_1m")
LIVE_OHLC_KEY = getattr(settings, "BREAKDOWN_LIVE_OHLC_KEY", "live_ohlc_data")
PREV_DAY_HASH = getattr(settings, "BREAKDOWN_PREV_DAY_HASH", "prev_day_ohlc")

# Settings Keys (Written by Frontend)
KEY_GLOBAL_SETTINGS = "algo:settings:global"
KEY_BEAR_SETTINGS = "algo:settings:bear"  # <--- Uses Bear specific settings

# Operational Control Keys (Written by Frontend)
KEY_BLACKLIST = "algo:blacklist"                # Redis Set of banned symbols
KEY_ENGINE_ENABLED = "algo:engine:bear:enabled" # String "1" (ON) or "0" (OFF)
KEY_PANIC_TRIGGER = "algo:panic:bear"           # Exists if panic button pressed

# Constants
ENTRY_OFFSET_PCT = 0.0001
STOP_OFFSET_PCT = 0.0002
BREAKDOWN_MAX_CANDLE_PCT = 0.007
MAX_MONITORING_MINUTES = 6 
RECONCILE_SLEEP_PER_CALL = 0.2

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _get_prev_day_low(redis_conn: redis.Redis, symbol: str) -> Optional[float]:
    try:
        raw = redis_conn.hget(PREV_DAY_HASH, symbol)
        if not raw: return None
        if isinstance(raw, bytes): raw = raw.decode('utf-8')
        parsed = json.loads(raw)
        low = parsed.get("low")
        return float(low) if low is not None else None
    except: return None

def _parse_candle_ts(ts_str: Optional[str]) -> dt:
    if not ts_str: return timezone.now()
    try:
        dt_obj = dt.fromisoformat(ts_str)
        if dt_obj.tzinfo is None: return IST.localize(dt_obj)
        return dt_obj.astimezone(IST)
    except: return timezone.now()

def _parse_ratio_string(ratio_str: str, default: float) -> float:
    """Parses '1:2' or '1:1.5' into float 2.0 or 1.5"""
    try:
        if not ratio_str or ':' not in ratio_str: return default
        return float(ratio_str.split(':')[1])
    except: return default

# ---------------------------------------------------------------------------
# Main client
# ---------------------------------------------------------------------------
class CashBreakdownClient:
    DAILY_RESET_TIME = dt_time(20, 0, 0)

    def __init__(self, account: Account):
        self.account = account
        self.kite = get_kite(account)
        self.running = True
        self.last_reconcile_time = time.time()

        self.open_trades: Dict[str, CashBreakdownTrade] = {}
        self.pending_trades: Dict[str, CashBreakdownTrade] = {}

        self.group_name = f"CBD_GROUP:{self.account.id}"
        self.consumer_name = f"CBD_CONSUMER:{threading.get_ident()}" 

        # Redis keys
        today_iso = dt.now(IST).date().isoformat()
        self.limit_reached_key = f"breakdown_limit_reached:{self.account.id}:{today_iso}"
        self.trade_count_key = f"breakdown_trade_count:{self.account.id}:{today_iso}"
        self.active_entries_set = f"breakdown_active_entries:{self.account.id}"
        self.exiting_trades_set = f"breakdown_exiting_trades:{self.account.id}"
        self.entry_lock_key_prefix = f"cbd_entry_lock:{self.account.id}"
        self.daily_pnl_key = f"cbd_daily_realized_pnl:{self.account.id}:{today_iso}"

        if not redis_client:
            self.running = False; return

        # Initialize Stream
        try: redis_client.xgroup_create(CANDLE_STREAM_KEY, self.group_name, id='0', mkstream=True)
        except: pass

        self._load_trades_from_db()

    # ------------------- Settings & Status Fetching -------------------
    def _get_global_settings(self) -> Dict[str, Any]:
        try:
            data = redis_client.get(KEY_GLOBAL_SETTINGS)
            if data: return json.loads(data)
        except: pass
        return {"volume_criteria": []}

    def _get_bear_settings(self) -> Dict[str, Any]:
        """Fetch Bear engine specific settings."""
        try:
            data = redis_client.get(KEY_BEAR_SETTINGS)
            if data: return json.loads(data)
        except: pass
        return {}

    def _is_engine_buying_enabled(self) -> bool:
        try:
            val = redis_client.get(KEY_ENGINE_ENABLED)
            if val is None: return True 
            return val.decode('utf-8') == "1"
        except: return True

    def _is_symbol_blacklisted(self, symbol: str) -> bool:
        try: return redis_client.sismember(KEY_BLACKLIST, symbol)
        except: return False
# ------------------- Daily housekeeping -------------------
    def _daily_reset_trades(self) -> None:
        """Reset DB + Redis state once per day after DAILY_RESET_TIME."""
        now_ist = dt.now(IST)
        reset_time_passed = now_ist.time() >= self.DAILY_RESET_TIME

        target_reset_date = now_ist.date()
        if now_ist.time() < dt_time(0, 30):
            target_reset_date = now_ist.date() - timedelta(days=1)

        reset_flag_key = f"cbd_daily_reset_done:{self.account.id}:{target_reset_date.isoformat()}"

        if reset_time_passed and redis_client.set(reset_flag_key, "1", nx=True, ex=86400 * 2):
            logger.warning("CBD(%s): Performing daily reset for date=%s", self.account.user.username, target_reset_date)
            try:
                CashBreakdownTrade.objects.filter(account=self.account).delete()
                with redis_client.pipeline() as pipe:
                    pipe.delete(self.trade_count_key)
                    pipe.delete(self.limit_reached_key)
                    pipe.delete(self.active_entries_set)
                    pipe.delete(self.exiting_trades_set)
                    pipe.delete(self.daily_pnl_key)
                    for key in redis_client.keys(f"{self.entry_lock_key_prefix}:*"):
                        pipe.delete(key)
                    pipe.execute()

                self.open_trades.clear()
                self.pending_trades.clear()
                logger.info("CBD(%s): Daily reset complete.", self.account.user.username)
            except Exception as e:
                logger.critical("CBD(%s): Daily reset failed: %s", self.account.user.username, e, exc_info=True)
                redis_client.delete(reset_flag_key)

    # ------------------- DB Sync -------------------
    def _sync_state_from_db(self) -> None:
        try:
            today_start = IST.localize(dt.combine(dt.now(IST).date(), dt_time.min))
            daily_count = CashBreakdownTrade.objects.filter(
                account=self.account, created_at__gte=today_start,
                status__in=['PENDING_ENTRY', 'OPEN', 'PENDING_EXIT', 'CLOSED']
            ).count()
            redis_client.set(self.trade_count_key, daily_count)
            redis_client.expire(self.trade_count_key, 86400)
        except: pass

    def _load_trades_from_db(self) -> None:
        try:
            active_db = CashBreakdownTrade.objects.filter(
                account=self.account,
                status__in=["OPEN", "PENDING_EXIT", "PENDING", "PENDING_ENTRY"],
            )
            self.open_trades.clear()
            self.pending_trades.clear()
            symbols_to_monitor = []
            exiting_ids = []

            for trade in active_db:
                symbols_to_monitor.append(trade.symbol)
                if trade.status in ("OPEN", "PENDING_EXIT"): self.open_trades[trade.symbol] = trade
                if trade.status == "PENDING": self.pending_trades[trade.symbol] = trade
                if trade.status == "PENDING_EXIT": exiting_ids.append(trade.id)

            with redis_client.pipeline() as pipe:
                pipe.delete(self.active_entries_set)
                pipe.delete(self.exiting_trades_set)
                if symbols_to_monitor: pipe.sadd(self.active_entries_set, *symbols_to_monitor)
                if exiting_ids: pipe.sadd(self.exiting_trades_set, *exiting_ids)
                pipe.execute()
            self._sync_state_from_db()
        except: pass

    def _get_todays_symbol_counts(self) -> Dict[str, int]:
        today_start = IST.localize(dt.combine(dt.now(IST).date(), dt_time.min))
        qs = CashBreakdownTrade.objects.filter(account=self.account, created_at__gte=today_start, status__in=['PENDING_ENTRY', 'OPEN', 'PENDING_EXIT', 'CLOSED'])
        counts = qs.values("symbol").annotate(count=models.Count("symbol"))
        return {item["symbol"]: item["count"] for item in counts}

    def _get_live_ohlc(self) -> Dict[str, Any]:
        try:
            raw = redis_client.get(LIVE_OHLC_KEY)
            if isinstance(raw, bytes): raw = raw.decode('utf-8')
            return json.loads(raw) if raw else {}
        except: return {}

    # ------------------- Risk / Order Logic -------------------
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.1), retry=retry_if_not_exception_type(TokenException))
    def _check_and_increment_trade_count(self) -> bool:
        settings = self._get_bear_settings()
        max_trades = int(settings.get("total_trades", 5))
        lua_script = """
        local c = tonumber(redis.call('GET', KEYS[1]) or 0)
        if c >= tonumber(ARGV[1]) then return 0 end
        redis.call('INCR', KEYS[1]); redis.call('EXPIRE', KEYS[1], 86400); return 1
        """
        try: return bool(redis_client.eval(lua_script, 1, self.trade_count_key, max_trades))
        except: return False

    def _calculate_quantity(self, entry_price: float, sl_price: float) -> int:
        try:
            current_trade_count = int(redis_client.get(self.trade_count_key) or 0)
            tier = min(current_trade_count + 1, 3)
            settings = self._get_bear_settings()
            max_loss = float(settings.get(f"risk_trade_{tier}", 1000))
        except: max_loss = 1000.0

        risk = abs(sl_price - entry_price)
        if risk <= 0.001: return 0
        return max(0, int(floor(max_loss / risk)))

    def _place_order(self, symbol, qty, txn_type):
        return self.kite.place_order(tradingsymbol=symbol, quantity=qty, transaction_type=txn_type, product=self.kite.PRODUCT_MIS, order_type=self.kite.ORDER_TYPE_MARKET, exchange=self.kite.EXCHANGE_NSE, variety=self.kite.VARIETY_REGULAR)

    # ------------------- 1. SCANNER (Process Candle) -------------------
    def _process_candle(self, candle_payload: Dict[str, Any]) -> None:
        symbol = candle_payload.get("symbol")
        if not symbol: return

        # CHECK 1: Engine Enabled?
        if not self._is_engine_buying_enabled(): return 

        # CHECK 2: Blacklist?
        if self._is_symbol_blacklisted(symbol): return

        if redis_client.sismember(self.active_entries_set, symbol): return

        # Dynamic Trades Per Stock
        bear_settings = self._get_bear_settings()
        max_per_stock = int(bear_settings.get("trades_per_stock", 2))
        if self._get_todays_symbol_counts().get(symbol, 0) >= max_per_stock: return

        prev_low = _get_prev_day_low(redis_client, symbol)
        if not prev_low: return

        try:
            low = float(candle_payload.get("low") or 0)
            high = float(candle_payload.get("high") or 0)
            vol = int(candle_payload.get("volume") or 0)
            close = float(candle_payload.get("close") or 0)
            open_p = float(candle_payload.get("open") or 0)
            ts = _parse_candle_ts(candle_payload.get("ts"))
            vol_sma = float(candle_payload.get("vol_sma_375") or 0)
            vol_price_cr = float(candle_payload.get("vol_price_cr") or 0)
        except: return

        # Breakdown Strategy Logic
        ref = close if close > 0 else 1.0
        if not (close < open_p): return
        if not (open_p > prev_low > close): return
        if not (low < prev_low): return
        if ((high - low) / ref) > BREAKDOWN_MAX_CANDLE_PCT: return

        # Global Volume Check
        global_settings = self._get_global_settings()
        passed = False
        for level in global_settings.get("volume_criteria", []):
            if vol_price_cr >= float(level.get('min_vol_price_cr', 999)):
                if vol_sma > 101 and vol >= (vol_sma * float(level.get('sma_multiplier', 999))):
                    passed = True; break
        
        if not passed: return

        # Register Setup
        rr_mult = _parse_ratio_string(bear_settings.get("risk_reward", "1:2"), 2.0)
        entry = low * (1.0 - ENTRY_OFFSET_PCT)
        stop = high + (high * STOP_OFFSET_PCT)
        # Short Target = Entry - (Risk * R)
        risk = stop - entry
        target = entry - (rr_mult * risk)

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
                logger.info(f"CBD: Setup Found {symbol} (Target: {target:.2f})")
        except Exception as e: logger.error(f"CBD: Setup error {symbol}: {e}")

    # ------------------- 2. ENTRY EXECUTION -------------------
    def _try_enter_pending(self) -> None:
        if not self._is_engine_buying_enabled(): return 

        if not self.pending_trades: return
        live_ohlc = self._get_live_ohlc()
        to_remove = []
        now = dt.now(IST)

        for symbol, trade in list(self.pending_trades.items()):
            # Blacklist check
            if self._is_symbol_blacklisted(symbol):
                trade.status = "EXPIRED"; trade.exit_reason = "Blacklisted"; trade.save()
                redis_client.srem(self.active_entries_set, symbol)
                to_remove.append(symbol); continue

            lock_key = f"{self.entry_lock_key_prefix}:{symbol}"
            if redis_client.exists(lock_key): continue

            ltp = live_ohlc.get(symbol, {}).get("ltp", 0.0)
            if ltp == 0: continue

            # Time Expiry
            if trade.candle_ts and (now > trade.candle_ts + timedelta(minutes=MAX_MONITORING_MINUTES)):
                trade.status = "EXPIRED"; trade.save(); redis_client.srem(self.active_entries_set, symbol)
                to_remove.append(symbol); continue

            # Invalidated (Moved above SL)
            if ltp > trade.stop_level:
                trade.status = "EXPIRED"; trade.save(); redis_client.srem(self.active_entries_set, symbol)
                to_remove.append(symbol); continue

            # Trigger (Price moved below Entry)
            if ltp < trade.entry_level:
                if not redis_client.set(lock_key, "1", nx=True, ex=5): continue
                try:
                    qty = self._calculate_quantity(ltp, trade.stop_level)
                    if qty <= 0: raise ValueError("Qty 0")
                    if not self._check_and_increment_trade_count(): raise Exception("Global Limit")

                    oid = self._place_order(symbol, qty, "SELL") # SHORT
                    with transaction.atomic():
                        trade.status = "PENDING_ENTRY"; trade.quantity = qty; trade.entry_order_id = oid; trade.save()
                    logger.info(f"CBD: SELL {symbol} Qty:{qty}")
                except Exception as e:
                    logger.error(f"CBD: Entry failed {symbol}: {e}")
                    redis_client.decr(self.trade_count_key)
                    trade.status = "FAILED_ENTRY"; trade.save()
                    redis_client.srem(self.active_entries_set, symbol)
                finally:
                    redis_client.delete(lock_key)
                    to_remove.append(symbol)

        for s in to_remove: self.pending_trades.pop(s, None)

    # ------------------- 3. MONITORING (Always Runs) -------------------
    def monitor_trades(self) -> None:
        # Panic Check
        if redis_client.exists(KEY_PANIC_TRIGGER):
            redis_client.delete(KEY_PANIC_TRIGGER)
            logger.warning("CBD: PANIC BUTTON PRESSED.")
            self.force_square_off("Panic Button")

        if not self.open_trades: return
        settings = self._get_bear_settings()
        live_ohlc = self._get_live_ohlc()
        unrealized_pnl = 0.0

        trail_mult = _parse_ratio_string(settings.get("trailing_sl", "1:1"), 1.0)

        for symbol, trade in list(self.open_trades.items()):
            if redis_client.sismember(self.exiting_trades_set, trade.id): continue
            
            ltp = live_ohlc.get(symbol, {}).get("ltp", 0.0)
            if ltp == 0: continue
            
            try: trade = CashBreakdownTrade.objects.get(id=trade.id); self.open_trades[symbol] = trade
            except: continue

            # Short PnL: (Entry - Current) * Qty
            unrealized_pnl += (trade.entry_price - ltp) * trade.quantity
            risk = trade.stop_level - trade.entry_price

            # Trailing (If price drops significantly, move SL down to Entry)
            if risk > 0 and trade.stop_level > trade.entry_price:
                # If LTP <= Entry - (1R)
                trigger_price = trade.entry_price - (trail_mult * risk)
                if ltp <= trigger_price:
                    trade.stop_level = trade.entry_price
                    trade.save(update_fields=['stop_level'])
                    logger.info(f"CBD: TSL -> Breakeven for {symbol}")

            # Exits
            if ltp >= trade.stop_level: self.exit_trade(trade, ltp, "SL Hit")
            elif ltp <= trade.target_level: self.exit_trade(trade, ltp, "Target Hit")

        # Global P&L
        if settings.get("pnl_exit_enabled", False):
            try: realized = float(redis_client.get(self.daily_pnl_key) or 0)
            except: realized = 0.0
            net = realized + unrealized_pnl
            if net >= float(settings.get("max_profit", 999999)) or net <= -1 * float(settings.get("max_loss", 999999)):
                redis_client.sadd(self.limit_reached_key, "P&L_EXIT")
                self.force_square_off(f"P&L Exit (Net: {net:.2f})")

    def exit_trade(self, trade, ltp, reason):
        if redis_client.sismember(self.exiting_trades_set, trade.id): return
        try:
            oid = self._place_order(trade.symbol, trade.quantity, "BUY") # BUY TO COVER
            with transaction.atomic():
                t = CashBreakdownTrade.objects.select_for_update().get(id=trade.id)
                t.status = "PENDING_EXIT"; t.exit_reason = reason; t.exit_order_id = oid; t.save()
                redis_client.sadd(self.exiting_trades_set, t.id)
        except Exception as e: logger.error(f"CBD: Exit failed {trade.symbol}: {e}")

    def force_square_off(self, reason):
        for s, t in list(self.open_trades.items()):
            if t.status == "OPEN": self.exit_trade(t, None, reason)

    # ------------------- Reconcile & Stream -------------------
    @transaction.atomic
    def reconcile_trades(self):
        qs = CashBreakdownTrade.objects.select_for_update().filter(account=self.account, status__in=["PENDING_ENTRY", "PENDING_EXIT"])
        if not qs.exists(): return

        for trade in qs:
            order_id = trade.entry_order_id if trade.status == "PENDING_ENTRY" else trade.exit_order_id
            is_entry = trade.status == "PENDING_ENTRY"

            if not order_id:
                trade.status = "FAILED_ENTRY" if is_entry else "FAILED_EXIT"; trade.save()
                if is_entry: 
                    try: redis_client.decr(self.trade_count_key) 
                    except: pass
                redis_client.srem(self.active_entries_set, trade.symbol)
                continue

            try:
                time.sleep(RECONCILE_SLEEP_PER_CALL)
                hist = self.kite.order_history(order_id)
                last = hist[-1] if hist else {}
                status = last.get('status')

                if is_entry:
                    if status == "COMPLETE":
                        filled = int(last.get("filled_quantity", 0))
                        if filled > 0:
                            avg_price = float(last.get("average_price", 0.0))
                            
                            # CRITICAL: Recalculate Target for Short
                            settings = self._get_bear_settings()
                            risk_mult = _parse_ratio_string(settings.get("risk_reward", "1:2"), 2.0)
                            
                            real_risk = trade.stop_level - avg_price
                            new_target = avg_price - (real_risk * risk_mult)
                            
                            trade.status = "OPEN"; trade.entry_price = avg_price
                            trade.quantity = filled; trade.entry_time = timezone.now()
                            trade.target_level = new_target
                            trade.save()
                            self.open_trades[trade.symbol] = trade; self.pending_trades.pop(trade.symbol, None)
                            
                            logger.info(f"CBD: FILLED {trade.symbol} @ {avg_price:.2f}. New Target: {new_target:.2f}")

                    elif status in ("REJECTED", "CANCELLED", "FAILED"):
                        try: redis_client.decr(self.trade_count_key)
                        except: pass
                        trade.status = "FAILED_ENTRY"; trade.save()
                        self.pending_trades.pop(trade.symbol, None)
                        redis_client.srem(self.active_entries_set, trade.symbol)
                else: # Exit
                    if status == "COMPLETE":
                        trade.status = "CLOSED"; trade.exit_price = float(last.get("average_price", 0.0))
                        trade.exit_time = timezone.now()
                        # Short PnL = (Entry - Exit) * Qty
                        trade.pnl = (trade.entry_price - trade.exit_price) * trade.quantity
                        trade.save()
                        redis_client.incrbyfloat(self.daily_pnl_key, trade.pnl)
                        self.open_trades.pop(trade.symbol, None)
                        redis_client.srem(self.exiting_trades_set, trade.id)
                        redis_client.srem(self.active_entries_set, trade.symbol)
                    elif status in ("REJECTED", "CANCELLED", "FAILED"):
                        trade.status = "OPEN"; trade.exit_order_id = None; trade.save()
                        redis_client.srem(self.exiting_trades_set, trade.id)

            except TokenException: self.running = False; break
            except Exception as e: logger.error(f"CBD: Reconcile error {trade.symbol}: {e}")

    def _listen_to_stream(self):
        while self.running:
            try:
                msgs = redis_client.xreadgroup(self.group_name, self.consumer_name, {CANDLE_STREAM_KEY: '>'}, count=50, block=1000)
                if msgs:
                    for _, messages in msgs:
                        ack_ids = []
                        for mid, fields in messages:
                            try:
                                raw = fields.get(b'data') or fields.get('data')
                                if raw: 
                                    if isinstance(raw, bytes): raw = raw.decode()
                                    payload = json.loads(raw)
                                    now = dt.now(IST).time()
                                    if now >= self.account.breakdown_start_time and now <= self.account.breakdown_end_time:
                                        self._process_candle(payload)
                                ack_ids.append(mid)
                            except: pass
                        if ack_ids: redis_client.xack(CANDLE_STREAM_KEY, self.group_name, *ack_ids)
            except: time.sleep(1)

    def run(self):
        self._daily_reset_trades()
        threading.Thread(target=self._listen_to_stream, daemon=True).start()
        while self.running:
            try:
                self._try_enter_pending() 
                self.monitor_trades()     
                if time.time() - self.last_reconcile_time > RECONCILE_SLEEP_PER_CALL:
                    self.reconcile_trades(); self.last_reconcile_time = time.time()
                time.sleep(0.1)
            except: time.sleep(1)

    def stop(self): self.running = False