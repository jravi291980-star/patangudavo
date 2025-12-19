import json
import redis
import asyncio
import threading
import logging
import sys
import time
from datetime import datetime as dt
from math import floor
import pytz

from django.core.management.base import BaseCommand
from django.db import close_old_connections
from django.conf import settings
from kiteconnect import KiteTicker, KiteConnect

# Models aur Utils import
from trading.models import Account, CashBreakoutTrade, CashBreakdownTrade
from trading.hft_utils import get_redis_client, LUA_INC_LIMIT

# --- Heroku Console Logging Setup ---
# Isse saare logs "heroku logs --tail" mein realtime dikhenge
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Nexus_Breakout")
IST = pytz.timezone("Asia/Kolkata")

class Command(BaseCommand):
    help = 'Nexus 1: Cash Breakout HFT Engine with Verbose Live Scanner'

    def add_arguments(self, parser):
        # Default user_id 1 rakha gaya hai
        parser.add_argument('--user_id', type=int, default=1)

    def handle(self, *args, **options):
        user_id = options['user_id']
        self.stdout.write(self.style.SUCCESS(f'--- NEXUS 1: ENGINE STARTING (User ID: {user_id}) ---'))
        
        try:
            # Engine initialize aur run
            engine = BreakoutNexus(user_id=user_id)
            engine.run()
        except Exception as e:
            logger.error(f"ENGINE CRITICAL CRASH: {e}", exc_info=True)
            self.stdout.write(self.style.ERROR(f'Engine Crash: {e}'))

class BreakoutNexus:
    def __init__(self, user_id):
        self.user_id = user_id
        # Safe Redis Client (Heroku SSL Compatibility Integrated)
        self.r = get_redis_client()
        
        # 1. Django DB se account details aur access token fetch karna
        self.acc = Account.objects.get(user__id=user_id)
        if not self.acc.access_token:
            raise Exception("Access Token missing! Pehle Dashboard se login kijiye.")
            
        # Zerodha API connections
        self.kite = KiteConnect(api_key=self.acc.api_key)
        self.kite.set_access_token(self.acc.access_token)
        self.kws = KiteTicker(self.acc.api_key, self.acc.access_token)
        
        # --- ZERO-LATENCY RAM STATE (Pure Python Dicts) ---
        self.stocks = {}        # 1700 Stocks data (OHLC + Status)
        self.open_trades = {}   # Active positions monitoring {token: data}
        self.config = {}        # Dashboard parameters cache
        self.banned_set = set()
        self.engine_live = {'bull': False, 'bear': False}
        
        # --- Metrics & Visual Logging State ---
        self.tick_count = 0
        self.candles_built_this_min = 0
        self.last_min_logged = -1
        self.last_summary_time = time.time()
        
        # Initial data load
        self._load_morning_cache()
        self._sync_dashboard_params()

    def _load_morning_cache(self):
        """OHLC aur SMA data ko Redis se RAM mein load karna (O(1) Lookup)"""
        logger.info("Initializing RAM Cache: Building seeds for 1700+ stocks...")
        
        instr_map = json.loads(self.r.get('instrument_map') or '{}')
        pdl_map = self.r.hgetall('prev_day_ohlc')
        sma_map = self.r.hgetall('algo:fixed_vol_sma')
        universe = set(settings.STOCK_INDEX_MAPPING.keys())
        
        for symbol in universe:
            token_str = next((t for t, d in instr_map.items() if d['symbol'] == symbol), None)
            if not token_str: continue
            
            token = int(token_str)
            pdl_raw = pdl_map.get(symbol)
            if not pdl_raw: continue
            pdl = json.loads(pdl_raw)

            # Har stock ka initial state build karna
            self.stocks[token] = {
                'symbol': symbol,
                'prev_high': float(pdl['high']),
                'prev_low': float(pdl['low']),
                'sma': float(sma_map.get(symbol, 0)),
                'status': 'PENDING',    # PENDING -> TRIGGER_WATCH -> EXECUTING -> OPEN
                'trigger_px': 0.0,
                'trigger_at': None,
                'side_latch': None,
                'stop_base': 0.0,
                'stock_trades': 0,
                'candle': None,
                'last_vol': 0,
                'last_ltp': 0
            }
        logger.info(f"CACHE READY: {len(self.stocks)} stocks monitor ho rahe hain.")

    def _sync_dashboard_params(self):
        """Settings ko RAM mein refresh karna (har 1 minute ya dashboard update par)"""
        for side in ['bull', 'bear']:
            raw_data = self.r.get(f"algo:settings:{side}")
            data = json.loads(raw_data) if raw_data else {}
            
            # RR/TSL parsing logic (1:2 format handle karne ke liye)
            rr = float(data.get('risk_reward', '1:2').split(':')[1]) if ':' in str(data.get('risk_reward', '')) else 2.0
            tsl = float(data.get('trailing_sl', '1:1.5').split(':')[1]) if ':' in str(data.get('trailing_sl', '')) else 1.5

            self.config[side] = {
                'max_total': int(data.get('total_trades', 5)),
                'max_per_stock': int(data.get('trades_per_stock', 2)),
                'rr': rr, 'tsl': tsl,
                'vol_matrix': data.get('volume_criteria', []), # 10 Levels integrated
                'risk_tiers': [
                    float(data.get('risk_trade_1', 2000)),
                    float(data.get('risk_trade_2', 1500)),
                    float(data.get('risk_trade_3', 1000))
                ]
            }
        
        # Banned symbols aur engine status update
        self.banned_set = self.r.smembers("algo:banned_symbols")
        self.engine_live['bull'] = self.r.get("algo:engine:bull:enabled") == "1"
        self.engine_live['bear'] = self.r.get("algo:engine:bear:enabled") == "1"

    def on_connect(self, ws, response):
        """WebSocket connect hone par confirmation log aur subscription"""
        logger.info("WebSocket Info: Successfully connected to Kite Ticker.")
        tokens = list(self.stocks.keys())
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)
        logger.info(f"Subscription: {len(tokens)} stocks full mode mein stream ho rahe hain.")

    def on_ticks(self, ws, ticks):
        """HOT PATH: High Frequency logic execution (Speed optimization)"""
        now = dt.now(IST)
        bucket = now.replace(second=0, microsecond=0)
        self.tick_count += len(ticks)
        
        # --- 1. HEARTBEAT & ALIVE SUMMARY (Har 10 sec) ---
        if time.time() - self.last_summary_time > 10:
            self.r.set("algo:data:heartbeat", int(now.timestamp()), ex=20)
            logger.info(f"ALIVE Summary: Ticks: {self.tick_count} | Active Trades: {len(self.open_trades)}")
            self.last_summary_time = time.time()
            # Sampling: Top active stock dikhao confirm karne ke liye
            sample = list(self.stocks.values())[0]
            logger.info(f"SAMPLE DATA: {sample['symbol']} LTP: {sample['last_ltp']} | Status: {sample['status']}")

        # --- 2. MINUTE ROLLOVER LOG ---
        if now.minute != self.last_min_logged:
            if self.last_min_logged != -1:
                logger.info(f"=== MINUTE SYNC ({bucket.strftime('%H:%M')}) | Candles built: {self.candles_built_this_min} ===")
            self.last_min_logged = now.minute
            self.candles_built_this_min = 0
            self._sync_dashboard_params()

        for tick in ticks:
            token = tick.get('instrument_token')
            stock = self.stocks.get(token)
            if not stock or stock['symbol'] in self.banned_set: continue

            ltp = tick['last_price']
            vol = tick.get('volume_traded', 0)
            stock['last_ltp'] = ltp

            # --- STEP 1: EXIT MONITORING (Priority 1) ---
            if stock['status'] == 'OPEN':
                # Manual dashboard exit check
                if self.r.get(f"algo:manual_exit:{stock['symbol']}") == "1":
                    logger.info(f"MANUAL EXIT triggered for {stock['symbol']}")
                    self._fire_hft_order(token, 'EXIT', ltp, 0, "MANUAL_EXIT")
                    self.r.delete(f"algo:manual_exit:{stock['symbol']}")
                    continue
                self._manage_exit_and_tsl(token, ltp)
                continue

            # --- STEP 2: TRIGGER WATCH (6-min Window logic) ---
            if stock['status'] == 'TRIGGER_WATCH':
                # Timer check: 6 minute se zyada ho gaya toh reset
                if (now - stock['trigger_at']).total_seconds() > 360:
                    logger.info(f"TIMER EXPIRED: {stock['symbol']} reset to PENDING.")
                    stock['status'] = 'PENDING'
                    continue
                
                # Signal break check (0.01% Buffer already included in trigger_px)
                if stock['side_latch'] == 'BULL' and ltp > stock['trigger_px']:
                    logger.info(f"!!! BREAKOUT HIT !!!: {stock['symbol']} Buying @ {ltp}")
                    self._fire_hft_order(token, 'BUY', ltp, stock['stop_base'], "BULL_BREAKOUT")
                elif stock['side_latch'] == 'BEAR' and ltp < stock['trigger_px']:
                    logger.info(f"!!! BREAKDOWN HIT !!!: {stock['symbol']} Selling @ {ltp}")
                    self._fire_hft_order(token, 'SELL', ltp, stock['stop_base'], "BEAR_BREAKDOWN")
                continue

            # --- STEP 3: CANDLE AGGREGATION & LIVE SCANNER ---
            if stock['candle'] and stock['candle']['bucket'] != bucket:
                # CANDLE DONE log logic
                self.candles_built_this_min += 1
                self._analyze_candle_logic(token, stock['candle'])
                stock['candle'] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': 0}
            elif not stock['candle']:
                stock['candle'] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': 0}
            else:
                c = stock['candle']
                c['high'] = max(c['high'], ltp); c['low'] = min(c['low'], ltp); c['close'] = ltp
                if stock['last_vol'] > 0: c['volume'] += max(0, vol - stock['last_vol'])
            
            stock['last_vol'] = vol

    def _analyze_candle_logic(self, token, candle):
        """Har completed candle par strategy scanning rules"""
        stock = self.stocks[token]
        
        # High Volume Spike Alert log
        if candle['volume'] > stock['sma'] * 3:
            logger.info(f"HIGH VOL SPIKE: {stock['symbol']} | C: {candle['close']} | V: {int(candle['volume'])} (SMA: {int(stock['sma'])})")

        if stock['status'] != 'PENDING': return

        # BULL Crossing check: Price breakout candidate?
        if self.engine_live['bull'] and candle['open'] < stock['prev_high'] < candle['close']:
            is_qualified = self._is_vol_qualified(token, candle, 'bull')
            logger.info(f"SCANNER: {stock['symbol']} crossed PrevHigh {stock['prev_high']} | Matrix-OK: {is_qualified}")
            
            if is_qualified:
                # SIGNAL DETECTED log
                logger.info(f"SIGNAL DETECTED: {stock['symbol']} Bull Breakout Monitoring @ {candle['high']}")
                stock['status'] = 'TRIGGER_WATCH'; stock['side_latch'] = 'BULL'
                stock['trigger_px'] = candle['high'] * 1.0001 # 0.01% Entry Buffer
                stock['trigger_at'] = dt.now(IST)
                stock['stop_base'] = stock['prev_high'] if (candle['high'] - candle['low'])/candle['close'] > 0.007 else candle['low']

        # BEAR Crossing check
        elif self.engine_live['bear'] and candle['open'] > stock['prev_low'] > candle['close']:
            is_qualified = self._is_vol_qualified(token, candle, 'bear')
            logger.info(f"SCANNER: {stock['symbol']} crossed PrevLow {stock['prev_low']} | Matrix-OK: {is_qualified}")
            
            if is_qualified:
                logger.info(f"SIGNAL DETECTED: {stock['symbol']} Bear Breakdown Monitoring @ {candle['low']}")
                stock['status'] = 'TRIGGER_WATCH'; stock['side_latch'] = 'BEAR'
                stock['trigger_px'] = candle['low'] * 0.9999 # 0.01% Entry Buffer
                stock['trigger_at'] = dt.now(IST)
                stock['stop_base'] = stock['prev_low'] if (candle['high'] - candle['low'])/candle['close'] > 0.007 else candle['high']

    def _is_vol_qualified(self, token, candle, side):
        """10-Level Volume SMA Matrix checking logic"""
        stock = self.stocks[token]
        matrix = self.config[side]['vol_matrix']
        if not matrix: return False
        
        c_vol = candle['volume']; c_close = candle['close']; s_sma = stock['sma']
        for level in matrix:
            try:
                # SMA Multiplier aur Price in Crores check
                if s_sma < float(level.get('min_sma_avg', 0)): continue 
                if c_vol < (s_sma * float(level.get('sma_multiplier', 1))): continue
                if (c_vol * c_close) / 10000000.0 >= float(level.get('min_vol_price_cr', 0)):
                    return True
            except: continue
        return False

    def _manage_exit_and_tsl(self, token, ltp):
        """Step-wise Trailing monitoring in RAM with 0.02% Exit Buffer"""
        trade = self.open_trades.get(token)
        if not trade: return

        if trade['side'] == 'BUY':
            # 0.02% Buffer included in Exit triggers
            if ltp >= (trade['target'] * 1.0002) or ltp <= (trade['sl'] * 0.9998):
                logger.info(f"EXIT TRIGGER: {self.stocks[token]['symbol']} Bull Closed @ {ltp}")
                self._fire_hft_order(token, 'SELL', ltp, 0, "BULL_EXIT")
                return
            # Trailing Profit step detection
            profit = ltp - trade['entry_px']
            if profit >= trade['step']:
                lvls = floor(profit / trade['step'])
                new_sl = trade['entry_px'] + ((lvls - 1) * trade['step'])
                if new_sl > trade['sl']: trade['sl'] = round(new_sl * 20) / 20

        elif trade['side'] == 'SELL':
            if ltp <= (trade['target'] * 0.9998) or ltp >= (trade['sl'] * 1.0002):
                logger.info(f"EXIT TRIGGER: {self.stocks[token]['symbol']} Bear Closed @ {ltp}")
                self._fire_hft_order(token, 'BUY', ltp, 0, "BEAR_EXIT")
                return
            profit = trade['entry_px'] - ltp
            if profit >= trade['step']:
                lvls = floor(profit / trade['step'])
                new_sl = trade['entry_px'] - ((lvls - 1) * trade['step'])
                if new_sl < trade['sl']: trade['sl'] = round(new_sl * 20) / 20

    def _fire_hft_order(self, token, side, price, stop_base, reason):
        """Atomic Handoff: Execution thread-safe queue mein dalna"""
        stock = self.stocks[token]
        stock['status'] = 'EXECUTING' # Ghost trigger protection
        asyncio.run_coroutine_threadsafe(self._async_kite_execute(token, side, price, stop_base, reason), self.loop)

    async def _async_kite_execute(self, token, side, price, stop_base, reason):
        """Kite API interact karega aur Slippage sync karega"""
        stock = self.stocks[token]
        config_label = 'bull' if (side == 'BUY' or 'BULL' in reason) else 'bear'
    
        try:
            # 1. LUA Limit check (Atomic Counter in Redis)
            if side in ['BUY', 'SELL']:
                if self.r.eval(LUA_INC_LIMIT, 1, f"hft:count:{self.user_id}:{config_label}", self.config[config_label]['max_total']) == -1:
                    logger.warning(f"LIMIT BLOCKED: {config_label} limit reached for user {self.user_id}")
                    stock['status'] = 'PENDING'; return

            # 2. Risk Calculation based on tiers
            risk_tier = self.config[config_label]['risk_tiers'][min(stock['stock_trades'], 2)]
            risk_amt = abs(price - stop_base)
            qty = max(1, int(floor(risk_tier / risk_amt))) if risk_amt > 0 else 1

            # 3. Market MIS Order Placement
            oid = self.kite.place_order(
                tradingsymbol=stock['symbol'], exchange='NSE', transaction_type=side,
                quantity=qty, order_type='MARKET', product='MIS', variety='regular'
            )
            logger.info(f"API ORDER: {stock['symbol']} {side} Fire! OID: {oid}")

            # 4. SLIPPAGE SYNC: Actual Demat Average Price fetch karna
            actual_px = price
            await asyncio.sleep(0.2)
            try:
                hist = self.kite.order_history(oid)
                if hist[-1]['status'] == 'COMPLETE':
                    actual_px = float(hist[-1]['average_price'])
            except: pass 
            
            # 5. Position Memory management
            if side in ['BUY', 'SELL']:
                stock['status'] = 'OPEN'
                stock['stock_trades'] += 1
                risk = abs(actual_px - stop_base)
                self.open_trades[token] = {
                    'side': side, 'entry_px': actual_px, 'sl': stop_base, 'qty': qty,
                    'target': actual_px + (risk * self.config[config_label]['rr']) if side == 'BUY' else actual_px - (risk * self.config[label]['rr']),
                    'step': risk * self.config[config_label]['tsl'], 'oid': oid
                }
                logger.info(f"HFT SUCCESS: {stock['symbol']} Position Active @ {actual_px}")
            else:
                stock['status'] = 'CLOSED'
                self.open_trades.pop(token, None)
                logger.info(f"HFT SUCCESS: {stock['symbol']} Position Closed @ {actual_px}")

        except Exception as e:
            logger.error(f"KITE API ERROR: {e}")
            stock['status'] = 'PENDING'

    async def settings_poller(self):
        """Dashboard parameters ko refresh karne wala background worker"""
        while True:
            try:
                self._sync_dashboard_params()
                # Database connection stability for Heroku
                close_old_connections()
                await asyncio.sleep(5)
            except: pass

    def run(self):
        """Engine startup execution"""
        self.loop = asyncio.new_event_loop()
        # Background worker thread chalu karein
        threading.Thread(target=self.loop.run_forever, daemon=True).start()
        
        # Pollers register karein
        asyncio.run_coroutine_threadsafe(self.settings_poller(), self.loop)
        
        # Kite Ticker Connect
        self.kws.on_connect = self.on_connect
        self.kws.on_ticks = self.on_ticks
        self.kws.connect(threaded=True)
        
        # Main process alive rakhne ke liye infinite wait
        while True:
            time.sleep(1)