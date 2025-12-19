import json
import redis
import asyncio
import threading
import logging
import sys
import time
from datetime import datetime as dt, time as dt_time
from math import floor
import pytz

from django.core.management.base import BaseCommand
from django.db import close_old_connections
from django.conf import settings
from kiteconnect import KiteTicker, KiteConnect

from trading.models import Account, MomentumBullTrade, MomentumBearTrade
from trading.hft_utils import get_redis_client, LUA_INC_LIMIT

# --- Heroku Console Logging Setup ---
# Saare logs sys.stdout par redirect hain taaki Heroku Dashboard aur CLI par real-time dikhein
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Nexus_Momentum")
IST = pytz.timezone("Asia/Kolkata")

class Command(BaseCommand):
    help = 'Nexus 2: Momentum HFT Engine (9:15 Range Breakout) with Verbose Monitoring'

    def add_arguments(self, parser):
        parser.add_argument('--user_id', type=int, default=1)

    def handle(self, *args, **options):
        user_id = options['user_id']
        self.stdout.write(self.style.SUCCESS(f'--- NEXUS 2: MOMENTUM ENGINE STARTING (User ID: {user_id}) ---'))
        
        try:
            engine = MomentumNexus(user_id=user_id)
            engine.run()
        except Exception as e:
            logger.error(f"MOMENTUM ENGINE CRITICAL CRASH: {e}", exc_info=True)
            self.stdout.write(self.style.ERROR(f'Engine Crash: {e}'))

class MomentumNexus:
    def __init__(self, user_id):
        self.user_id = user_id
        # Safe Redis Client (Heroku SSL Compatibility Integrated)
        self.r = get_redis_client()
        
        # 1. Database se account details fetch karna
        self.acc = Account.objects.get(user__id=user_id)
        if not self.acc.access_token:
            raise Exception("Access Token missing! Pehle Dashboard se login kijiye.")
            
        self.kite = KiteConnect(api_key=self.acc.api_key)
        self.kite.set_access_token(self.acc.access_token)
        self.kws = KiteTicker(self.acc.api_key, self.acc.access_token)
        
        # --- ZERO-LATENCY RAM STATE ---
        self.stocks = {}        # 1700 Stocks cache
        self.open_trades = {}   # Active positions monitoring
        self.config = {}        # Settings cache
        self.banned_set = set()
        self.engine_live = {'mom_bull': False, 'mom_bear': False}
        
        # Monitoring Metrics
        self.tick_count = 0
        self.last_summary_time = time.time()
        self.range_log_timer = time.time()
        
        # Initial Seeds aur Config Sync
        self._load_morning_seeds()
        self._sync_dashboard_params()

    def _load_morning_seeds(self):
        """SMA data aur Instruments ko Redis se RAM mein load karna"""
        logger.info("Initializing RAM Cache: Loading Momentum Seeds...")
        
        instr_map = json.loads(self.r.get('instrument_map') or '{}')
        sma_map = self.r.hgetall('algo:fixed_vol_sma')
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
        logger.info(f"CACHE READY: {len(self.stocks)} stocks cached for Momentum.")

    def _sync_dashboard_params(self):
        """Dashboard settings ko RAM mein refresh karna (har 2 sec)"""
        for side in ['mom_bull', 'mom_bear']:
            raw_data = self.r.get(f"algo:settings:{side}")
            data = json.loads(raw_data) if raw_data else {}
            
            # RR/TSL Parsing
            rr = float(data.get('risk_reward', '1:2').split(':')[1]) if ':' in str(data.get('risk_reward')) else 2.0
            tsl = float(data.get('trailing_sl', '1:1.5').split(':')[1]) if ':' in str(data.get('trailing_sl')) else 1.5

            self.config[side] = {
                'max_total': int(data.get('max_trades', 3)),
                'sl_pct': float(data.get('stop_loss_pct', 0.5)) / 100.0,
                'rr': rr, 'tsl': tsl,
                'risk_flat': float(data.get('risk_per_trade', 2000))
            }
        
        self.banned_set = self.r.smembers("algo:banned_symbols")
        self.engine_live['mom_bull'] = self.r.get("algo:engine:mom_bull:enabled") == "1"
        self.engine_live['mom_bear'] = self.r.get("algo:engine:mom_bear:enabled") == "1"

    def on_connect(self, ws, response):
        """WebSocket connect hone par confirmation aur subscription"""
        logger.info("WebSocket Info: Successfully connected to Kite Ticker.")
        tokens = list(self.stocks.keys())
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)
        logger.info(f"SUBSCRIPTION: {len(tokens)} tokens subscribed for Momentum Engine.")

    def on_ticks(self, ws, ticks):
        """HOT PATH: High Frequency Momentum logic"""
        now = dt.now(IST)
        now_time = now.time()
        
        self.tick_count += len(ticks)
        
        # 1. ALIVE Summary: Har 10 second mein monitoring state log karna
        if time.time() - self.last_summary_time > 10:
            self.r.set("algo:mom:heartbeat", int(now.timestamp()), ex=20)
            logger.info(f"ALIVE Summary: Processed {self.tick_count} ticks | Active Positions: {len(self.open_trades)} | Heartbeat Updated")
            self.last_summary_time = time.time()
            self._sync_dashboard_params()

        # Timing Windows
        is_range_building = dt_time(9, 15) <= now_time < dt_time(9, 16)
        is_trade_active = now_time >= dt_time(9, 16)

        for tick in ticks:
            token = tick.get('instrument_token')
            stock = self.stocks.get(token)
            if not stock or stock['symbol'] in self.banned_set: continue

            ltp = tick['last_price']
            stock['last_ltp'] = ltp

            # --- STEP 1: EXIT MONITORING ---
            if stock['status'] == 'OPEN':
                # Manual dashboard exit
                if self.r.get(f"algo:manual_exit:{stock['symbol']}") == "1":
                    logger.info(f"MANUAL EXIT: Closing {stock['symbol']} due to dashboard signal.")
                    self._fire_hft_order(token, 'EXIT', ltp, "MANUAL_EXIT")
                    self.r.delete(f"algo:manual_exit:{stock['symbol']}")
                    continue
                self._monitor_momentum_exit(token, ltp)
                continue

            # --- STEP 2: RANGE BUILDING (9:15 - 9:16) ---
            if is_range_building:
                if stock['hi'] == 0 or ltp > stock['hi']: stock['hi'] = ltp
                if stock['lo'] == 0 or ltp < stock['lo']: stock['lo'] = ltp
                
                # Periodic log for range activity
                if time.time() - self.range_log_timer > 30:
                    logger.info(f"RANGE BUILDING: {stock['symbol']} | Hi: {stock['hi']} | Lo: {stock['lo']}")
                    self.range_log_timer = time.time()
                continue

            # --- STEP 3: MOMENTUM TRIGGER (9:16+) ---
            if is_trade_active and stock['status'] == 'WAITING':
                # Bull Breakout: Range High + 0.01% Buffer
                if self.engine_live['mom_bull'] and ltp > (stock['hi'] * 1.0001):
                    # SIGNAL DETECTED: Momentum candidate found
                    logger.info(f"SIGNAL DETECTED: {stock['symbol']} Bull Momentum @ {ltp} | Target Range High: {stock['hi']}")
                    self._fire_hft_order(token, 'BUY', ltp, "MOM_BULL_HIT")
                
                # Bear Breakdown: Range Low - 0.01% Buffer
                elif self.engine_live['mom_bear'] and ltp < (stock['lo'] * 0.9999):
                    # SIGNAL DETECTED: Momentum candidate found
                    logger.info(f"SIGNAL DETECTED: {stock['symbol']} Bear Momentum @ {ltp} | Target Range Low: {stock['lo']}")
                    self._fire_hft_order(token, 'SELL', ltp, "MOM_BEAR_HIT")

    def _monitor_momentum_exit(self, token, ltp):
        """RAM based management with Trailing SL and 0.02% Exit Buffer"""
        trade = self.open_trades.get(token)
        if not trade: return

        if trade['side'] == 'BUY':
            if ltp >= (trade['target'] * 1.0002) or ltp <= (trade['sl'] * 0.9998):
                logger.info(f"POSITION EXIT: {self.stocks[token]['symbol']} Bull Exit @ {ltp}")
                self._fire_hft_order(token, 'SELL', ltp, "MOM_BULL_EXIT")
                return
            # Step Trailing
            profit = ltp - trade['entry_px']
            if profit >= trade['step']:
                lvls = floor(profit / trade['step'])
                new_sl = trade['entry_px'] + ((lvls - 1) * trade['step'])
                if new_sl > trade['sl']: trade['sl'] = round(new_sl * 20) / 20

        elif trade['side'] == 'SELL':
            if ltp <= (trade['target'] * 0.9998) or ltp >= (trade['sl'] * 1.0002):
                logger.info(f"POSITION EXIT: {self.stocks[token]['symbol']} Bear Exit @ {ltp}")
                self._fire_hft_order(token, 'BUY', ltp, "MOM_BEAR_EXIT")
                return
            profit = trade['entry_px'] - ltp
            if profit >= trade['step']:
                lvls = floor(profit / trade['step'])
                new_sl = trade['entry_px'] - ((lvls - 1) * trade['step'])
                if new_sl < trade['sl']: trade['sl'] = round(new_sl * 20) / 20

    def _fire_hft_order(self, token, side, price, reason):
        """Atomic Handoff to async execution engine"""
        stock = self.stocks[token]
        stock['status'] = 'EXECUTING'
        asyncio.run_coroutine_threadsafe(self._async_execute(token, side, price, reason), self.loop)

    async def _async_execute(self, token, side, price, reason):
        """API interaction and Slippage Price Sync"""
        stock = self.stocks[token]
        label = 'mom_bull' if (side == 'BUY' or 'BULL' in reason) else 'mom_bear'
        
        try:
            # 1. Atomic Counter Check (Race condition lock)
            if side in ['BUY', 'SELL']:
                if self.r.eval(LUA_INC_LIMIT, 1, f"hft:count:{self.user_id}:{label}", self.config[label]['max_total']) == -1:
                    logger.warning(f"LIMIT REACHED: {label} momentum trades full for today.")
                    stock['status'] = 'WAITING'; return

            # 2. Risk based Quantity
            risk_px = price * self.config[label]['sl_pct']
            qty = max(1, int(floor(self.config[label]['risk_flat'] / risk_px))) if risk_px > 0 else 1

            # 3. Kite MIS Market Order
            oid = self.kite.place_order(
                tradingsymbol=stock['symbol'], exchange='NSE', transaction_type=side,
                quantity=qty, order_type='MARKET', product='MIS', variety='regular'
            )
            logger.info(f"ORDER PLACED: {stock['symbol']} {side} | Qty: {qty} | OID: {oid}")

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
                risk_amt = actual_px * self.config[label]['sl_pct']
                self.open_trades[token] = {
                    'side': side, 'entry_px': actual_px, 'qty': qty, 'oid': oid,
                    'sl': actual_px - risk_amt if side == 'BUY' else actual_px + risk_amt,
                    'target': actual_px + (risk_amt * self.config[label]['rr']) if side == 'BUY' else actual_px - (risk_amt * self.config[label]['rr']),
                    'step': risk_amt * self.config[label]['tsl']
                }
                logger.info(f"SUCCESS: {stock['symbol']} Momentum Opened @ {actual_px}")
            else:
                stock['status'] = 'CLOSED'
                self.open_trades.pop(token, None)
                logger.info(f"SUCCESS: {stock['symbol']} Momentum Position Closed @ {actual_px}")

        except Exception as e:
            logger.error(f"KITE API EXECUTION ERROR: {e}")
            stock['status'] = 'WAITING'

    async def settings_poller(self):
        """Dashboard settings sync worker"""
        while True:
            try:
                self._sync_dashboard_params()
                close_old_connections() # Heroku DB security
                await asyncio.sleep(2)
            except: pass

    def run(self):
        """Engine startup logic"""
        self.loop = asyncio.new_event_loop()
        threading.Thread(target=self.loop.run_forever, daemon=True).start()
        
        # Start Background workers
        asyncio.run_coroutine_threadsafe(self.settings_poller(), self.loop)
        
        # WebSocket Connect
        self.kws.on_connect = self.on_connect
        self.kws.on_ticks = self.on_ticks
        self.kws.connect(threaded=True)
        
        while True:
            time.sleep(1)