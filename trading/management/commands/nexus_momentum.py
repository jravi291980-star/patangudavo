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

# --- Logging Setup (Heroku console ke liye) ---
# Saare logs sys.stdout par redirect kiye hain taaki "heroku logs --tail" mein sab dikhe
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Nexus_Momentum")
IST = pytz.timezone("Asia/Kolkata")

class Command(BaseCommand):
    help = 'HFT Momentum Engine (Nexus 2) ko start karta hai (9:15 Range Breakout)'

    def add_arguments(self, parser):
        # Default user_id 1 rakha gaya hai dashboard compatibility ke liye
        parser.add_argument('--user_id', type=int, default=1)

    def handle(self, *args, **options):
        user_id = options['user_id']
        self.stdout.write(self.style.SUCCESS(f'--- Nexus 2: Momentum Engine User {user_id} ke liye chalu ---'))
        
        try:
            engine = MomentumNexus(user_id=user_id)
            engine.run()
        except Exception as e:
            logger.error(f"Critical Momentum Failure: {e}", exc_info=True)
            self.stdout.write(self.style.ERROR(f'Engine Crash: {e}'))

class MomentumNexus:
    def __init__(self, user_id):
        self.user_id = user_id
        # Heroku SSL Ready Redis Client (hft_utils se)
        self.r = get_redis_client()
        
        # 1. Django DB se settings aur credentials load karna
        self.acc = Account.objects.get(user__id=user_id)
        if not self.acc.access_token:
            raise Exception("Access Token missing! Pehle Dashboard se login karein.")
            
        self.kite = KiteConnect(api_key=self.acc.api_key)
        self.kite.set_access_token(self.acc.access_token)
        self.kws = KiteTicker(self.acc.api_key, self.acc.access_token)
        
        # --- ZERO-LATENCY RAM STATE (Fastest Lookups) ---
        self.stocks = {}        # 1700 stocks ka live state
        self.open_trades = {}   # Active positions monitoring
        self.config = {}        # Dashboard parameters cache
        self.banned_set = set()
        self.engine_live = {'mom_bull': False, 'mom_bear': False}
        self.tick_count = 0
        
        # Morning setup
        self._load_morning_seeds()
        self._sync_dashboard_params()

    def _load_morning_seeds(self):
        """Instruments aur SMA data ko RAM mein pre-load karna"""
        logger.info("Nexus 2: RAM seeds build ho rahe hain...")
        
        instr_map = json.loads(self.r.get('instrument_map') or '{}')
        sma_map = self.r.hgetall('algo:fixed_vol_sma')
        universe = set(settings.STOCK_INDEX_MAPPING.keys())
        
        for symbol in universe:
            token_str = next((t for t, d in instr_map.items() if d['symbol'] == symbol), None)
            if not token_str: continue
            
            token = int(token_str)
            self.stocks[token] = {
                'symbol': symbol,
                'hi': 0.0, 'lo': 0.0,   # 9:15-9:16 Range setup
                'sma': float(sma_map.get(symbol, 0)),
                'status': 'WAITING',   # WAITING -> EXECUTING -> OPEN -> CLOSED
                'stock_trades': 0,
                'last_ltp': 0.0
            }
        logger.info(f"Nexus 2: {len(self.stocks)} stocks RAM mein cache ho gaye.")

    def _sync_dashboard_params(self):
        """Dashboard settings aur Ban list ko Redis se RAM mein sync karna"""
        for side in ['mom_bull', 'mom_bear']:
            raw_data = self.r.get(f"algo:settings:{side}")
            data = json.loads(raw_data) if raw_data else {}
            
            # Risk/Reward aur TSL formats handle karna (e.g., '1:2')
            rr_val = 2.0
            if 'risk_reward' in data and ':' in str(data['risk_reward']):
                rr_val = float(data['risk_reward'].split(':')[1])
            
            tsl_val = 1.5
            if 'trailing_sl' in data and ':' in str(data['trailing_sl']):
                tsl_val = float(data['trailing_sl'].split(':')[1])

            self.config[side] = {
                'max_total': int(data.get('max_trades', 3)),
                'sl_pct': float(data.get('stop_loss_pct', 0.5)) / 100.0,
                'rr': rr_val,
                'tsl': tsl_val,
                'risk_flat': float(data.get('risk_per_trade', 2000))
            }
        
        self.banned_set = self.r.smembers("algo:banned_symbols")
        self.engine_live['mom_bull'] = self.r.get("algo:engine:mom_bull:enabled") == "1"
        self.engine_live['mom_bear'] = self.r.get("algo:engine:mom_bear:enabled") == "1"

    def on_ticks(self, ws, ticks):
        """HOT PATH: Sabse fast execution logic (Microseconds matter)"""
        now = dt.now(IST)
        now_time = now.time()
        
        self.tick_count += len(ticks)
        # Dashboard Heartbeat aur Console ALIVE log (Every 1000 ticks)
        if self.tick_count % 1000 == 0:
            self.r.set("algo:mom:heartbeat", int(now.timestamp()), ex=15)
            logger.info(f"MOMENTUM ALIVE: {self.tick_count} ticks processed. Current Sample: {ticks[0]['last_price']}")

        # Timing Windows
        is_range_building = dt_time(9, 15) <= now_time < dt_time(9, 16)
        is_trade_active = now_time >= dt_time(9, 16)

        for tick in ticks:
            token = tick.get('instrument_token')
            stock = self.stocks.get(token)
            if not stock or stock['symbol'] in self.banned_set: continue
            
            ltp = tick['last_price']
            stock['last_ltp'] = ltp

            # --- STEP 1: EXIT MONITORING (Top Priority) ---
            if stock['status'] == 'OPEN':
                # Dashboard Manual Exit signal
                if self.r.get(f"algo:manual_exit:{stock['symbol']}") == "1":
                    logger.info(f"Manual Exit signal detected for {stock['symbol']}")
                    self._fire_hft_order(token, 'EXIT', ltp, "MANUAL_EXIT")
                    self.r.delete(f"algo:manual_exit:{stock['symbol']}")
                    continue
                self._monitor_momentum_exit(token, ltp)
                continue

            # --- STEP 2: RANGE BUILDING (9:15:00 - 9:15:59) ---
            if is_range_building:
                if stock['hi'] == 0 or ltp > stock['hi']: stock['hi'] = ltp
                if stock['lo'] == 0 or ltp < stock['lo']: stock['lo'] = ltp
                continue

            # --- STEP 3: BREAKOUT DETECTION (9:16+) ---
            if is_trade_active and stock['status'] == 'WAITING':
                # Bull Breakout: Range High + 0.01% Buffer
                if self.engine_live['mom_bull'] and ltp > (stock['hi'] * 1.0001):
                    logger.info(f"MOM BULL HIT: {stock['symbol']} @ {ltp}")
                    self._fire_hft_order(token, 'BUY', ltp, "MOM_BULL_HIT")
                
                # Bear Breakdown: Range Low - 0.01% Buffer
                elif self.engine_live['mom_bear'] and ltp < (stock['lo'] * 0.9999):
                    logger.info(f"MOM BEAR HIT: {stock['symbol']} @ {ltp}")
                    self._fire_hft_order(token, 'SELL', ltp, "MOM_BEAR_HIT")

    def _monitor_momentum_exit(self, token, ltp):
        """Position management with 0.02% Buffer logic"""
        trade = self.open_trades.get(token)
        if not trade: return

        if trade['side'] == 'BUY':
            # Exit Conditions with Slippage Buffer
            if ltp >= (trade['target'] * 1.0002) or ltp <= (trade['sl'] * 0.9998):
                logger.info(f"EXIT TRIGGER: {self.stocks[token]['symbol']} Target/SL Hit")
                self._fire_hft_order(token, 'SELL', ltp, "MOM_BULL_EXIT")
                return
            # Step-wise Trailing
            profit = ltp - trade['entry_px']
            if profit >= trade['step']:
                lvls = floor(profit / trade['step'])
                new_sl = trade['entry_px'] + ((lvls - 1) * trade['step'])
                if new_sl > trade['sl']: trade['sl'] = round(new_sl * 20) / 20

        elif trade['side'] == 'SELL':
            if ltp <= (trade['target'] * 0.9998) or ltp >= (trade['sl'] * 1.0002):
                logger.info(f"EXIT TRIGGER: {self.stocks[token]['symbol']} Target/SL Hit")
                self._fire_hft_order(token, 'BUY', ltp, "MOM_BEAR_EXIT")
                return
            profit = trade['entry_px'] - ltp
            if profit >= trade['step']:
                lvls = floor(profit / trade['step'])
                new_sl = trade['entry_px'] - ((lvls - 1) * trade['step'])
                if new_sl < trade['sl']: trade['sl'] = round(new_sl * 20) / 20

    def _fire_hft_order(self, token, side, price, reason):
        """Atomic latching and async execution dispatch"""
        stock = self.stocks[token]
        # Memory latching to prevent ghost triggers
        stock['status'] = 'EXECUTING'
        asyncio.run_coroutine_threadsafe(self._async_execute(token, side, price, reason), self.loop)

    async def _async_execute(self, token, side, price, reason):
        """Kite API call and price synchronization logic"""
        stock = self.stocks[token]
        label = 'mom_bull' if (side == 'BUY' or 'BULL' in reason) else 'mom_bear'
        
        try:
            # 1. Global Trade Counter (Race condition safety)
            if side in ['BUY', 'SELL']:
                if self.r.eval(LUA_INC_LIMIT, 1, f"hft:count:{self.user_id}:{label}", self.config[label]['max_total']) == -1:
                    logger.warning(f"Engine Limit Reached for {label}")
                    stock['status'] = 'WAITING'; return

            # 2. Risk Tier based Quantity
            risk_per_share = price * self.config[label]['sl_pct']
            qty = max(1, int(floor(self.config[label]['risk_flat'] / risk_per_share))) if risk_per_share > 0 else 1

            # 3. Kite Market Order
            oid = self.kite.place_order(
                tradingsymbol=stock['symbol'], exchange='NSE', transaction_type=side,
                quantity=qty, order_type='MARKET', product='MIS', variety='regular'
            )

            # 4. SLIPPAGE SYNC: Fetching actual completion price
            actual_px = price
            await asyncio.sleep(0.2)
            try:
                hist = self.kite.order_history(oid)
                if hist[-1]['status'] == 'COMPLETE':
                    actual_px = float(hist[-1]['average_price'])
            except: pass # Tick price fallback
            
            # 5. Position Memory Update (Target/SL are based on ACTUAL demat price)
            if side in ['BUY', 'SELL']:
                stock['status'] = 'OPEN'
                risk_amt = actual_px * self.config[label]['sl_pct']
                self.open_trades[token] = {
                    'side': side, 'entry_px': actual_px, 'qty': qty, 'oid': oid,
                    'sl': actual_px - risk_amt if side == 'BUY' else actual_px + risk_amt,
                    'target': actual_px + (risk_amt * self.config[label]['rr']) if side == 'BUY' else actual_px - (risk_amt * self.config[label]['rr']),
                    'step': risk_amt * self.config[label]['tsl']
                }
                logger.info(f"MOMENTUM SUCCESS: {stock['symbol']} {side} @ {actual_px}")
            else:
                stock['status'] = 'CLOSED'
                self.open_trades.pop(token, None)
                logger.info(f"MOMENTUM EXIT: {stock['symbol']} Closed @ {actual_px}")

        except Exception as e:
            logger.error(f"Kite Order Execution Error: {e}")
            stock['status'] = 'WAITING'

    async def settings_poller(self):
        """Dashboard settings refresh worker"""
        while True:
            try:
                self._sync_dashboard_params()
                # Heroku Postgres safety (close idle connections)
                close_old_connections()
                await asyncio.sleep(2)
            except: pass

    def run(self):
        """Main execution entry point"""
        self.loop = asyncio.new_event_loop()
        # Worker thread for background async tasks
        threading.Thread(target=self.loop.run_forever, daemon=True).start()
        
        # Start Workers
        asyncio.run_coroutine_threadsafe(self.settings_poller(), self.loop)
        
        # Kite Connection
        self.kws.on_ticks = self.on_ticks
        self.kws.connect(threaded=True)
        
        # Main process protection loop
        while True:
            time.sleep(1)