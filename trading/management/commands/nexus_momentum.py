import json
import redis
import asyncio
import threading
import logging
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

# --- Logging aur Timezone Setup ---
logger = logging.getLogger("Nexus_Momentum")
IST = pytz.timezone("Asia/Kolkata")

class Command(BaseCommand):
    help = 'HFT Momentum Engine (Nexus 2) ko start karta hai (9:15 Range Breakout)'

    def add_arguments(self, parser):
        # Default user_id 1 rakha gaya hai
        parser.add_argument('--user_id', type=int, default=1)

    def handle(self, *args, **options):
        user_id = options['user_id']
        self.stdout.write(self.style.SUCCESS(f'Nexus 2: Momentum Engine User {user_id} ke liye live ho raha hai...'))
        
        try:
            engine = MomentumNexus(user_id=user_id)
            engine.run()
        except Exception as e:
            logger.error(f"Critical Momentum Failure: {e}", exc_info=True)
            self.stdout.write(self.style.ERROR(f'Momentum Engine Crash: {e}'))

class MomentumNexus:
    def __init__(self, user_id):
        self.user_id = user_id
        # Heroku Redis SSL Ready Client
        self.r = get_redis_client()
        
        # 1. DB se account fetch karein
        self.acc = Account.objects.get(user__id=user_id)
        if not self.acc.access_token:
            raise Exception("Access Token nahi mila! Dashboard se login karein.")
            
        self.kite = KiteConnect(api_key=self.acc.api_key)
        self.kite.set_access_token(self.acc.access_token)
        self.kws = KiteTicker(self.acc.api_key, self.acc.access_token)
        
        # --- ZERO-LATENCY RAM STATE ---
        self.stocks = {}        # Universe lookup
        self.open_trades = {}   # Active monitoring
        self.config = {}        # Config cache
        self.banned_set = set()
        self.engine_live = {'mom_bull': False, 'mom_bear': False}
        
        # Initial Setup
        self._load_morning_seeds()
        self._sync_dashboard_params()

    def _load_morning_seeds(self):
        """Instruments aur SMA data ko RAM mein load karna"""
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
                'hi': 0.0, 'lo': 0.0,   # 9:15-9:16 high/low range
                'sma': float(sma_map.get(symbol, 0)),
                'status': 'WAITING',   # WAITING -> EXECUTING -> OPEN -> CLOSED
                'stock_trades': 0,
                'last_ltp': 0.0
            }
        logger.info(f"Nexus 2: {len(self.stocks)} stocks cached.")

    def _sync_dashboard_params(self):
        """Dashboard settings refresh worker logic"""
        for side in ['mom_bull', 'mom_bear']:
            raw_data = self.r.get(f"algo:settings:{side}")
            data = json.loads(raw_data) if raw_data else {}
            
            # RR aur TSL parsing logic
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
        """HOT PATH: Sabse fast momentum logic yahan chalega"""
        now = dt.now(IST)
        now_time = now.time()
        # Heartbeat taaki dashboard 'Live' dikhaye
        self.r.set("algo:data:heartbeat", int(now.timestamp()), ex=15)

        # Timing Windows Setup
        is_range_building = dt_time(9, 15) <= now_time < dt_time(9, 16)
        is_trade_active = now_time >= dt_time(9, 16)

        for tick in ticks:
            token = tick.get('instrument_token')
            stock = self.stocks.get(token)
            if not stock or stock['symbol'] in self.banned_set: continue
            
            ltp = tick['last_price']
            stock['last_ltp'] = ltp

            # 1. OPEN POSITIONS MONITORING (Top Priority)
            if stock['status'] == 'OPEN':
                # Dashboard Manual Exit check
                if self.r.get(f"algo:manual_exit:{stock['symbol']}") == "1":
                    self._fire_hft_order(token, 'EXIT', ltp, "MANUAL_EXIT")
                    self.r.delete(f"algo:manual_exit:{stock['symbol']}")
                    continue
                self._monitor_momentum_exit(token, ltp)
                continue

            # 2. RANGE BUILDING (9:15 - 9:16)
            if is_range_building:
                if stock['hi'] == 0 or ltp > stock['hi']: stock['hi'] = ltp
                if stock['lo'] == 0 or ltp < stock['lo']: stock['lo'] = ltp
                continue

            # 3. BREAKOUT DETECTION (After 9:16)
            if is_trade_active and stock['status'] == 'WAITING':
                # Bull Breakout: Range High + 0.01% Buffer
                if self.engine_live['mom_bull'] and ltp > (stock['hi'] * 1.0001):
                    self._fire_hft_order(token, 'BUY', ltp, "MOM_BULL_HIT")
                
                # Bear Breakdown: Range Low - 0.01% Buffer
                elif self.engine_live['mom_bear'] and ltp < (stock['lo'] * 0.9999):
                    self._fire_hft_order(token, 'SELL', ltp, "MOM_BEAR_HIT")

    def _monitor_momentum_exit(self, token, ltp):
        """RAM based management with 0.02% Exit Buffer"""
        trade = self.open_trades.get(token)
        if not trade: return

        if trade['side'] == 'BUY':
            # Target ya SL with buffer
            if ltp >= (trade['target'] * 1.0002) or ltp <= (trade['sl'] * 0.9998):
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
                self._fire_hft_order(token, 'BUY', ltp, "MOM_BEAR_EXIT")
                return
            profit = trade['entry_px'] - ltp
            if profit >= trade['step']:
                lvls = floor(profit / trade['step'])
                new_sl = trade['entry_px'] - ((lvls - 1) * trade['step'])
                if new_sl < trade['sl']: trade['sl'] = round(new_sl * 20) / 20

    def _fire_hft_order(self, token, side, price, reason):
        """Atomic Protection latching"""
        stock = self.stocks[token]
        stock['status'] = 'EXECUTING'
        asyncio.run_coroutine_threadsafe(self._async_execute(token, side, price, reason), self.loop)

    async def _async_execute(self, token, side, price, reason):
        """Kite API call aur slippage handling logic"""
        stock = self.stocks[token]
        label = 'mom_bull' if (side == 'BUY' or 'BULL' in reason) else 'mom_bear'
        
        try:
            # 1. Global LUA Limit Check (Race condition lock)
            if side in ['BUY', 'SELL']:
                if self.r.eval(LUA_INC_LIMIT, 1, f"hft:count:{self.user_id}:{label}", self.config[label]['max_total']) == -1:
                    logger.warning(f"Limit reached for {label}")
                    stock['status'] = 'WAITING'; return

            # 2. Risk based Quantity Calculation
            risk_per_share = price * self.config[label]['sl_pct']
            qty = max(1, int(floor(self.config[label]['risk_flat'] / risk_per_share))) if risk_per_share > 0 else 1

            # 3. Kite MIS Market Order execution
            oid = self.kite.place_order(
                tradingsymbol=stock['symbol'], exchange='NSE', transaction_type=side,
                quantity=qty, order_type='MARKET', product='MIS', variety='regular'
            )

            # 4. SLIPPAGE SYNC: Actual average price confirm karna
            actual_entry = price
            await asyncio.sleep(0.2)
            hist = self.kite.order_history(oid)
            if hist[-1]['status'] == 'COMPLETE':
                actual_entry = float(hist[-1]['average_price'])
            
            # 5. Position Success Memory Update
            if side in ['BUY', 'SELL']:
                stock['status'] = 'OPEN'
                risk_amt = actual_entry * self.config[label]['sl_pct']
                self.open_trades[token] = {
                    'side': side, 'entry_px': actual_entry, 'qty': qty, 'oid': oid,
                    'sl': actual_entry - risk_amt if side == 'BUY' else actual_entry + risk_amt,
                    'target': actual_entry + (risk_amt * self.config[label]['rr']) if side == 'BUY' else actual_entry - (risk_amt * self.config[label]['rr']),
                    'step': risk_amt * self.config[label]['tsl']
                }
            else:
                stock['status'] = 'CLOSED'
                self.open_trades.pop(token, None)

            logger.info(f"MOM {side} SUCCESS: {stock['symbol']} @ {actual_entry}")

        except Exception as e:
            logger.error(f"Execution Error: {e}")
            stock['status'] = 'WAITING'

    async def settings_poller(self):
        """Dashboard settings sync har 2 sec mein"""
        while True:
            try:
                self._sync_dashboard_params()
                # Database connection refresh for stability
                close_old_connections()
                await asyncio.sleep(2)
            except: pass

    def run(self):
        """Engine start logic"""
        self.loop = asyncio.new_event_loop()
        # Async worker thread start karein
        threading.Thread(target=self.loop.run_forever, daemon=True).start()
        
        # Pollers register karein
        asyncio.run_coroutine_threadsafe(self.settings_poller(), self.loop)
        
        # Ticker connect
        self.kws.on_ticks = self.on_ticks
        self.kws.connect(threaded=True)
        
        # Keep process alive
        while True:
            time.sleep(1)