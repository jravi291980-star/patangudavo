import json
import redis
import asyncio
import threading
import logging
import time
from datetime import datetime as dt
from math import floor
import pytz

from django.core.management.base import BaseCommand
from django.db import close_old_connections
from django.conf import settings
from kiteconnect import KiteTicker, KiteConnect

from trading.models import Account, CashBreakoutTrade, CashBreakdownTrade
from trading.hft_utils import get_redis_client, LUA_INC_LIMIT

# --- Logging Setup ---
logger = logging.getLogger("Nexus_Breakout")
IST = pytz.timezone("Asia/Kolkata")

class Command(BaseCommand):
    help = 'HFT Cash Breakout Engine (Nexus 1) ko start karta hai'

    def add_arguments(self, parser):
        # Default user_id 1 rakha gaya hai
        parser.add_argument('--user_id', type=int, default=1)

    def handle(self, *args, **options):
        user_id = options['user_id']
        self.stdout.write(self.style.SUCCESS(f'Nexus 1: Breakout Engine User {user_id} ke liye chalu ho raha hai...'))
        
        try:
            engine = BreakoutNexus(user_id=user_id)
            engine.run()
        except Exception as e:
            logger.error(f"Critical Engine Failure: {e}", exc_info=True)
            self.stdout.write(self.style.ERROR(f'Engine Crash ho gaya: {e}'))

class BreakoutNexus:
    def __init__(self, user_id):
        self.user_id = user_id
        # Heroku SSL ready Redis client
        self.r = get_redis_client()
        
        # 1. DB se Account details load karein
        self.acc = Account.objects.get(user__id=user_id)
        if not self.acc.access_token:
            raise Exception("Access Token nahi mila! Dashboard se login karein.")
            
        self.kite = KiteConnect(api_key=self.acc.api_key)
        self.kite.set_access_token(self.acc.access_token)
        self.kws = KiteTicker(self.acc.api_key, self.acc.access_token)
        
        # --- ZERO-LATENCY RAM STATE ---
        self.stocks = {}        # 1700 stocks ka data
        self.open_trades = {}   # Active positions {token: data}
        self.config = {}        # Dashboard parameters
        self.banned_set = set()
        self.engine_live = {'bull': False, 'bear': False}
        
        # Morning Data load karein
        self._load_morning_cache()
        # Dashboard parameters pehli baar sync karein
        self._sync_dashboard_params()

    def _load_morning_cache(self):
        """OHLC aur SMA data ko Redis se RAM mein load karna"""
        logger.info("Nexus 1: RAM Cache build ho raha hai...")
        
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
        logger.info(f"Nexus 1: {len(self.stocks)} stocks RAM mein cache ho gaye.")

    def _sync_dashboard_params(self):
        """Dashboard settings ko Redis se RAM mein refresh karna"""
        for side in ['bull', 'bear']:
            raw_data = self.r.get(f"algo:settings:{side}")
            data = json.loads(raw_data) if raw_data else {}
            
            # Risk Reward parser
            rr_val = 2.0
            if 'risk_reward' in data and ':' in str(data['risk_reward']):
                rr_val = float(data['risk_reward'].split(':')[1])
            
            tsl_val = 1.5
            if 'trailing_sl' in data and ':' in str(data['trailing_sl']):
                tsl_val = float(data['trailing_sl'].split(':')[1])

            self.config[side] = {
                'max_total': int(data.get('total_trades', 5)),
                'max_per_stock': int(data.get('trades_per_stock', 2)),
                'rr': rr_val,
                'tsl': tsl_val,
                'vol_matrix': data.get('volume_criteria', []), # 10 Levels integrated
                'risk_tiers': [
                    float(data.get('risk_trade_1', 2000)),
                    float(data.get('risk_trade_2', 1500)),
                    float(data.get('risk_trade_3', 1000))
                ]
            }
        
        self.banned_set = self.r.smembers("algo:banned_symbols")
        self.engine_live['bull'] = self.r.get("algo:engine:bull:enabled") == "1"
        self.engine_live['bear'] = self.r.get("algo:engine:bear:enabled") == "1"

    def _is_vol_qualified(self, token, candle, side):
        """10-Level Volume SMA Matrix checking logic"""
        stock = self.stocks[token]
        matrix = self.config[side]['vol_matrix']
        if not matrix: return False

        c_vol = candle['volume']
        c_close = candle['close']
        s_sma = stock['sma']

        for level in matrix:
            # Min Avg SMA check
            if s_sma < float(level.get('min_sma_avg', 0)): continue 
            # SMA Multiplier check
            if c_vol < (s_sma * float(level.get('sma_multiplier', 1))): continue
            # Vol Price (Cr) check
            vol_cr = (c_vol * c_close) / 10000000.0
            if vol_cr >= float(level.get('min_vol_price_cr', 0)):
                return True
        return False

    def on_ticks(self, ws, ticks):
        """Main WebSocket loop - Yahan sabse fast execution hota hai"""
        now = dt.now(IST)
        bucket = now.replace(second=0, microsecond=0)
        # Heartbeat update for Dashboard
        self.r.set("algo:data:heartbeat", int(now.timestamp()), ex=15)

        for tick in ticks:
            token = tick.get('instrument_token')
            stock = self.stocks.get(token)
            if not stock or stock['symbol'] in self.banned_set: continue

            ltp = tick['last_price']
            vol = tick.get('volume_traded', 0)
            stock['last_ltp'] = ltp

            # 1. EXIT MONITORING
            if stock['status'] == 'OPEN':
                # Manual dashboard exit check
                if self.r.get(f"algo:manual_exit:{stock['symbol']}") == "1":
                    self._fire_hft_order(token, 'EXIT', ltp, 0, "MANUAL_EXIT")
                    self.r.delete(f"algo:manual_exit:{stock['symbol']}")
                    continue
                self._manage_exit_and_tsl(token, ltp)
                continue

            # 2. TRIGGER WATCH (6-min timer logic)
            if stock['status'] == 'TRIGGER_WATCH':
                if (now - stock['trigger_at']).total_seconds() > 360:
                    stock['status'] = 'PENDING'
                    continue
                
                # 0.01% Buffer already in trigger_px
                if stock['side_latch'] == 'BULL' and ltp > stock['trigger_px']:
                    self._fire_hft_order(token, 'BUY', ltp, stock['stop_base'], "BULL_BREAKOUT")
                elif stock['side_latch'] == 'BEAR' and ltp < stock['trigger_px']:
                    self._fire_hft_order(token, 'SELL', ltp, stock['stop_base'], "BEAR_BREAKDOWN")
                continue

            # 3. CANDLE FORMATION (1-min aggregation)
            if stock['candle'] and stock['candle']['bucket'] != bucket:
                self._check_for_signal(token, stock['candle'])
                stock['candle'] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': 0}
            elif not stock['candle']:
                stock['candle'] = {'bucket': bucket, 'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': 0}
            else:
                c = stock['candle']
                c['high'] = max(c['high'], ltp)
                c['low'] = min(c['low'], ltp)
                c['close'] = ltp
                if stock['last_vol'] > 0:
                    c['volume'] += max(0, vol - stock['last_vol'])
            
            stock['last_vol'] = vol

    def _check_for_signal(self, token, candle):
        """Candle close hone par strategy conditions check karna"""
        stock = self.stocks[token]
        if stock['status'] != 'PENDING': return

        # BULL Breakout Conditions
        if self.engine_live['bull'] and candle['open'] < stock['prev_high'] < candle['close']:
            if self._is_vol_qualified(token, candle, 'bull'):
                if stock['stock_trades'] < self.config['bull']['max_per_stock']:
                    stock['status'] = 'TRIGGER_WATCH'
                    stock['side_latch'] = 'BULL'
                    stock['trigger_px'] = candle['high'] * 1.0001 # 0.01% Entry Buffer
                    stock['trigger_at'] = dt.now(IST)
                    # Adaptive SL logic
                    stock['stop_base'] = stock['prev_high'] if (candle['high'] - candle['low'])/candle['close'] > 0.007 else candle['low']

        # BEAR Breakdown Conditions
        elif self.engine_live['bear'] and candle['open'] > stock['prev_low'] > candle['close']:
            if self._is_vol_qualified(token, candle, 'bear'):
                if stock['stock_trades'] < self.config['bear']['max_per_stock']:
                    stock['status'] = 'TRIGGER_WATCH'
                    stock['side_latch'] = 'BEAR'
                    stock['trigger_px'] = candle['low'] * 0.9999 # 0.01% Entry Buffer
                    stock['trigger_at'] = dt.now(IST)
                    stock['stop_base'] = stock['prev_low'] if (candle['high'] - candle['low'])/candle['close'] > 0.007 else candle['high']

    def _manage_exit_and_tsl(self, token, ltp):
        """Position management with 0.02% Exit Buffer"""
        trade = self.open_trades.get(token)
        if not trade: return

        if trade['side'] == 'BUY':
            # Target ya SL hit with buffer
            if ltp >= (trade['target'] * 1.0002) or ltp <= (trade['sl'] * 0.9998):
                self._fire_hft_order(token, 'SELL', ltp, 0, "BULL_EXIT")
                return
            # Step-wise Trailing
            profit = ltp - trade['entry_px']
            if profit >= trade['step']:
                lvls = floor(profit / trade['step'])
                new_sl = trade['entry_px'] + ((lvls - 1) * trade['step'])
                if new_sl > trade['sl']: trade['sl'] = round(new_sl * 20) / 20

        elif trade['side'] == 'SELL':
            if ltp <= (trade['target'] * 0.9998) or ltp >= (trade['sl'] * 1.0002):
                self._fire_hft_order(token, 'BUY', ltp, 0, "BEAR_EXIT")
                return
            profit = trade['entry_px'] - ltp
            if profit >= trade['step']:
                lvls = floor(profit / trade['step'])
                new_sl = trade['entry_px'] - ((lvls - 1) * trade['step'])
                if new_sl < trade['sl']: trade['sl'] = round(new_sl * 20) / 20

    def _fire_hft_order(self, token, side, price, stop_base, reason):
        """Order firing ko thread-safe async queue mein dalna"""
        stock = self.stocks[token]
        stock['status'] = 'EXECUTING'
        asyncio.run_coroutine_threadsafe(self._async_kite_execute(token, side, price, stop_base, reason), self.loop)

    async def _async_kite_execute(self, token, side, price, stop_base, reason):
        """Kite API call aur Slippage control logic"""
        stock = self.stocks[token]
        label = side.lower() if side in ['BUY', 'SELL'] else ('bull' if 'BULL' in reason else 'bear')
        
        try:
            # 1. LUA based atomic trade counter check
            if side in ['BUY', 'SELL']:
                if self.r.eval(LUA_INC_LIMIT, 1, f"hft:count:{self.user_id}:{label}", self.config[label]['max_total']) == -1:
                    logger.warning(f"Limit Reached for {label}")
                    stock['status'] = 'PENDING'; return

            # 2. Dynamic Risk Quantity
            risk_tier = self.config[label]['risk_tiers'][min(stock['stock_trades'], 2)]
            risk_per_qty = abs(price - stop_base)
            qty = max(1, int(floor(risk_tier / risk_per_qty))) if risk_per_qty > 0 else 1

            # 3. Market MIS Order
            oid = self.kite.place_order(
                tradingsymbol=stock['symbol'], exchange='NSE', transaction_type=side,
                quantity=qty, order_type='MARKET', product='MIS', variety='regular'
            )

            # 4. SLIPPAGE SYNC: Actual Avg Price fetch karna
            actual_px = price
            await asyncio.sleep(0.2)
            hist = self.kite.order_history(oid)
            if hist[-1]['status'] == 'COMPLETE':
                actual_px = float(hist[-1]['average_price'])
            
            # 5. Position Memory update
            if side in ['BUY', 'SELL']:
                stock['status'] = 'OPEN'
                stock['stock_trades'] += 1
                risk = abs(actual_px - stop_base)
                self.open_trades[token] = {
                    'side': side, 'entry_px': actual_px, 'sl': stop_base, 'qty': qty,
                    'target': actual_px + (risk * self.config[label]['rr']) if side == 'BUY' else actual_px - (risk * self.config[label]['rr']),
                    'step': risk * self.config[label]['tsl'], 'oid': oid
                }
            else:
                stock['status'] = 'CLOSED'
                self.open_trades.pop(token, None)
            
            logger.info(f"HFT Order {side} SUCCESS: {stock['symbol']} @ {actual_px}")

        except Exception as e:
            logger.error(f"Execution Error: {e}")
            stock['status'] = 'PENDING'

    async def settings_poller(self):
        """Dashboard settings ko har 2 sec mein refresh karne wala worker"""
        while True:
            try:
                self._sync_dashboard_params()
                # Django DB connection refresh for Heroku
                close_old_connections()
                await asyncio.sleep(2)
            except: pass

    def run(self):
        """Engine start point"""
        self.loop = asyncio.new_event_loop()
        # Background event loop thread start
        threading.Thread(target=self.loop.run_forever, daemon=True).start()
        
        # Async workers register karein
        asyncio.run_coroutine_threadsafe(self.settings_poller(), self.loop)
        
        # WebSocket Connect
        self.kws.on_ticks = self.on_ticks
        self.kws.connect(threaded=True)
        
        # Keep process alive
        while True:
            time.sleep(1)