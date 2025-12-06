import json
import logging
import time
from datetime import datetime as dt, timedelta, time as datetime_time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytz
from django.core.management.base import BaseCommand
from django.conf import settings
from kiteconnect import KiteConnect

from trading.models import Account
from trading.utils import get_redis_connection, get_kite

logger = logging.getLogger(__name__)
IST = pytz.timezone('Asia/Kolkata')

class Command(BaseCommand):
    help = 'Flushes Redis candle history and seeds it with the previous trading days 1-minute data for Volume SMA.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--days-back',
            type=int,
            default=1,
            help='How many days back to look for the previous trading day (default handles weekends auto)'
        )

    def _get_previous_trading_day(self):
        """
        Finds the last valid trading day (skips weekends).
        Does not account for public holidays automatically, 
        so manual intervention needed on holidays or advanced logic.
        """
        today = dt.now(IST).date()
        
        # If running before 9 AM, we likely want yesterday's data relative to today.
        # But generally, we just want the "last completed market session".
        
        prev_day = today - timedelta(days=1)
        
        # 0=Monday, 4=Friday, 5=Saturday, 6=Sunday
        while prev_day.weekday() >= 5:  # If Sat or Sun, go back
            prev_day -= timedelta(days=1)
            
        return prev_day

    def _default_json_serializer(self, obj):
        """Matches the serializer used in your Websocket manager"""
        if isinstance(obj, (dt, datetime_time)):
            return obj.isoformat()
        return str(obj)

    def fetch_and_seed_stock(self, kite, symbol, token, from_date, to_date, redis_client):
        """
        Fetches data for a single stock and pushes to Redis.
        """
        key = f'candles:1m:{symbol}'
        
        try:
            # 1. Fetch 1minute data
            # Kite historical API: interval='minute'
            candles = kite.historical_data(
                instrument_token=token,
                from_date=from_date,
                to_date=to_date,
                interval='minute'
            )

            if not candles:
                # logger.warning(f"SEED: No data found for {symbol}")
                return 0

            # 2. Process and Format
            # We expect a list of 375 candles (09:15 to 15:29)
            formatted_candles = []
            
            for candle in candles:
                # Format must match CandleAggregator exactly
                payload = {
                    'symbol': symbol,
                    'interval': '1m',
                    # Kite returns native datetime, convert to isoformat string
                    'ts': candle['date'].isoformat(), 
                    'open': candle['open'],
                    'high': candle['high'],
                    'low': candle['low'],
                    'close': candle['close'],
                    'volume': candle['volume'],
                    # We don't have SMA for these historicals, set 0 or None.
                    # The live engine calculates SMA *using* these, it doesn't read SMA *from* these.
                    'vol_sma_375': 0, 
                    'vol_price_cr': round((candle['volume'] * candle['close']) / 10000000.0, 4)
                }
                formatted_candles.append(json.dumps(payload, default=self._default_json_serializer))

            if not formatted_candles:
                return 0

            # 3. Flush & Push to Redis
            # We use a pipeline to make this atomic and fast
            pipe = redis_client.pipeline()
            pipe.delete(key) # Flush old data
            pipe.rpush(key, *formatted_candles) # Seed new data
            pipe.execute()

            return len(formatted_candles)

        except Exception as e:
            logger.error(f"SEED: Error processing {symbol}: {e}")
            return 0

    def handle(self, *args, **options):
        redis_client = get_redis_connection()
        if not redis_client:
            self.stdout.write(self.style.ERROR("Redis connection failed"))
            return

        # 1. Get Master Account for Kite API
        master_account = Account.objects.filter(is_master=True, user__is_superuser=True).first()
        if not master_account:
            self.stdout.write(self.style.ERROR("No master account found"))
            return

        kite = get_kite(master_account)
        
        # 2. Identify Time Range
        prev_date = self._get_previous_trading_day()
        
        # We fetch the full day: 09:15 to 15:30
        from_date = dt.combine(prev_date, datetime_time(9, 15))
        to_date = dt.combine(prev_date, datetime_time(15, 30))

        self.stdout.write(self.style.SUCCESS(f"Seeding data for Date: {prev_date} ({from_date} to {to_date})"))

        # 3. Get Instrument Tokens for Stock Universe
        # We need to map Symbol -> Token
        logger.info("Fetching Instrument dump to map tokens...")
        instruments = kite.instruments("NSE")
        
        # Filter for our universe
        target_symbols = settings.STOCK_INDEX_MAPPING.keys()
        
        # Create map: {'RELIANCE': 738561, ...}
        symbol_token_map = {}
        for inst in instruments:
            if inst['tradingsymbol'] in target_symbols:
                symbol_token_map[inst['tradingsymbol']] = inst['instrument_token']

        logger.info(f"Found {len(symbol_token_map)} tokens matching STOCK_INDEX_MAPPING universe.")

        # 4. Threaded Execution
        # Fetching 500 stocks sequentially takes too long. We use threads.
        total_seeded = 0
        stocks_processed = 0
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Create a dictionary of futures
            future_to_symbol = {
                executor.submit(
                    self.fetch_and_seed_stock, 
                    kite, 
                    sym, 
                    token, 
                    from_date, 
                    to_date, 
                    redis_client
                ): sym for sym, token in symbol_token_map.items()
            }

            for future in as_completed(future_to_symbol):
                sym = future_to_symbol[future]
                try:
                    count = future.result()
                    total_seeded += count
                    stocks_processed += 1
                    if stocks_processed % 50 == 0:
                        self.stdout.write(f"Processed {stocks_processed}/{len(symbol_token_map)} stocks...")
                except Exception as exc:
                    self.stdout.write(self.style.ERROR(f'{sym} generated an exception: {exc}'))

        duration = time.time() - start_time
        
        self.stdout.write(self.style.SUCCESS(
            f"DONE. Seeded {total_seeded} candles across {stocks_processed} stocks in {duration:.2f} seconds."
        ))