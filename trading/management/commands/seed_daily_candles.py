import json
import logging
import time
from datetime import datetime as dt, timedelta, time as datetime_time

import pytz
from django.core.management.base import BaseCommand
from django.conf import settings
from kiteconnect import KiteConnect, exceptions as kite_exceptions
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from trading.models import Account
from trading.utils import get_redis_connection, get_kite

logger = logging.getLogger(__name__)
IST = pytz.timezone('Asia/Kolkata')

class Command(BaseCommand):
    help = 'Flushes Redis candle history and seeds it with previous trading day 1-minute data (Rate Limited).'

    def _get_previous_trading_day(self):
        """Finds the last valid trading day (skips weekends)."""
        today = dt.now(IST).date()
        prev_day = today - timedelta(days=1)
        # 0=Monday, 4=Friday, 5=Saturday, 6=Sunday
        while prev_day.weekday() >= 5:  # If Sat or Sun, go back
            prev_day -= timedelta(days=1)
        return prev_day

    def _default_json_serializer(self, obj):
        if isinstance(obj, (dt, datetime_time)):
            return obj.isoformat()
        return str(obj)

    # --- RETRY LOGIC ---
    # Retry up to 3 times on Network/Data errors with exponential backoff.
    # We do not retry on 429 immediately here because the global loop handles the pacing.
    @retry(
        retry=retry_if_exception_type((kite_exceptions.NetworkException, kite_exceptions.DataException, Exception)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    def fetch_and_seed_stock(self, kite, symbol, token, from_date, to_date, redis_client):
        """
        Fetches data for a single stock and pushes to Redis.
        """
        key = f'candles:1m:{symbol}'
        
        # 1. Fetch 1-minute data
        # Kite Historical API: interval='minute'
        candles = kite.historical_data(
            instrument_token=token,
            from_date=from_date,
            to_date=to_date,
            interval='minute'
        )

        if not candles:
            return 0

        # 2. Process and Format
        formatted_candles = []
        for candle in candles:
            payload = {
                'symbol': symbol,
                'interval': '1m',
                'ts': candle['date'].isoformat(), 
                'open': candle['open'],
                'high': candle['high'],
                'low': candle['low'],
                'close': candle['close'],
                'volume': candle['volume'],
                # Set SMA to 0 for historicals; Live engine calculates baseline from these.
                'vol_sma_375': 0, 
                'vol_price_cr': round((candle['volume'] * candle['close']) / 10000000.0, 4)
            }
            formatted_candles.append(json.dumps(payload, default=self._default_json_serializer))

        if not formatted_candles:
            return 0

        # 3. Flush & Push to Redis (Atomic Pipeline)
        pipe = redis_client.pipeline()
        pipe.delete(key) # Flush old data
        pipe.rpush(key, *formatted_candles) # Seed new data
        pipe.execute()

        return len(formatted_candles)

    def handle(self, *args, **options):
        redis_client = get_redis_connection()
        if not redis_client:
            self.stdout.write(self.style.ERROR("Redis connection failed"))
            return

        master_account = Account.objects.filter(is_master=True, user__is_superuser=True).first()
        if not master_account:
            self.stdout.write(self.style.ERROR("No master account found"))
            return

        kite = get_kite(master_account)
        
        # 1. Determine Date Range
        prev_date = self._get_previous_trading_day()
        from_date = dt.combine(prev_date, datetime_time(9, 15))
        to_date = dt.combine(prev_date, datetime_time(15, 30))

        self.stdout.write(self.style.SUCCESS(f"Seeding data for Date: {prev_date} ({from_date} to {to_date})"))

        # 2. Map Tokens
        logger.info("Fetching Instrument dump...")
        try:
            instruments = kite.instruments("NSE")
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Failed to fetch instruments: {e}"))
            return

        target_symbols = settings.STOCK_INDEX_MAPPING.keys()
        
        symbol_token_map = {}
        for inst in instruments:
            if inst['tradingsymbol'] in target_symbols:
                symbol_token_map[inst['tradingsymbol']] = inst['instrument_token']

        total_stocks = len(symbol_token_map)
        logger.info(f"Found {total_stocks} tokens matching universe.")

        # 3. Sequential Execution with Rate Limiting
        total_seeded = 0
        stocks_processed = 0
        errors = 0
        
        start_time = time.time()
        
        self.stdout.write(f"Starting sequential processing of {total_stocks} stocks...")

        for symbol, token in symbol_token_map.items():
            try:
                count = self.fetch_and_seed_stock(
                    kite, symbol, token, from_date, to_date, redis_client
                )
                total_seeded += count
            except Exception as e:
                # Log error but continue to next stock
                logger.error(f"SEED: Error processing {symbol}: {e}")
                self.stdout.write(self.style.ERROR(f"Failed {symbol}: {e}"))
                errors += 1
            
            stocks_processed += 1
            
            # Progress bar style output
            if stocks_processed % 25 == 0:
                 self.stdout.write(f"Progress: {stocks_processed}/{total_stocks} | Errors: {errors}")

            # --- RATE LIMIT ENFORCEMENT ---
            # Kite Limit: 3 requests/second (1 req every 0.33s).
            # Sleep 0.4s to be safe and account for network overhead.
            time.sleep(0.4) 

        duration = time.time() - start_time
        
        self.stdout.write(self.style.SUCCESS(
            f"DONE. Seeded {total_seeded} candles. Processed {stocks_processed} stocks. "
            f"Errors: {errors}. Time taken: {duration:.2f}s"
        ))