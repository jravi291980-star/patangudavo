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
    help = 'Seeds Redis with 5 days of 1-minute candle data for 1875 SMA.'

    def _get_start_date(self, days_back=5):
        """Finds the start date going back N trading days."""
        today = dt.now(IST).date()
        count = 0
        curr = today
        while count < days_back:
            curr -= timedelta(days=1)
            # 0=Mon, 4=Fri, 5=Sat, 6=Sun
            if curr.weekday() < 5:
                count += 1
        return curr

    def _default_json_serializer(self, obj):
        if isinstance(obj, (dt, datetime_time)):
            return obj.isoformat()
        return str(obj)

    @retry(
        retry=retry_if_exception_type((kite_exceptions.NetworkException, kite_exceptions.DataException, Exception)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    def fetch_and_seed_stock(self, kite, symbol, token, from_date, to_date, redis_client):
        key = f'candles:1m:{symbol}'
        
        # Fetch 1-minute data for the extended period
        candles = kite.historical_data(
            instrument_token=token,
            from_date=from_date,
            to_date=to_date,
            interval='minute'
        )

        if not candles:
            return 0

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
                'vol_sma_375': 0, # Will be calc'd by live engine
                'vol_price_cr': round((candle['volume'] * candle['close']) / 10000000.0, 4)
            }
            formatted_candles.append(json.dumps(payload, default=self._default_json_serializer))

        if not formatted_candles:
            return 0

        pipe = redis_client.pipeline()
        pipe.delete(key)
        pipe.rpush(key, *formatted_candles)
        pipe.execute()

        return len(formatted_candles)

    def handle(self, *args, **options):
        redis_client = get_redis_connection()
        if not redis_client: return

        master_account = Account.objects.filter(is_master=True, user__is_superuser=True).first()
        if not master_account:
            self.stdout.write(self.style.ERROR("No master account found"))
            return

        kite = get_kite(master_account)
        
        # 1. 5 Days Lookback for 1875 SMA
        start_date = self._get_start_date(days_back=5)
        end_date = dt.now(IST).date()
        
        from_date = dt.combine(start_date, datetime_time(9, 15))
        to_date = dt.combine(end_date, datetime_time(15, 30))

        self.stdout.write(self.style.SUCCESS(f"Seeding 5-day history: {from_date} to {to_date}"))

        try:
            instruments = kite.instruments("NSE")
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Failed to fetch instruments: {e}"))
            return

        target_symbols = settings.STOCK_INDEX_MAPPING.keys()
        symbol_token_map = {i['tradingsymbol']: i['instrument_token'] for i in instruments if i['tradingsymbol'] in target_symbols}

        total_stocks = len(symbol_token_map)
        stocks_processed = 0
        
        self.stdout.write(f"Processing {total_stocks} stocks...")

        for symbol, token in symbol_token_map.items():
            try:
                self.fetch_and_seed_stock(kite, symbol, token, from_date, to_date, redis_client)
            except Exception as e:
                logger.error(f"SEED: Error {symbol}: {e}")
            
            stocks_processed += 1
            if stocks_processed % 20 == 0:
                self.stdout.write(f"Processed: {stocks_processed}/{total_stocks}")
            time.sleep(0.4) # Rate limit

        self.stdout.write(self.style.SUCCESS("Seeding Complete."))