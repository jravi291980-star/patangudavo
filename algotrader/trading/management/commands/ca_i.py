# trading/management/commands/cache_instruments.py
from django.core.management.base import BaseCommand
from trading.websockets import cache_instruments_for_day
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Fetches and caches all exchange instruments for the day.'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting the instrument caching process...'))
        try:
            cache_instruments_for_day()
            self.stdout.write(self.style.SUCCESS('Successfully cached all instruments.'))
        except Exception as e:
            logger.error(f"Instrument caching failed: {e}", exc_info=True)
            self.stdout.write(self.style.ERROR('An error occurred during instrument caching. Check logs.'))