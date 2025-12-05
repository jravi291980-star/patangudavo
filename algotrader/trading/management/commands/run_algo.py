import time
import logging
import threading
from django.core.management.base import BaseCommand
from trading.models import Account
from trading.bull_engine import CashBreakoutClient
from trading.bear_engine import CashBreakdownClient
from trading.websockets import start_websocket_thread, start_fno_websocket_thread

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Runs the Algo Trading Engines and Websockets'

    def handle(self, *args, **options):
        self.stdout.write("Starting Algo Trading Worker...")
        
        # 1. Fetch Master Account
        master_account = Account.objects.filter(is_master=True).first()
        if not master_account:
            self.stdout.write(self.style.ERROR("No Master Account found! Please create an account with is_master=True."))
            return

        if not master_account.access_token:
            self.stdout.write(self.style.WARNING("Master Account has no Access Token. Waiting for login via Dashboard..."))
            # In a real loop, you might wait/retry here.
            return

        # 2. Start Websockets
        self.stdout.write("Starting Websockets...")
        # Cash/Index WebSocket (Feed for Candle Aggregator)
        ws_cash = start_websocket_thread(master_account)
        # F&O WebSocket (Optional, if you use FNO logic)
        # ws_fno = start_fno_websocket_thread(master_account) 

        # 3. Start Bull Engine
        self.stdout.write("Starting Bull Engine (Cash Breakout)...")
        bull_client = CashBreakoutClient(master_account)
        bull_thread = threading.Thread(target=bull_client.run, daemon=True)
        bull_thread.start()

        # 4. Start Bear Engine
        self.stdout.write("Starting Bear Engine (Cash Breakdown)...")
        bear_client = CashBreakdownClient(master_account)
        bear_thread = threading.Thread(target=bear_client.run, daemon=True)
        bear_thread.start()

        self.stdout.write(self.style.SUCCESS("All Engines Started. Press Ctrl+C to stop."))

        # 5. Keep Main Thread Alive
        try:
            while True:
                time.sleep(1)
                # Optional: Health checks on threads can go here
        except KeyboardInterrupt:
            self.stdout.write("Stopping engines...")
            bull_client.stop()
            bear_client.stop()
            ws_cash.stop()
            # ws_fno.stop()
            self.stdout.write("Stopped.")