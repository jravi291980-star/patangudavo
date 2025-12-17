import time
import logging
import sys
import threading
from django.core.management.base import BaseCommand
from trading.models import Account
from trading.websockets import start_websocket_thread

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Runs ONLY the Websocket Data Engine (Separate Process)'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting DATA Worker Process..."))
        
        # 1. Fetch Master Account
        # Try finding an account marked as 'is_master' first
        master_account = Account.objects.filter(is_master=True).first()
        if not master_account:
            # Fallback: Use the account associated with the first superuser
            master_account = Account.objects.filter(user__is_superuser=True).first()
            
        if not master_account:
            self.stdout.write(self.style.ERROR("CRITICAL: No Master Account found."))
            # Exit with error code 1 so the supervisor/Heroku knows to restart it
            sys.exit(1)

        if not master_account.access_token:
            self.stdout.write(self.style.ERROR(f"CRITICAL: Master Account ({master_account.user.username}) has no Access Token. Please login via Dashboard."))
            sys.exit(1)

        # 2. Start Websocket Engine (Threaded)
        # This function (from websockets.py) initializes the LiveTickerManager and starts its run loop in a daemon thread.
        try:
            ws_manager = start_websocket_thread(master_account)
            if not ws_manager:
                 self.stdout.write(self.style.ERROR("Failed to start Websocket Manager."))
                 sys.exit(1)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Websocket Launch Exception: {e}"))
            sys.exit(1)

        self.stdout.write(self.style.SUCCESS(f">> Websocket Connected for {master_account.user.username}"))
        self.stdout.write(self.style.SUCCESS(">> Streaming Data to Redis..."))

        # 3. Monitor Loop
        # This keeps the main process alive. If the background thread dies, we kill the process to force a restart.
        try:
            while True:
                # Check if the websocket manager's running flag is still True
                # The 'run' loop in LiveTickerManager sets self.running = False if it exits or crashes.
                if not ws_manager.running:
                    logger.critical("Data Engine stopped running (Thread died). Exiting process to trigger restart.")
                    sys.exit(1)
                
                # Check if the thread itself is alive (double check)
                # In start_websocket_thread, we didn't return the thread object, so we rely on the .running flag.
                # However, if we wanted to be stricter, we could inspect threading.enumerate().
                
                time.sleep(5)
        except KeyboardInterrupt:
            self.stdout.write("Stopping Data Worker...")
            ws_manager.stop()
            self.stdout.write("Stopped.")