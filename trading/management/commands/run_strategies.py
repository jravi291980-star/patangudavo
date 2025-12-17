import time
import logging
import threading
import sys
from django.core.management.base import BaseCommand
from trading.models import Account

# Import Your 4 Trading Engines
from trading.bull_engine import CashBreakoutClient
from trading.bear_engine import CashBreakdownClient
from trading.momentum_bull_engine import MomentumBullClient
from trading.momentum_bear_engine import MomentumBearClient

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Runs the 4 Algo Trading Strategies (Separate Process)'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting STRATEGY Worker Process..."))
        
        # 1. Fetch Master Account
        master_account = Account.objects.filter(is_master=True).first()
        if not master_account:
            master_account = Account.objects.filter(user__is_superuser=True).first()
        
        if not master_account:
            self.stdout.write(self.style.ERROR("CRITICAL: No Master Account found. Cannot start strategies."))
            sys.exit(1)

        # --- Resilience Wrapper ---
        # This function acts as a supervisor for each individual engine.
        # If an engine crashes (raises an Exception), this loop catches it and restarts ONLY that engine.
        def run_resilient_engine(EngineClass, name):
            while True:
                try:
                    logger.info(f"[{name}] Initializing...")
                    # Initialize the engine class with the account
                    engine = EngineClass(master_account)
                    
                    # Run the engine. This is a BLOCKING call (it has a while loop inside).
                    # It will only return if engine.stop() is called or it crashes.
                    engine.run() 
                    
                    logger.warning(f"[{name}] Stopped cleanly. Restarting in 2s...")
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"[{name}] CRASHED with error: {e}")
                    logger.info(f"[{name}] Auto-Restarting in 5 seconds...")
                    time.sleep(5)

        # 2. Define the Strategies to Launch
        strategies = [
            (CashBreakoutClient, "BULL_ENGINE"),
            (CashBreakdownClient, "BEAR_ENGINE"),
            (MomentumBullClient, "MOM_BULL"),
            (MomentumBearClient, "MOM_BEAR"),
        ]

        # 3. Launch Each Strategy in a Separate Daemon Thread
        threads = []
        for Cls, name in strategies:
            t = threading.Thread(
                target=run_resilient_engine, 
                args=(Cls, name), 
                daemon=True, 
                name=name
            )
            t.start()
            threads.append(t)
            self.stdout.write(f"Launched {name}")

        self.stdout.write(self.style.SUCCESS(f"All {len(strategies)} Strategies Running in Parallel."))

        # 4. Main Process Loop (Supervisor)
        # This keeps the main process alive. If we didn't have this, the script would exit 
        # and all daemon threads (the engines) would be killed immediately.
        try:
            while True:
                # We can add a high-level health check here if needed.
                # For now, we just sleep to keep the process running.
                time.sleep(10)
                
                # Optional: Check if threads are alive (they should be, due to the resilient wrapper)
                dead_threads = [t.name for t in threads if not t.is_alive()]
                if dead_threads:
                    logger.critical(f"CRITICAL: The following engine threads have died completely: {dead_threads}")
                    # If threads die despite the wrapper, something is very wrong (e.g., OOM).
                    # We exit to force Heroku to restart the whole dyno.
                    sys.exit(1)

        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("\nStopping Strategies..."))
            # Note: Since threads are daemons, they will be killed automatically when this script exits.
            # But we can try a graceful exit if we wanted to add stop() calls here.
            self.stdout.write("Stopped.")