import logging
import json
from django.core.management.base import BaseCommand
from kiteconnect import exceptions as kite_exceptions

# Assuming these imports exist in your project structure
from trading.models import Account
from trading.utils import get_kite

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Fetches LTP for a list of symbols and filters out those trading below 50.0.'

    # --- DEFINE TARGET SYMBOLS HERE ---
    # You can customize this list or import it from settings if available.
    TARGET_SYMBOLS = [
        "ITC", "TCS", "IDEA", "YESBANK", "RELIANCE", 
        "TATAMOTORS", "SUZLON", "INFY", "ZOMATO"
    ]
    
    LTP_THRESHOLD = 50.0
    
    def handle(self, *args, **options):
        self.stdout.write(f"--- Starting LTP Filter (Threshold: ₹{self.LTP_THRESHOLD}) ---")

        master_account = Account.objects.filter(is_master=True, user__is_superuser=True).first()
        if not master_account:
            self.stdout.write(self.style.ERROR("Error: No master account found. Cannot initialize Kite API."))
            return

        try:
            kite = get_kite(master_account)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error initializing Kite: {e}"))
            return

        # 1. Prepare symbols for Kite API
        # Kite requires symbols in 'EXCHANGE:SYMBOL' format (e.g., 'NSE:ITC')
        kite_symbols = [f"NSE:{symbol}" for symbol in self.TARGET_SYMBOLS]
        
        # 2. Fetch Quotes
        self.stdout.write(f"Fetching quotes for {len(self.TARGET_SYMBOLS)} symbols...")
        quote_data = {}
        try:
            quote_data = kite.quote(kite_symbols)
        except kite_exceptions.InputException as e:
            self.stdout.write(self.style.ERROR(f"Kite Input Error (Check symbols): {e}"))
            return
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Kite API Error: {e}"))
            return

        filtered_symbols = []
        
        # 3. Process and Filter Data
        for kite_key, data in quote_data.items():
            # Extract the pure symbol (e.g., "ITC" from "NSE:ITC")
            symbol = kite_key.split(':')[-1]
            
            ltp = data.get('last_price', 0.0)
            
            if ltp is None or ltp == 0.0:
                self.stdout.write(self.style.WARNING(f"Skipping {symbol}: LTP missing or zero."))
                continue

            if ltp >= self.LTP_THRESHOLD:
                filtered_symbols.append((symbol, ltp))
            else:
                self.stdout.write(f"Filtered out {symbol}: LTP is {ltp:.2f} (Below threshold).")

        # 4. Print Results
        if filtered_symbols:
            self.stdout.write(self.style.SUCCESS("\n--- FILTERED SYMBOL LIST (LTP >= 50.0) ---"))
            
            # Sort by LTP (optional, but helpful)
            filtered_symbols.sort(key=lambda x: x[1], reverse=True)
            
            for symbol, ltp in filtered_symbols:
                self.stdout.write(f"{symbol.ljust(15)}: ₹{ltp:.2f}")

            # Print the final list of names for easy copying
            final_names = [s[0] for s in filtered_symbols]
            self.stdout.write(self.style.SUCCESS("\nFinal Filtered Symbol Names:"))
            self.stdout.write(str(final_names))

        else:
            self.stdout.write(self.style.WARNING("\nNo stocks met the required LTP threshold."))