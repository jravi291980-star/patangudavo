# # import json
# # import logging
# # import time
# # from datetime import datetime as dt
# # from django.http import JsonResponse
# # from django.views.decorators.csrf import csrf_exempt
# # from django.views.decorators.http import require_http_methods
# # from django.contrib.auth.decorators import login_required
# # from django.shortcuts import redirect, render
# # from kiteconnect import KiteConnect
# # from .models import Account, CashBreakoutTrade, CashBreakdownTrade
# # from .utils import get_redis_connection, get_kite

# # logger = logging.getLogger(__name__)
# # redis_client = get_redis_connection()

# # # --- REDIS KEYS ---
# # KEY_ACCESS_TOKEN = "kite:access_token"
# # KEY_GLOBAL_SETTINGS = "algo:settings:global"
# # KEY_BULL_SETTINGS = "algo:settings:bull"
# # KEY_BEAR_SETTINGS = "algo:settings:bear"
# # KEY_ENGINE_BULL_ENABLED = "algo:engine:bull:enabled"
# # KEY_ENGINE_BEAR_ENABLED = "algo:engine:bear:enabled"
# # KEY_BLACKLIST = "algo:blacklist"
# # KEY_PANIC_BULL = "algo:panic:bull"
# # KEY_PANIC_BEAR = "algo:panic:bear"
# # DATA_HEARTBEAT_KEY = "algo:data:heartbeat"

# # # --- HELPER ---
# # def get_user_account(user):
# #     return Account.objects.filter(user=user).first()

# # # --- VIEWS ---

# # def home(request):
# #     return render(request, 'trading/dashboard.html')

# # @login_required
# # def kite_login(request):
# #     account = get_user_account(request.user)
# #     if not account or not account.api_key:
# #         return JsonResponse({'error': 'Account or API Key not configured in Database'}, status=400)

# #     kite = KiteConnect(api_key=account.api_key)
# #     return redirect(kite.login_url())

# # @login_required
# # def kite_callback(request):
# #     request_token = request.GET.get('request_token')
# #     if not request_token:
# #         return JsonResponse({'error': 'No request_token received'}, status=400)

# #     account = get_user_account(request.user)
# #     if not account:
# #         return JsonResponse({'error': 'Account not found'}, status=400)

# #     try:
# #         kite = KiteConnect(api_key=account.api_key)
# #         if not account.api_secret:
# #              return JsonResponse({'error': 'API Secret not set in Dashboard'}, status=400)

# #         data = kite.generate_session(request_token, api_secret=account.api_secret)
# #         access_token = data['access_token']

# #         # 1. Store in Database
# #         account.access_token = access_token
# #         account.save()

# #         # 2. Store in Redis
# #         if redis_client:
# #             redis_client.set(KEY_ACCESS_TOKEN, access_token)
        
# #         logger.info(f"Access token generated successfully for user {request.user.username}")
# #         return redirect('/dashboard')
        
# #     except Exception as e:
# #         logger.error(f"Error generating Kite session: {e}", exc_info=True)
# #         return JsonResponse({'error': f"Kite Auth Failed: {str(e)}"}, status=500)

# # # --- DASHBOARD API ---

# # @login_required
# # @require_http_methods(["GET"])
# # def dashboard_stats(request):
# #     """
# #     Returns Real-Time P&L (Realized from DB + Unrealized from Live Feed) 
# #     and Data Continuity Status.
# #     """
# #     account = get_user_account(request.user)
# #     if not account: return JsonResponse({'error': 'No account'}, status=404)

# #     today = dt.now().date()
    
# #     # 1. Realized P&L from DB (Closed Trades today)
# #     bull_realized = sum(t.pnl for t in CashBreakoutTrade.objects.filter(account=account, status='CLOSED', exit_time__date=today))
# #     bear_realized = sum(t.pnl for t in CashBreakdownTrade.objects.filter(account=account, status='CLOSED', exit_time__date=today))

# #     # 2. Unrealized P&L from Redis (Calculated Live)
# #     # We fetch OPEN trades and compare entry_price with live LTP
# #     open_bull = CashBreakoutTrade.objects.filter(account=account, status='OPEN')
# #     open_bear = CashBreakdownTrade.objects.filter(account=account, status='OPEN')
    
# #     bull_unrealized = 0.0
# #     bear_unrealized = 0.0
# #     live_data = {}
    
# #     if redis_client:
# #         try:
# #             raw = redis_client.get('live_ohlc_data')
# #             if raw: live_data = json.loads(raw)
# #         except: pass

# #     # Calculate Bull Unrealized (LTP - Entry)
# #     for t in open_bull:
# #         ltp = float(live_data.get(t.symbol, {}).get('ltp', t.entry_price or 0))
# #         if ltp > 0 and t.entry_price:
# #             bull_unrealized += (ltp - t.entry_price) * t.quantity

# #     # Calculate Bear Unrealized (Entry - LTP)
# #     for t in open_bear:
# #         ltp = float(live_data.get(t.symbol, {}).get('ltp', t.entry_price or 0))
# #         if ltp > 0 and t.entry_price:
# #             bear_unrealized += (t.entry_price - ltp) * t.quantity

# #     # 3. Data Heartbeat Check (Green/Red Dot)
# #     last_beat = 0
# #     if redis_client:
# #         try: last_beat = int(redis_client.get(DATA_HEARTBEAT_KEY) or 0)
# #         except: pass
    
# #     # Data is "connected" if heartbeat updated within last 15 seconds
# #     data_connected = (time.time() - last_beat) < 15

# #     bull_active = redis_client.get(KEY_ENGINE_BULL_ENABLED) or "1" if redis_client else "1"
# #     bear_active = redis_client.get(KEY_ENGINE_BEAR_ENABLED) or "1" if redis_client else "1"

# #     # Total P&L Breakdown
# #     total_bull_pnl = bull_realized + bull_unrealized
# #     total_bear_pnl = bear_realized + bear_unrealized
# #     total_global_pnl = total_bull_pnl + total_bear_pnl

# #     return JsonResponse({
# #         'pnl': {
# #             'bull': round(total_bull_pnl, 2),
# #             'bear': round(total_bear_pnl, 2),
# #             'total': round(total_global_pnl, 2),
# #             'unrealized': round(bull_unrealized + bear_unrealized, 2)
# #         },
# #         'engine_status': {
# #             'bull': bull_active.decode() if isinstance(bull_active, bytes) else str(bull_active),
# #             'bear': bear_active.decode() if isinstance(bear_active, bytes) else str(bear_active)
# #         },
# #         'data_connected': data_connected,
# #         'sentiment': {'percentage': 0, 'isBullish': True} 
# #     })

# # @csrf_exempt
# # @login_required
# # @require_http_methods(["GET", "POST"])
# # def global_settings_view(request):
# #     """
# #     Handles Global Settings:
# #     1. API Keys (Saved to DB)
# #     2. Global Volume Criteria (Saved to DB JSONField + Redis)
# #     """
# #     account = get_user_account(request.user)
# #     if not account: return JsonResponse({'error': 'No account'}, 404)

# #     if request.method == "POST":
# #         try:
# #             payload = json.loads(request.body)
            
# #             # 1. Update API Keys
# #             if 'api_key' in payload: account.api_key = payload['api_key']
# #             if 'api_secret' in payload: account.api_secret = payload['api_secret']
            
# #             # 2. Update Volume Settings (JSON)
# #             if 'volume_criteria' in payload:
# #                 account.volume_settings_json = payload['volume_criteria']
# #                 # Sync to Redis for Engines
# #                 if redis_client:
# #                     redis_client.set(KEY_GLOBAL_SETTINGS, json.dumps({"volume_criteria": payload['volume_criteria']}))

# #             account.save()
# #             return JsonResponse({'status': 'updated', 'message': 'Global Settings saved successfully'})
# #         except Exception as e:
# #             return JsonResponse({'error': str(e)}, status=400)
    
# #     # GET Request
# #     # Prefer Redis for speed, fallback to DB
# #     redis_vol_data = []
# #     if redis_client:
# #         raw = redis_client.get(KEY_GLOBAL_SETTINGS)
# #         if raw: 
# #             try: redis_vol_data = json.loads(raw).get('volume_criteria', [])
# #             except: pass
    
# #     # If Redis empty, use DB
# #     if not redis_vol_data:
# #         redis_vol_data = account.volume_settings_json or []

# #     return JsonResponse({
# #         "api_key": account.api_key or "",
# #         "api_secret": account.api_secret or "",
# #         "volume_criteria": redis_vol_data
# #     })

# # @csrf_exempt
# # @login_required
# # @require_http_methods(["GET", "POST"])
# # def engine_settings_view(request, engine_type):
# #     """
# #     Syncs Engine Specific Settings (Risk, Time, etc.)
# #     between React Dashboard -> Redis -> DB Model.
# #     Unified Input: Admin/Model <-> Dashboard.
# #     """
# #     account = get_user_account(request.user)
# #     if not account: return JsonResponse({'error': 'No account'}, 404)
    
# #     if not redis_client: return JsonResponse({'error': 'Redis unavailable'}, 503)

# #     redis_key = KEY_BULL_SETTINGS if engine_type == 'bull' else KEY_BEAR_SETTINGS

# #     if request.method == "POST":
# #         try:
# #             payload = json.loads(request.body)
# #             # 1. Update Redis (Fast Access for Engines)
# #             redis_client.set(redis_key, json.dumps(payload))
            
# #             # 2. Update DB Model (Persistence for Admin/Reload)
# #             if engine_type == 'bull':
# #                 account.breakout_risk_reward = payload.get('risk_reward', account.breakout_risk_reward)
# #                 account.breakout_trailing_sl = payload.get('trailing_sl', account.breakout_trailing_sl)
# #                 account.breakout_max_trades = int(payload.get('total_trades', account.breakout_max_trades))
# #                 account.breakout_trades_per_stock = int(payload.get('trades_per_stock', account.breakout_trades_per_stock))
# #                 account.breakout_risk_trade_1 = float(payload.get('risk_trade_1', account.breakout_risk_trade_1))
# #                 account.breakout_risk_trade_2 = float(payload.get('risk_trade_2', account.breakout_risk_trade_2))
# #                 account.breakout_risk_trade_3 = float(payload.get('risk_trade_3', account.breakout_risk_trade_3))
# #                 # Sync Timings
# #                 if 'start_time' in payload: account.breakout_start_time = payload['start_time']
# #                 if 'end_time' in payload: account.breakout_end_time = payload['end_time']
# #             else:
# #                 account.breakdown_risk_reward = payload.get('risk_reward', account.breakdown_risk_reward)
# #                 account.breakdown_trailing_sl = payload.get('trailing_sl', account.breakdown_trailing_sl)
# #                 account.breakdown_max_trades = int(payload.get('total_trades', account.breakdown_max_trades))
# #                 account.breakdown_trades_per_stock = int(payload.get('trades_per_stock', account.breakdown_trades_per_stock))
# #                 account.breakdown_risk_trade_1 = float(payload.get('risk_trade_1', account.breakdown_risk_trade_1))
# #                 account.breakdown_risk_trade_2 = float(payload.get('risk_trade_2', account.breakdown_risk_trade_2))
# #                 account.breakdown_risk_trade_3 = float(payload.get('risk_trade_3', account.breakdown_risk_trade_3))
# #                 if 'start_time' in payload: account.breakdown_start_time = payload['start_time']
# #                 if 'end_time' in payload: account.breakdown_end_time = payload['end_time']
            
# #             # Global P&L Sync
# #             if 'pnl_exit_enabled' in payload: account.pnl_exit_enabled = payload['pnl_exit_enabled']
# #             if 'max_profit' in payload: account.max_daily_profit = float(payload['max_profit'])
# #             if 'max_loss' in payload: account.max_daily_loss = float(payload['max_loss'])

# #             account.save()
# #             return JsonResponse({'status': 'updated', 'engine': engine_type})
# #         except Exception as e:
# #             return JsonResponse({'error': str(e)}, status=400)

# #     # GET: Return merged data from Redis (preferred) or DB
# #     data = redis_client.get(redis_key)
# #     if data:
# #         return JsonResponse(json.loads(data))
# #     else:
# #         # Fallback to DB values if Redis is empty
# #         prefix = 'breakout' if engine_type == 'bull' else 'breakdown'
        
# #         # Helper to safely format time
# #         def fmt_time(t): return t.strftime('%H:%M') if t else "09:15"

# #         return JsonResponse({
# #             'risk_reward': getattr(account, f'{prefix}_risk_reward'),
# #             'trailing_sl': getattr(account, f'{prefix}_trailing_sl'),
# #             'total_trades': getattr(account, f'{prefix}_max_trades'),
# #             'trades_per_stock': getattr(account, f'{prefix}_trades_per_stock'),
# #             'risk_trade_1': getattr(account, f'{prefix}_risk_trade_1'),
# #             'risk_trade_2': getattr(account, f'{prefix}_risk_trade_2'),
# #             'risk_trade_3': getattr(account, f'{prefix}_risk_trade_3'),
# #             'start_time': fmt_time(getattr(account, f'{prefix}_start_time')),
# #             'end_time': fmt_time(getattr(account, f'{prefix}_end_time')),
# #             'pnl_exit_enabled': account.pnl_exit_enabled,
# #             'max_profit': account.max_daily_profit,
# #             'max_loss': account.max_daily_loss
# #         })

# # @csrf_exempt
# # @login_required
# # @require_http_methods(["POST"])
# # def control_action(request):
# #     """
# #     Handles Dashboard Buttons: Toggle Engine, Ban Stock, Panic, Individual Exit.
# #     """
# #     if not redis_client: return JsonResponse({'error': 'Redis unavailable'}, 503)

# #     try:
# #         data = json.loads(request.body)
# #         action = data.get('action')
# #         account = get_user_account(request.user)
        
# #         if action == 'toggle_engine':
# #             side = data.get('side')
# #             enabled = data.get('enabled')
# #             key = KEY_ENGINE_BULL_ENABLED if side == 'bull' else KEY_ENGINE_BEAR_ENABLED
# #             val = "1" if enabled else "0"
# #             redis_client.set(key, val)
            
# #             # Sync to DB
# #             if account:
# #                 if side == 'bull': account.is_breakout_cash_active = enabled
# #                 else: account.is_breakdown_cash_active = enabled
# #                 account.save()
# #             return JsonResponse({'status': 'success', 'message': f'{side} engine set to {val}'})

# #         elif action == 'ban_symbol':
# #             symbol = data.get('symbol')
# #             redis_client.sadd(KEY_BLACKLIST, symbol)
# #             return JsonResponse({'status': 'success', 'message': f'{symbol} banned'})

# #         elif action == 'unban_symbol':
# #             symbol = data.get('symbol')
# #             redis_client.srem(KEY_BLACKLIST, symbol)
# #             return JsonResponse({'status': 'success', 'message': f'{symbol} unbanned'})
            
# #         elif action == 'panic_exit':
# #             side = data.get('side')
# #             key = KEY_PANIC_BULL if side == 'bull' else KEY_PANIC_BEAR
# #             redis_client.set(key, "1")
# #             return JsonResponse({'status': 'success', 'message': f'Panic signal sent to {side} engine'})

# #         elif action == 'exit_trade':
# #             # INDIVIDUAL TRADE EXIT LOGIC
# #             symbol = data.get('symbol')
# #             if not symbol: return JsonResponse({'error': 'Symbol required'}, 400)

# #             # We don't know if it's bull or bear, check both
# #             t_bull = CashBreakoutTrade.objects.filter(account=account, symbol=symbol, status='OPEN').first()
# #             if t_bull:
# #                 # Add to set so Bull Engine picks it up in next monitor loop
# #                 redis_client.sadd(f"breakout_exiting_trades:{account.id}_MANUAL", symbol) # Signal
# #                 redis_client.sadd(f"breakout_exiting_trades:{account.id}", t_bull.id) # Immediate filter
# #                 # Ideally, we call a celery task or direct function, 
# #                 # but adding to Redis set is how the Engine loop knows to skip normal logic and process exit.
# #                 # However, the engine needs to perform the order.
# #                 # Since the engine loop is running in a separate process, we need a trigger.
# #                 # The "Panic" key is global. For individual, let's use a specific key pattern the engine watches?
# #                 # OR simpler: The engine's `monitor_trades` loop checks for manual triggers? 
# #                 # Actually, in the provided engine code, `exiting_trades_set` makes it *skip* processing.
# #                 # We need to tell the engine to *actively exit*.
                
# #                 # Correct approach: Push a command to a Redis List the engine consumes, OR simpler for now:
# #                 # Add to a "force_exit" set the engine checks.
# #                 redis_client.sadd(f"breakout_force_exit_requests:{account.id}", str(t_bull.id))
            
# #             t_bear = CashBreakdownTrade.objects.filter(account=account, symbol=symbol, status='OPEN').first()
# #             if t_bear:
# #                 redis_client.sadd(f"breakdown_force_exit_requests:{account.id}", str(t_bear.id))

# #             return JsonResponse({'status': 'success', 'message': f'Exit signal queued for {symbol}'})

# #     except Exception as e:
# #         return JsonResponse({'error': str(e)}, status=400)
    
# #     return JsonResponse({'error': 'Invalid Action'}, status=400)

# # @login_required
# # @require_http_methods(["GET"])
# # def get_orders(request):
# #     """
# #     Returns ONLY Executed Orders (OPEN, CLOSED, PENDING_EXIT).
# #     Hides Failed/Expired/Pending-Entry to keep the table clean.
# #     """
# #     account = get_user_account(request.user)
# #     if not account: return JsonResponse({'bull': [], 'bear': []})

# #     today = dt.now().date()
    
# #     # Filter for executed/active statuses only
# #     valid_statuses = ['OPEN', 'CLOSED', 'PENDING_EXIT']
    
# #     bull_qs = CashBreakoutTrade.objects.filter(account=account, created_at__date=today, status__in=valid_statuses).order_by('-created_at')
# #     bear_qs = CashBreakdownTrade.objects.filter(account=account, created_at__date=today, status__in=valid_statuses).order_by('-created_at')
    
# #     def serialize(qs):
# #         data = []
# #         for t in qs:
# #             data.append({
# #                 'id': t.id,
# #                 'symbol': t.symbol,
# #                 'entry': t.entry_price or t.entry_level,
# #                 'target': t.target_level,
# #                 'sl': t.stop_level,
# #                 'status': t.status,
# #                 'pnl': round(t.pnl, 2),
# #                 'trade_count': 1 
# #             })
# #         return data

# #     return JsonResponse({
# #         'bull': serialize(bull_qs),
# #         'bear': serialize(bear_qs)
# #     })

# # @login_required
# # @require_http_methods(["GET"])
# # def get_scanner_data(request):
# #     """
# #     Returns ALL signals generated today:
# #     PENDING (Waiting for trigger)
# #     EXPIRED (Time/SL invalidation)
# #     FAILED_ENTRY (Order rejected)
    
# #     This ensures the scanner table shows the full history of the day's setups.
# #     """
# #     account = get_user_account(request.user)
# #     if not account: return JsonResponse({'bull': [], 'bear': []})

# #     today = dt.now().date()
    
# #     # Show PENDING, EXPIRED, and FAILED to keep history
# #     display_statuses = ['PENDING', 'EXPIRED', 'FAILED_ENTRY']
    
# #     bull_qs = CashBreakoutTrade.objects.filter(account=account, created_at__date=today, status__in=display_statuses).order_by('-created_at')
# #     bear_qs = CashBreakdownTrade.objects.filter(account=account, created_at__date=today, status__in=display_statuses).order_by('-created_at')

# #     def serialize_scan(qs, signal_label):
# #         data = []
# #         for t in qs:
# #             candle_size_pct = 0.0
# #             if t.candle_close > 0:
# #                 candle_size_pct = ((t.candle_high - t.candle_low) / t.candle_close) * 100
            
# #             ts_str = t.candle_ts.strftime('%H:%M:%S') if t.candle_ts else "--:--:--"

# #             # Append status to signal label for clarity (e.g. "Breakout (Expired)")
# #             strength_text = signal_label
# #             if t.status != 'PENDING':
# #                 strength_text += f" ({t.status})"

# #             data.append({
# #                 'id': t.id,
# #                 'symbol': t.symbol,
# #                 'signal_strength': strength_text,
# #                 'price': t.candle_close,
# #                 'trigger': t.entry_level,
# #                 'time': ts_str,
# #                 'volume_price': f"{t.volume_price:.2f}Cr",
# #                 'candle_size': f"{candle_size_pct:.2f}%",
# #                 'status': t.status # For frontend coloring
# #             })
# #         return data

# #     return JsonResponse({
# #         'bull': serialize_scan(bull_qs, 'Breakout'),
# #         'bear': serialize_scan(bear_qs, 'Breakdown')
# #     })

# import json
# import logging
# import time
# from datetime import datetime as dt
# from django.http import JsonResponse
# from django.views.decorators.csrf import csrf_exempt
# from django.views.decorators.http import require_http_methods
# from django.contrib.auth.decorators import login_required
# from django.shortcuts import redirect, render
# from kiteconnect import KiteConnect
# from .models import Account, CashBreakoutTrade, CashBreakdownTrade
# from .utils import get_redis_connection, get_kite

# logger = logging.getLogger(__name__)
# redis_client = get_redis_connection()

# # --- REDIS KEYS ---
# KEY_ACCESS_TOKEN = "kite:access_token"
# KEY_BULL_SETTINGS = "algo:settings:bull"
# KEY_BEAR_SETTINGS = "algo:settings:bear"
# KEY_ENGINE_BULL_ENABLED = "algo:engine:bull:enabled"
# KEY_ENGINE_BEAR_ENABLED = "algo:engine:bear:enabled"
# KEY_BLACKLIST = "algo:blacklist"
# KEY_PANIC_BULL = "algo:panic:bull"
# KEY_PANIC_BEAR = "algo:panic:bear"
# DATA_HEARTBEAT_KEY = "algo:data:heartbeat"

# def get_user_account(user):
#     return Account.objects.filter(user=user).first()

# def home(request):
#     return render(request, 'trading/dashboard.html')

# @login_required
# def kite_login(request):
#     account = get_user_account(request.user)
#     if not account or not account.api_key:
#         return JsonResponse({'error': 'Account or API Key not configured in Database'}, status=400)
#     kite = KiteConnect(api_key=account.api_key)
#     return redirect(kite.login_url())

# @login_required
# def kite_callback(request):
#     request_token = request.GET.get('request_token')
#     if not request_token:
#         return JsonResponse({'error': 'No request_token received'}, status=400)
#     account = get_user_account(request.user)
#     if not account:
#         return JsonResponse({'error': 'Account not found'}, status=400)
#     try:
#         kite = KiteConnect(api_key=account.api_key)
#         if not account.api_secret:
#              return JsonResponse({'error': 'API Secret not set in Dashboard'}, status=400)
#         data = kite.generate_session(request_token, api_secret=account.api_secret)
#         access_token = data['access_token']
#         account.access_token = access_token
#         account.save()
#         if redis_client: redis_client.set(KEY_ACCESS_TOKEN, access_token)
#         return redirect('/dashboard')
#     except Exception as e:
#         logger.error(f"Error generating Kite session: {e}", exc_info=True)
#         return JsonResponse({'error': f"Kite Auth Failed: {str(e)}"}, status=500)

# @login_required
# @require_http_methods(["GET"])
# def dashboard_stats(request):
#     account = get_user_account(request.user)
#     if not account: return JsonResponse({'error': 'No account'}, status=404)
#     today = dt.now().date()
    
#     bull_realized = sum(t.pnl for t in CashBreakoutTrade.objects.filter(account=account, status='CLOSED', exit_time__date=today))
#     bear_realized = sum(t.pnl for t in CashBreakdownTrade.objects.filter(account=account, status='CLOSED', exit_time__date=today))

#     open_bull = CashBreakoutTrade.objects.filter(account=account, status='OPEN')
#     open_bear = CashBreakdownTrade.objects.filter(account=account, status='OPEN')
    
#     bull_unrealized = 0.0
#     bear_unrealized = 0.0
#     live_data = {}
#     if redis_client:
#         try:
#             raw = redis_client.get('live_ohlc_data')
#             if raw: live_data = json.loads(raw)
#         except: pass

#     for t in open_bull:
#         ltp = float(live_data.get(t.symbol, {}).get('ltp', t.entry_price or 0))
#         if ltp > 0 and t.entry_price: bull_unrealized += (ltp - t.entry_price) * t.quantity

#     for t in open_bear:
#         ltp = float(live_data.get(t.symbol, {}).get('ltp', t.entry_price or 0))
#         if ltp > 0 and t.entry_price: bear_unrealized += (t.entry_price - ltp) * t.quantity

#     last_beat = 0
#     if redis_client:
#         try: last_beat = int(redis_client.get(DATA_HEARTBEAT_KEY) or 0)
#         except: pass
#     data_connected = (time.time() - last_beat) < 15

#     bull_active = redis_client.get(KEY_ENGINE_BULL_ENABLED) or "1" if redis_client else "1"
#     bear_active = redis_client.get(KEY_ENGINE_BEAR_ENABLED) or "1" if redis_client else "1"

#     total_bull_pnl = bull_realized + bull_unrealized
#     total_bear_pnl = bear_realized + bear_unrealized
#     total_global_pnl = total_bull_pnl + total_bear_pnl

#     return JsonResponse({
#         'pnl': {
#             'bull': round(total_bull_pnl, 2),
#             'bear': round(total_bear_pnl, 2),
#             'total': round(total_global_pnl, 2),
#             'unrealized': round(bull_unrealized + bear_unrealized, 2)
#         },
#         'engine_status': {
#             'bull': bull_active.decode() if isinstance(bull_active, bytes) else str(bull_active),
#             'bear': bear_active.decode() if isinstance(bear_active, bytes) else str(bear_active)
#         },
#         'data_connected': data_connected
#     })

# @csrf_exempt
# @login_required
# @require_http_methods(["GET", "POST"])
# def engine_settings_view(request, engine_type):
#     """
#     Syncs Engine Specific Settings (Including SEPARATE VOLUME CRITERIA)
#     between React Dashboard -> Redis -> DB Model.
#     """
#     account = get_user_account(request.user)
#     if not account: return JsonResponse({'error': 'No account'}, 404)
#     if not redis_client: return JsonResponse({'error': 'Redis unavailable'}, 503)

#     redis_key = KEY_BULL_SETTINGS if engine_type == 'bull' else KEY_BEAR_SETTINGS

#     if request.method == "POST":
#         try:
#             payload = json.loads(request.body)
#             # 1. Update Redis (Fast Access for Engines)
#             redis_client.set(redis_key, json.dumps(payload))
            
#             # 2. Update DB Model (Persistence)
#             if engine_type == 'bull':
#                 account.breakout_risk_reward = payload.get('risk_reward', account.breakout_risk_reward)
#                 account.breakout_trailing_sl = payload.get('trailing_sl', account.breakout_trailing_sl)
#                 account.breakout_max_trades = int(payload.get('total_trades', account.breakout_max_trades))
#                 account.breakout_trades_per_stock = int(payload.get('trades_per_stock', account.breakout_trades_per_stock))
#                 account.breakout_risk_trade_1 = float(payload.get('risk_trade_1', account.breakout_risk_trade_1))
#                 account.breakout_risk_trade_2 = float(payload.get('risk_trade_2', account.breakout_risk_trade_2))
#                 account.breakout_risk_trade_3 = float(payload.get('risk_trade_3', account.breakout_risk_trade_3))
#                 if 'start_time' in payload: account.breakout_start_time = payload['start_time']
#                 if 'end_time' in payload: account.breakout_end_time = payload['end_time']
#                 if 'volume_criteria' in payload: account.bull_volume_settings_json = payload['volume_criteria']
#             else:
#                 account.breakdown_risk_reward = payload.get('risk_reward', account.breakdown_risk_reward)
#                 account.breakdown_trailing_sl = payload.get('trailing_sl', account.breakdown_trailing_sl)
#                 account.breakdown_max_trades = int(payload.get('total_trades', account.breakdown_max_trades))
#                 account.breakdown_trades_per_stock = int(payload.get('trades_per_stock', account.breakdown_trades_per_stock))
#                 account.breakdown_risk_trade_1 = float(payload.get('risk_trade_1', account.breakdown_risk_trade_1))
#                 account.breakdown_risk_trade_2 = float(payload.get('risk_trade_2', account.breakdown_risk_trade_2))
#                 account.breakdown_risk_trade_3 = float(payload.get('risk_trade_3', account.breakdown_risk_trade_3))
#                 if 'start_time' in payload: account.breakdown_start_time = payload['start_time']
#                 if 'end_time' in payload: account.breakdown_end_time = payload['end_time']
#                 if 'volume_criteria' in payload: account.bear_volume_settings_json = payload['volume_criteria']
            
#             if 'pnl_exit_enabled' in payload: account.pnl_exit_enabled = payload['pnl_exit_enabled']
#             if 'max_profit' in payload: account.max_daily_profit = float(payload['max_profit'])
#             if 'max_loss' in payload: account.max_daily_loss = float(payload['max_loss'])

#             account.save()
#             return JsonResponse({'status': 'updated', 'engine': engine_type})
#         except Exception as e:
#             return JsonResponse({'error': str(e)}, status=400)

#     # GET: Return merged data from Redis (preferred) or DB
#     data = redis_client.get(redis_key)
#     if data:
#         return JsonResponse(json.loads(data))
#     else:
#         prefix = 'breakout' if engine_type == 'bull' else 'breakdown'
#         vol_settings = account.bull_volume_settings_json if engine_type == 'bull' else account.bear_volume_settings_json
#         def fmt_time(t): return t.strftime('%H:%M') if t else "09:15"

#         return JsonResponse({
#             'risk_reward': getattr(account, f'{prefix}_risk_reward'),
#             'trailing_sl': getattr(account, f'{prefix}_trailing_sl'),
#             'total_trades': getattr(account, f'{prefix}_max_trades'),
#             'trades_per_stock': getattr(account, f'{prefix}_trades_per_stock'),
#             'risk_trade_1': getattr(account, f'{prefix}_risk_trade_1'),
#             'risk_trade_2': getattr(account, f'{prefix}_risk_trade_2'),
#             'risk_trade_3': getattr(account, f'{prefix}_risk_trade_3'),
#             'start_time': fmt_time(getattr(account, f'{prefix}_start_time')),
#             'end_time': fmt_time(getattr(account, f'{prefix}_end_time')),
#             'pnl_exit_enabled': account.pnl_exit_enabled,
#             'max_profit': account.max_daily_profit,
#             'max_loss': account.max_daily_loss,
#             'volume_criteria': vol_settings or []
#         })

# @csrf_exempt
# @login_required
# @require_http_methods(["POST"])
# def control_action(request):
#     if not redis_client: return JsonResponse({'error': 'Redis unavailable'}, 503)
#     try:
#         data = json.loads(request.body)
#         action = data.get('action')
#         account = get_user_account(request.user)
        
#         if action == 'toggle_engine':
#             side = data.get('side')
#             enabled = data.get('enabled')
#             key = KEY_ENGINE_BULL_ENABLED if side == 'bull' else KEY_ENGINE_BEAR_ENABLED
#             val = "1" if enabled else "0"
#             redis_client.set(key, val)
#             if account:
#                 if side == 'bull': account.is_breakout_cash_active = enabled
#                 else: account.is_breakdown_cash_active = enabled
#                 account.save()
#             return JsonResponse({'status': 'success', 'message': f'{side} engine set to {val}'})

#         elif action == 'ban_symbol':
#             symbol = data.get('symbol')
#             redis_client.sadd(KEY_BLACKLIST, symbol)
#             return JsonResponse({'status': 'success', 'message': f'{symbol} banned'})

#         elif action == 'unban_symbol':
#             symbol = data.get('symbol')
#             redis_client.srem(KEY_BLACKLIST, symbol)
#             return JsonResponse({'status': 'success', 'message': f'{symbol} unbanned'})
            
#         elif action == 'panic_exit':
#             side = data.get('side')
#             key = KEY_PANIC_BULL if side == 'bull' else KEY_PANIC_BEAR
#             redis_client.set(key, "1")
#             return JsonResponse({'status': 'success', 'message': f'Panic signal sent to {side} engine'})

#         elif action == 'exit_trade':
#             symbol = data.get('symbol')
#             if not symbol: return JsonResponse({'error': 'Symbol required'}, 400)
#             t_bull = CashBreakoutTrade.objects.filter(account=account, symbol=symbol, status='OPEN').first()
#             if t_bull: redis_client.sadd(f"breakout_force_exit_requests:{account.id}", str(t_bull.id))
#             t_bear = CashBreakdownTrade.objects.filter(account=account, symbol=symbol, status='OPEN').first()
#             if t_bear: redis_client.sadd(f"breakdown_force_exit_requests:{account.id}", str(t_bear.id))
#             return JsonResponse({'status': 'success', 'message': f'Exit signal queued for {symbol}'})

#     except Exception as e:
#         return JsonResponse({'error': str(e)}, status=400)
#     return JsonResponse({'error': 'Invalid Action'}, status=400)

# @login_required
# @require_http_methods(["GET"])
# def get_orders(request):
#     account = get_user_account(request.user)
#     if not account: return JsonResponse({'bull': [], 'bear': []})
#     today = dt.now().date()
#     valid_statuses = ['OPEN', 'CLOSED', 'PENDING_EXIT']
#     bull_qs = CashBreakoutTrade.objects.filter(account=account, created_at__date=today, status__in=valid_statuses).order_by('-created_at')
#     bear_qs = CashBreakdownTrade.objects.filter(account=account, created_at__date=today, status__in=valid_statuses).order_by('-created_at')
    
#     def serialize(qs):
#         data = []
#         for t in qs:
#             data.append({
#                 'id': t.id, 'symbol': t.symbol, 'entry': t.entry_price or t.entry_level,
#                 'target': t.target_level, 'sl': t.stop_level, 'status': t.status,
#                 'pnl': round(t.pnl, 2), 'trade_count': 1, 'quantity': t.quantity
#             })
#         return data
#     return JsonResponse({'bull': serialize(bull_qs), 'bear': serialize(bear_qs)})

# @login_required
# @require_http_methods(["GET"])
# def get_scanner_data(request):
#     account = get_user_account(request.user)
#     if not account: return JsonResponse({'bull': [], 'bear': []})
#     today = dt.now().date()
#     display_statuses = ['PENDING', 'EXPIRED', 'FAILED_ENTRY']
#     bull_qs = CashBreakoutTrade.objects.filter(account=account, created_at__date=today, status__in=display_statuses).order_by('-created_at')
#     bear_qs = CashBreakdownTrade.objects.filter(account=account, created_at__date=today, status__in=display_statuses).order_by('-created_at')

#     def serialize_scan(qs, label):
#         data = []
#         for t in qs:
#             candle_size_pct = 0.0
#             if t.candle_close > 0:
#                 candle_size_pct = ((t.candle_high - t.candle_low) / t.candle_close) * 100
#             ts_str = t.candle_ts.strftime('%H:%M:%S') if t.candle_ts else "--:--:--"
#             strength_text = label if t.status == 'PENDING' else f"{label} ({t.status})"
#             data.append({
#                 'id': t.id, 'symbol': t.symbol, 'signal_strength': strength_text,
#                 'price': t.candle_close, 'trigger': t.entry_level,
#                 'time': ts_str, 'volume_price': f"{t.volume_price:.2f}Cr",
#                 'candle_size': f"{candle_size_pct:.2f}%", 'status': t.status
#             })
#         return data
#     return JsonResponse({'bull': serialize_scan(bull_qs, 'Breakout'), 'bear': serialize_scan(bear_qs, 'Breakdown')})

# # Deprecated Global Settings View that only handles API keys now
# @csrf_exempt
# @login_required
# @require_http_methods(["GET", "POST"])
# def global_settings_view(request):
#     account = get_user_account(request.user)
#     if request.method == "POST":
#         try:
#             payload = json.loads(request.body)
#             if 'api_key' in payload: account.api_key = payload['api_key']
#             if 'api_secret' in payload: account.api_secret = payload['api_secret']
#             account.save()
#             return JsonResponse({'status': 'updated'})
#         except: return JsonResponse({'error': 'Failed'}, status=400)
#     return JsonResponse({"api_key": account.api_key or "", "api_secret": account.api_secret or ""})

import json
import logging
import time
from datetime import datetime as dt
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render
from django.db.models import Q  # <--- IMPORTED Q FOR COMPLEX QUERIES
from kiteconnect import KiteConnect
from .models import Account, CashBreakoutTrade, CashBreakdownTrade
from .utils import get_redis_connection, get_kite

logger = logging.getLogger(__name__)
redis_client = get_redis_connection()

# --- REDIS KEYS ---
KEY_ACCESS_TOKEN = "kite:access_token"
KEY_GLOBAL_SETTINGS = "algo:settings:global"
KEY_BULL_SETTINGS = "algo:settings:bull"
KEY_BEAR_SETTINGS = "algo:settings:bear"
KEY_ENGINE_BULL_ENABLED = "algo:engine:bull:enabled"
KEY_ENGINE_BEAR_ENABLED = "algo:engine:bear:enabled"
KEY_BLACKLIST = "algo:blacklist"
KEY_PANIC_BULL = "algo:panic:bull"
KEY_PANIC_BEAR = "algo:panic:bear"
DATA_HEARTBEAT_KEY = "algo:data:heartbeat"

def get_user_account(user):
    return Account.objects.filter(user=user).first()

def home(request):
    return render(request, 'trading/dashboard.html')

@login_required
def kite_login(request):
    account = get_user_account(request.user)
    if not account or not account.api_key:
        return JsonResponse({'error': 'Account or API Key not configured in Database'}, status=400)
    kite = KiteConnect(api_key=account.api_key)
    return redirect(kite.login_url())

@login_required
def kite_callback(request):
    request_token = request.GET.get('request_token')
    if not request_token:
        return JsonResponse({'error': 'No request_token received'}, status=400)
    account = get_user_account(request.user)
    if not account:
        return JsonResponse({'error': 'Account not found'}, status=400)
    try:
        kite = KiteConnect(api_key=account.api_key)
        if not account.api_secret:
             return JsonResponse({'error': 'API Secret not set in Dashboard'}, status=400)
        data = kite.generate_session(request_token, api_secret=account.api_secret)
        access_token = data['access_token']
        account.access_token = access_token
        account.save()
        if redis_client: redis_client.set(KEY_ACCESS_TOKEN, access_token)
        return redirect('/dashboard')
    except Exception as e:
        logger.error(f"Error generating Kite session: {e}", exc_info=True)
        return JsonResponse({'error': f"Kite Auth Failed: {str(e)}"}, status=500)

# --- DASHBOARD API ---

@login_required
@require_http_methods(["GET"])
def dashboard_stats(request):
    """
    Returns Real-Time P&L (Realized from DB + Unrealized from Live Feed) 
    and Data Continuity Status.
    """
    account = get_user_account(request.user)
    if not account: return JsonResponse({'error': 'No account'}, status=404)

    today = dt.now().date()
    
    # 1. Realized P&L from DB (Closed Trades today)
    bull_realized = sum(t.pnl for t in CashBreakoutTrade.objects.filter(account=account, status='CLOSED', exit_time__date=today))
    bear_realized = sum(t.pnl for t in CashBreakdownTrade.objects.filter(account=account, status='CLOSED', exit_time__date=today))

    # 2. Unrealized P&L from Redis (Calculated Live)
    open_bull = CashBreakoutTrade.objects.filter(account=account, status='OPEN')
    open_bear = CashBreakdownTrade.objects.filter(account=account, status='OPEN')
    
    bull_unrealized = 0.0
    bear_unrealized = 0.0
    live_data = {}
    
    if redis_client:
        try:
            raw = redis_client.get('live_ohlc_data')
            if raw: live_data = json.loads(raw)
        except: pass

    # Calculate Bull Unrealized (LTP - Entry)
    for t in open_bull:
        ltp = float(live_data.get(t.symbol, {}).get('ltp', t.entry_price or 0))
        if ltp > 0 and t.entry_price:
            bull_unrealized += (ltp - t.entry_price) * t.quantity

    # Calculate Bear Unrealized (Entry - LTP)
    for t in open_bear:
        ltp = float(live_data.get(t.symbol, {}).get('ltp', t.entry_price or 0))
        if ltp > 0 and t.entry_price:
            bear_unrealized += (t.entry_price - ltp) * t.quantity

    # 3. Data Heartbeat Check
    last_beat = 0
    if redis_client:
        try: last_beat = int(redis_client.get(DATA_HEARTBEAT_KEY) or 0)
        except: pass
    
    data_connected = (time.time() - last_beat) < 15

    bull_active = redis_client.get(KEY_ENGINE_BULL_ENABLED) or "1" if redis_client else "1"
    bear_active = redis_client.get(KEY_ENGINE_BEAR_ENABLED) or "1" if redis_client else "1"

    total_bull_pnl = bull_realized + bull_unrealized
    total_bear_pnl = bear_realized + bear_unrealized
    total_global_pnl = total_bull_pnl + total_bear_pnl

    return JsonResponse({
        'pnl': {
            'bull': round(total_bull_pnl, 2),
            'bear': round(total_bear_pnl, 2),
            'total': round(total_global_pnl, 2),
            'unrealized': round(bull_unrealized + bear_unrealized, 2)
        },
        'engine_status': {
            'bull': bull_active.decode() if isinstance(bull_active, bytes) else str(bull_active),
            'bear': bear_active.decode() if isinstance(bear_active, bytes) else str(bear_active)
        },
        'data_connected': data_connected
    })

@csrf_exempt
@login_required
@require_http_methods(["GET", "POST"])
def engine_settings_view(request, engine_type):
    account = get_user_account(request.user)
    if not account: return JsonResponse({'error': 'No account'}, 404)
    if not redis_client: return JsonResponse({'error': 'Redis unavailable'}, 503)

    redis_key = KEY_BULL_SETTINGS if engine_type == 'bull' else KEY_BEAR_SETTINGS

    if request.method == "POST":
        try:
            payload = json.loads(request.body)
            redis_client.set(redis_key, json.dumps(payload))
            
            if engine_type == 'bull':
                account.breakout_risk_reward = payload.get('risk_reward', account.breakout_risk_reward)
                account.breakout_trailing_sl = payload.get('trailing_sl', account.breakout_trailing_sl)
                account.breakout_max_trades = int(payload.get('total_trades', account.breakout_max_trades))
                account.breakout_trades_per_stock = int(payload.get('trades_per_stock', account.breakout_trades_per_stock))
                account.breakout_risk_trade_1 = float(payload.get('risk_trade_1', account.breakout_risk_trade_1))
                account.breakout_risk_trade_2 = float(payload.get('risk_trade_2', account.breakout_risk_trade_2))
                account.breakout_risk_trade_3 = float(payload.get('risk_trade_3', account.breakout_risk_trade_3))
                if 'start_time' in payload: account.breakout_start_time = payload['start_time']
                if 'end_time' in payload: account.breakout_end_time = payload['end_time']
                if 'volume_criteria' in payload: account.bull_volume_settings_json = payload['volume_criteria']
            else:
                account.breakdown_risk_reward = payload.get('risk_reward', account.breakdown_risk_reward)
                account.breakdown_trailing_sl = payload.get('trailing_sl', account.breakdown_trailing_sl)
                account.breakdown_max_trades = int(payload.get('total_trades', account.breakdown_max_trades))
                account.breakdown_trades_per_stock = int(payload.get('trades_per_stock', account.breakdown_trades_per_stock))
                account.breakdown_risk_trade_1 = float(payload.get('risk_trade_1', account.breakdown_risk_trade_1))
                account.breakdown_risk_trade_2 = float(payload.get('risk_trade_2', account.breakdown_risk_trade_2))
                account.breakdown_risk_trade_3 = float(payload.get('risk_trade_3', account.breakdown_risk_trade_3))
                if 'start_time' in payload: account.breakdown_start_time = payload['start_time']
                if 'end_time' in payload: account.breakdown_end_time = payload['end_time']
                if 'volume_criteria' in payload: account.bear_volume_settings_json = payload['volume_criteria']
            
            if 'pnl_exit_enabled' in payload: account.pnl_exit_enabled = payload['pnl_exit_enabled']
            if 'max_profit' in payload: account.max_daily_profit = float(payload['max_profit'])
            if 'max_loss' in payload: account.max_daily_loss = float(payload['max_loss'])

            account.save()
            return JsonResponse({'status': 'updated', 'engine': engine_type})
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=400)

    data = redis_client.get(redis_key)
    if data:
        return JsonResponse(json.loads(data))
    else:
        prefix = 'breakout' if engine_type == 'bull' else 'breakdown'
        vol_settings = account.bull_volume_settings_json if engine_type == 'bull' else account.bear_volume_settings_json
        def fmt_time(t): return t.strftime('%H:%M') if t else "09:15"

        return JsonResponse({
            'risk_reward': getattr(account, f'{prefix}_risk_reward'),
            'trailing_sl': getattr(account, f'{prefix}_trailing_sl'),
            'total_trades': getattr(account, f'{prefix}_max_trades'),
            'trades_per_stock': getattr(account, f'{prefix}_trades_per_stock'),
            'risk_trade_1': getattr(account, f'{prefix}_risk_trade_1'),
            'risk_trade_2': getattr(account, f'{prefix}_risk_trade_2'),
            'risk_trade_3': getattr(account, f'{prefix}_risk_trade_3'),
            'start_time': fmt_time(getattr(account, f'{prefix}_start_time')),
            'end_time': fmt_time(getattr(account, f'{prefix}_end_time')),
            'pnl_exit_enabled': account.pnl_exit_enabled,
            'max_profit': account.max_daily_profit,
            'max_loss': account.max_daily_loss,
            'volume_criteria': vol_settings or []
        })

@csrf_exempt
@login_required
@require_http_methods(["POST"])
def control_action(request):
    if not redis_client: return JsonResponse({'error': 'Redis unavailable'}, 503)
    try:
        data = json.loads(request.body)
        action = data.get('action')
        account = get_user_account(request.user)
        
        if action == 'toggle_engine':
            side = data.get('side')
            enabled = data.get('enabled')
            key = KEY_ENGINE_BULL_ENABLED if side == 'bull' else KEY_ENGINE_BEAR_ENABLED
            val = "1" if enabled else "0"
            redis_client.set(key, val)
            if account:
                if side == 'bull': account.is_breakout_cash_active = enabled
                else: account.is_breakdown_cash_active = enabled
                account.save()
            return JsonResponse({'status': 'success'})

        elif action == 'ban_symbol':
            symbol = data.get('symbol')
            redis_client.sadd(KEY_BLACKLIST, symbol)
            return JsonResponse({'status': 'success'})

        elif action == 'unban_symbol':
            symbol = data.get('symbol')
            redis_client.srem(KEY_BLACKLIST, symbol)
            return JsonResponse({'status': 'success'})
            
        elif action == 'panic_exit':
            side = data.get('side')
            key = KEY_PANIC_BULL if side == 'bull' else KEY_PANIC_BEAR
            redis_client.set(key, "1")
            return JsonResponse({'status': 'success'})

        elif action == 'exit_trade':
            symbol = data.get('symbol')
            if not symbol: return JsonResponse({'error': 'Symbol required'}, 400)
            t_bull = CashBreakoutTrade.objects.filter(account=account, symbol=symbol, status='OPEN').first()
            if t_bull: redis_client.sadd(f"breakout_force_exit_requests:{account.id}", str(t_bull.id))
            t_bear = CashBreakdownTrade.objects.filter(account=account, symbol=symbol, status='OPEN').first()
            if t_bear: redis_client.sadd(f"breakdown_force_exit_requests:{account.id}", str(t_bear.id))
            return JsonResponse({'status': 'success'})

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)
    return JsonResponse({'error': 'Invalid Action'}, status=400)

@login_required
@require_http_methods(["GET"])
def get_orders(request):
    """
    Returns ONLY Executed Orders (OPEN, CLOSED, PENDING_EXIT).
    FIX: Uses Q objects to ensure OPEN trades are shown regardless of date.
    """
    account = get_user_account(request.user)
    if not account: return JsonResponse({'bull': [], 'bear': []})

    today = dt.now().date()
    
    # LOGIC FIX: 
    # Show any trade that is currently OPEN or EXITING (regardless of created date)
    # OR any trade that CLOSED today.
    
    bull_qs = CashBreakoutTrade.objects.filter(account=account).filter(
        Q(status__in=['OPEN', 'PENDING_EXIT']) | 
        Q(status='CLOSED', created_at__date=today)
    ).order_by('-created_at')

    bear_qs = CashBreakdownTrade.objects.filter(account=account).filter(
        Q(status__in=['OPEN', 'PENDING_EXIT']) | 
        Q(status='CLOSED', created_at__date=today)
    ).order_by('-created_at')
    
    def serialize(qs):
        data = []
        for t in qs:
            data.append({
                'id': t.id, 'symbol': t.symbol, 'entry': t.entry_price or t.entry_level,
                'target': t.target_level, 'sl': t.stop_level, 'status': t.status,
                'pnl': round(t.pnl, 2), 'trade_count': 1, 'quantity': t.quantity
            })
        return data
    return JsonResponse({'bull': serialize(bull_qs), 'bear': serialize(bear_qs)})

@login_required
@require_http_methods(["GET"])
def get_scanner_data(request):
    account = get_user_account(request.user)
    if not account: return JsonResponse({'bull': [], 'bear': []})
    today = dt.now().date()
    
    display_statuses = ['PENDING', 'EXPIRED', 'FAILED_ENTRY']
    bull_qs = CashBreakoutTrade.objects.filter(account=account, created_at__date=today, status__in=display_statuses).order_by('-created_at')
    bear_qs = CashBreakdownTrade.objects.filter(account=account, created_at__date=today, status__in=display_statuses).order_by('-created_at')

    def serialize_scan(qs, label):
        data = []
        for t in qs:
            candle_size_pct = 0.0
            if t.candle_close > 0:
                candle_size_pct = ((t.candle_high - t.candle_low) / t.candle_close) * 100
            ts_str = t.candle_ts.strftime('%H:%M:%S') if t.candle_ts else "--:--:--"
            strength_text = label if t.status == 'PENDING' else f"{label} ({t.status})"
            data.append({
                'id': t.id, 'symbol': t.symbol, 'signal_strength': strength_text,
                'price': t.candle_close, 'trigger': t.entry_level,
                'time': ts_str, 'volume_price': f"{t.volume_price:.2f}Cr",
                'candle_size': f"{candle_size_pct:.2f}%", 'status': t.status
            })
        return data
    return JsonResponse({'bull': serialize_scan(bull_qs, 'Breakout'), 'bear': serialize_scan(bear_qs, 'Breakdown')})

@csrf_exempt
@login_required
@require_http_methods(["GET", "POST"])
def global_settings_view(request):
    account = get_user_account(request.user)
    if request.method == "POST":
        try:
            payload = json.loads(request.body)
            if 'api_key' in payload: account.api_key = payload['api_key']
            if 'api_secret' in payload: account.api_secret = payload['api_secret']
            account.save()
            return JsonResponse({'status': 'updated'})
        except: return JsonResponse({'error': 'Failed'}, status=400)
    return JsonResponse({"api_key": account.api_key or "", "api_secret": account.api_secret or ""})