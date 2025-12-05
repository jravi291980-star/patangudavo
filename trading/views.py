import json
import logging
import os
from datetime import datetime as dt
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect
from django.conf import settings
from kiteconnect import KiteConnect # Import KiteConnect directly for Login flow
from .models import Account, CashBreakoutTrade, CashBreakdownTrade
from .utils import get_redis_connection, get_kite # Updated imports

logger = logging.getLogger(__name__)
redis_client = get_redis_connection()

# --- REDIS KEYS ---
KEY_ACCESS_TOKEN = "kite:access_token"
KEY_GLOBAL_SETTINGS = "algo:settings:global"
KEY_BULL_SETTINGS = "algo:settings:bull"
KEY_BEAR_SETTINGS = "algo:settings:bear"
KEY_BLACKLIST = "algo:blacklist"
KEY_ENGINE_BULL_ENABLED = "algo:engine:bull:enabled"
KEY_ENGINE_BEAR_ENABLED = "algo:engine:bear:enabled"
KEY_PANIC_BULL = "algo:panic:bull"
KEY_PANIC_BEAR = "algo:panic:bear"

# --- HELPER ---
def get_user_account(user):
    return Account.objects.filter(user=user).first()

# --- KITE AUTHENTICATION FLOW ---

@login_required
def kite_login(request):
    """
    Initiates the Kite Connect login flow.
    Redirects user to Zerodha's login page.
    """
    account = get_user_account(request.user)
    if not account or not account.api_key:
        return JsonResponse({'error': 'Account or API Key not configured in Database'}, status=400)

    # Instantiate KiteConnect directly since we don't have an access_token yet
    kite = KiteConnect(api_key=account.api_key)
    
    # The redirect_url should match what you set in the Kite Developer Console
    return redirect(kite.login_url())
from django.http import HttpResponse

# Add this function
def home(request):
    return HttpResponse("<h1>Algo Trading Bot is Active</h1><p>Worker is running.</p>")
@login_required
def kite_callback(request):
    """
    Handles the callback from Zerodha.
    Exchanges request_token for access_token and stores it.
    """
    request_token = request.GET.get('request_token')
    if not request_token:
        return JsonResponse({'error': 'No request_token received'}, status=400)

    account = get_user_account(request.user)
    if not account:
        return JsonResponse({'error': 'Account not found'}, status=400)

    try:
        # Instantiate KiteConnect directly to exchange token
        kite = KiteConnect(api_key=account.api_key)
        
        # NOTE: Ideally, API_SECRET should be in environment variables or settings
        api_secret = getattr(settings, 'KITE_API_SECRET', os.environ.get('KITE_API_SECRET'))
        
        if not api_secret:
             logger.error("KITE_API_SECRET is missing in environment variables.")
             return JsonResponse({'error': 'Server misconfiguration: API_SECRET missing'}, status=500)

        data = kite.generate_session(request_token, api_secret=api_secret)
        access_token = data['access_token']

        # 1. Store in Database (Persistent)
        account.access_token = access_token
        account.save()

        # 2. Store in Redis (Fast access for Engine)
        if redis_client:
            redis_client.set(KEY_ACCESS_TOKEN, access_token)
        
        logger.info(f"Access token generated successfully for user {request.user.username}")
        
        return redirect('/dashboard')
        
    except Exception as e:
        logger.error(f"Error generating Kite session: {e}", exc_info=True)
        return JsonResponse({'error': f"Kite Auth Failed: {str(e)}"}, status=500)


# --- DASHBOARD API ---

@login_required
@require_http_methods(["GET"])
def dashboard_stats(request):
    """Returns P&L stats and Engine Status."""
    account = get_user_account(request.user)
    if not account: return JsonResponse({'error': 'No account'}, status=404)

    today = dt.now().date()
    
    # Calculate P&L from DB (Closed Trades)
    bull_pnl = 0.0
    for t in CashBreakoutTrade.objects.filter(account=account, status='CLOSED', exit_time__date=today):
        bull_pnl += t.pnl
        
    bear_pnl = 0.0
    for t in CashBreakdownTrade.objects.filter(account=account, status='CLOSED', exit_time__date=today):
        bear_pnl += t.pnl

    # Fetch Engine Status from Redis
    bull_active = "0"
    bear_active = "0"
    
    if redis_client:
        bull_active = redis_client.get(KEY_ENGINE_BULL_ENABLED) or "0"
        bear_active = redis_client.get(KEY_ENGINE_BEAR_ENABLED) or "0"

    return JsonResponse({
        'pnl': {
            'bull': round(bull_pnl, 2),
            'bear': round(bear_pnl, 2),
            'total': round(bull_pnl + bear_pnl, 2)
        },
        'engine_status': {
            'bull': bull_active,
            'bear': bear_active
        },
        'sentiment': {'percentage': 0, 'isBullish': True} 
    })

@csrf_exempt
@login_required
@require_http_methods(["GET", "POST"])
def global_settings_view(request):
    """Get or Update Global Volume/SMA Settings."""
    if not redis_client:
         return JsonResponse({'error': 'Redis not available'}, status=503)

    if request.method == "GET":
        data = redis_client.get(KEY_GLOBAL_SETTINGS)
        settings_data = json.loads(data) if data else {"volume_criteria": []}
        return JsonResponse(settings_data)
    
    elif request.method == "POST":
        try:
            payload = json.loads(request.body)
            redis_client.set(KEY_GLOBAL_SETTINGS, json.dumps(payload))
            return JsonResponse({'status': 'updated', 'data': payload})
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=400)

@csrf_exempt
@login_required
@require_http_methods(["GET", "POST"])
def engine_settings_view(request, engine_type):
    """Get or Update Bull/Bear Engine Settings (Risk, TSL, etc)."""
    if not redis_client:
         return JsonResponse({'error': 'Redis not available'}, status=503)

    redis_key = KEY_BULL_SETTINGS if engine_type == 'bull' else KEY_BEAR_SETTINGS
    
    if request.method == "GET":
        data = redis_client.get(redis_key)
        defaults = {
            "risk_reward": "1:2", "trailing_sl": "1:1", "total_trades": 5, "trades_per_stock": 2,
            "risk_trade_1": 2000, "risk_trade_2": 1500, "risk_trade_3": 1000,
            "pnl_exit_enabled": False, "max_profit": 5000, "max_loss": 2000
        }
        settings_data = json.loads(data) if data else defaults
        return JsonResponse(settings_data)

    elif request.method == "POST":
        try:
            payload = json.loads(request.body)
            redis_client.set(redis_key, json.dumps(payload))
            return JsonResponse({'status': 'updated', 'engine': engine_type})
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=400)

@csrf_exempt
@login_required
@require_http_methods(["POST"])
def control_action(request):
    """Handle Ban, Engine Toggle, and Panic Button."""
    if not redis_client:
         return JsonResponse({'error': 'Redis not available'}, status=503)

    try:
        data = json.loads(request.body)
        action = data.get('action')
        
        if action == 'toggle_engine':
            side = data.get('side') # 'bull' or 'bear'
            enabled = data.get('enabled') # boolean
            key = KEY_ENGINE_BULL_ENABLED if side == 'bull' else KEY_ENGINE_BEAR_ENABLED
            val = "1" if enabled else "0"
            redis_client.set(key, val)
            return JsonResponse({'status': 'success', 'message': f'{side} engine set to {val}'})

        elif action == 'ban_symbol':
            symbol = data.get('symbol')
            redis_client.sadd(KEY_BLACKLIST, symbol)
            return JsonResponse({'status': 'success', 'message': f'{symbol} banned'})

        elif action == 'unban_symbol':
            symbol = data.get('symbol')
            redis_client.srem(KEY_BLACKLIST, symbol)
            return JsonResponse({'status': 'success', 'message': f'{symbol} unbanned'})
            
        elif action == 'panic_exit':
            side = data.get('side') # 'bull' or 'bear'
            key = KEY_PANIC_BULL if side == 'bull' else KEY_PANIC_BEAR
            redis_client.set(key, "1")
            return JsonResponse({'status': 'success', 'message': f'Panic signal sent to {side} engine'})

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)

@login_required
@require_http_methods(["GET"])
def get_orders(request):
    """Fetch recent orders for the OrderTable component."""
    account = get_user_account(request.user)
    if not account: return JsonResponse({'bull': [], 'bear': []})

    today = dt.now().date()
    
    # Fetch Bull Trades
    bull_qs = CashBreakoutTrade.objects.filter(account=account, created_at__date=today).order_by('-created_at')
    bear_qs = CashBreakdownTrade.objects.filter(account=account, created_at__date=today).order_by('-created_at')
    
    def serialize(qs):
        data = []
        for t in qs:
            data.append({
                'id': t.id,
                'symbol': t.symbol,
                'entry': t.entry_level,
                'target': t.target_level,
                'sl': t.stop_level,
                'status': t.status,
                'pnl': round(t.pnl, 2),
                'trade_count': 1 
            })
        return data

    return JsonResponse({
        'bull': serialize(bull_qs),
        'bear': serialize(bear_qs)
    })

@login_required
@require_http_methods(["GET"])
def get_scanner_data(request):
    """
    Fetch live scanner data. 
    Returns pending trades (setups) found by Bull and Bear engines.
    """
    account = get_user_account(request.user)
    if not account:
        return JsonResponse({'bull': [], 'bear': []})

    today = dt.now().date()
    
    # Fetch Pending Bull Trades (Potential Breakouts)
    bull_qs = CashBreakoutTrade.objects.filter(
        account=account, 
        status='PENDING',
        created_at__date=today
    ).order_by('-created_at')
    
    # Fetch Pending Bear Trades (Potential Breakdowns)
    bear_qs = CashBreakdownTrade.objects.filter(
        account=account, 
        status='PENDING',
        created_at__date=today
    ).order_by('-created_at')

    def serialize_scan(qs, signal_label):
        data = []
        for t in qs:
            # Calculate Candle Size %
            candle_size_pct = 0.0
            if t.candle_close > 0:
                candle_size_pct = ((t.candle_high - t.candle_low) / t.candle_close) * 100
            
            # Format timestamp
            ts_str = t.candle_ts.strftime('%H:%M:%S') if t.candle_ts else "--:--:--"

            data.append({
                'id': t.id,
                'symbol': t.symbol,
                'signal_strength': signal_label,
                'price': t.candle_close,       # Price at time of scan
                'trigger': t.entry_level,      # Trigger Level
                'time': ts_str,
                'volume_price': f"{t.volume_price:.2f}Cr",
                'candle_size': f"{candle_size_pct:.2f}%"
            })
        return data

    return JsonResponse({
        'bull': serialize_scan(bull_qs, 'Breakout'),
        'bear': serialize_scan(bear_qs, 'Breakdown')
    })