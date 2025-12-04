import json
import logging
from datetime import datetime as dt
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.contrib.auth.decorators import login_required
from django.conf import settings
from .models import Account, CashBreakoutTrade, CashBreakdownTrade
from .utils import get_redis_connection

logger = logging.getLogger(__name__)
redis_client = get_redis_connection()

# --- REDIS KEYS (Must match Engine/Websocket) ---
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

@login_required
@require_http_methods(["GET"])
def dashboard_stats(request):
    """Returns P&L stats and Market Sentiment for the header."""
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

    # Redis P&L (Real-time sync if needed, but DB is safer for "Realized")
    # You could pull the 'live' pnl from redis if the engine writes it there frequently.

    return JsonResponse({
        'pnl': {
            'bull': round(bull_pnl, 2),
            'bear': round(bear_pnl, 2),
            'total': round(bull_pnl + bear_pnl, 2)
        },
        # Sentiment logic would go here if you calculate it elsewhere
        'sentiment': {'percentage': 0, 'isBullish': True} 
    })

@csrf_exempt
@login_required
@require_http_methods(["GET", "POST"])
def global_settings_view(request):
    """Get or Update Global Volume/SMA Settings."""
    if request.method == "GET":
        data = redis_client.get(KEY_GLOBAL_SETTINGS)
        settings_data = json.loads(data) if data else {"volume_criteria": []}
        return JsonResponse(settings_data)
    
    elif request.method == "POST":
        try:
            payload = json.loads(request.body)
            # Validate payload structure if necessary
            redis_client.set(KEY_GLOBAL_SETTINGS, json.dumps(payload))
            return JsonResponse({'status': 'updated', 'data': payload})
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=400)

@csrf_exempt
@login_required
@require_http_methods(["GET", "POST"])
def engine_settings_view(request, engine_type):
    """Get or Update Bull/Bear Engine Settings (Risk, TSL, etc)."""
    redis_key = KEY_BULL_SETTINGS if engine_type == 'bull' else KEY_BEAR_SETTINGS
    
    if request.method == "GET":
        data = redis_client.get(redis_key)
        # Return defaults if empty
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
            redis_client.set(key, "1") # Trigger for the loop
            return JsonResponse({'status': 'success', 'message': f'Panic signal sent to {side} engine'})

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)

@login_required
@require_http_methods(["GET"])
def get_orders(request):
    """Fetch recent orders for the OrderTable component."""
    account = get_user_account(request.user)
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
                'entry': t.entry_level, # or t.entry_price if filled
                'target': t.target_level,
                'sl': t.stop_level,
                'status': t.status,
                'pnl': round(t.pnl, 2),
                'trade_count': 1 # Placeholder, complex logic needed to calc daily trade count per stock
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
    Ideally, scanner data is pushed via Websocket to frontend.
    If polling, we read from Redis 'active_entries_set' or similar keys populated by engine.
    """
    # This is a simplified example. You might need to fetch details from the 'pending_trades' 
    # stored in DB or Redis.
    
    # For now, let's return empty or mock, assuming frontend gets this via direct socket or different mechanism
    # logic depends on how you want to expose "Potential Breakouts" vs "Active Trades"
    return JsonResponse({'bull': [], 'bear': []})