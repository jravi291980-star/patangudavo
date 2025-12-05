import json
import logging
import os
from datetime import datetime as dt
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render
from django.conf import settings
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
KEY_BLACKLIST = "algo:blacklist"
KEY_ENGINE_BULL_ENABLED = "algo:engine:bull:enabled"
KEY_ENGINE_BEAR_ENABLED = "algo:engine:bear:enabled"
KEY_PANIC_BULL = "algo:panic:bull"
KEY_PANIC_BEAR = "algo:panic:bear"

# --- HELPER ---
def get_user_account(user):
    return Account.objects.filter(user=user).first()

# --- VIEWS ---

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
        
        # FIX: Use the secret from the Database, not environment variables
        if not account.api_secret:
             return JsonResponse({'error': 'API Secret not set in Dashboard'}, status=400)

        data = kite.generate_session(request_token, api_secret=account.api_secret)
        access_token = data['access_token']

        # 1. Store in Database
        account.access_token = access_token
        account.save()

        # 2. Store in Redis
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
    account = get_user_account(request.user)
    if not account: return JsonResponse({'error': 'No account'}, status=404)

    today = dt.now().date()
    
    bull_pnl = sum(t.pnl for t in CashBreakoutTrade.objects.filter(account=account, status='CLOSED', exit_time__date=today))
    bear_pnl = sum(t.pnl for t in CashBreakdownTrade.objects.filter(account=account, status='CLOSED', exit_time__date=today))

    bull_active = redis_client.get(KEY_ENGINE_BULL_ENABLED) or "0" if redis_client else "0"
    bear_active = redis_client.get(KEY_ENGINE_BEAR_ENABLED) or "0" if redis_client else "0"

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
    """
    Handles Global Settings:
    1. API Keys (Saved to Postgres/DB)
    2. Global Strategy Settings (Saved to Redis)
    """
    # Get user account for DB operations
    account = get_user_account(request.user)

    if request.method == "GET":
        # 1. Fetch Redis Data
        redis_data = {}
        if redis_client:
            raw_data = redis_client.get(KEY_GLOBAL_SETTINGS)
            if raw_data:
                redis_data = json.loads(raw_data)
        
        # 2. Fetch DB Data (API Keys)
        db_data = {
            "api_key": account.api_key if account else "",
            "api_secret": account.api_secret if account else "",
        }

        # Merge and return
        return JsonResponse({**redis_data, **db_data})
    
    elif request.method == "POST":
        try:
            payload = json.loads(request.body)
            
            # 1. SAVE TO DB (API Keys)
            if account:
                if 'api_key' in payload:
                    account.api_key = payload['api_key']
                if 'api_secret' in payload:
                    account.api_secret = payload['api_secret']
                account.save()

            # 2. SAVE TO REDIS (Strategy Settings)
            # Remove keys we stored in DB so we don't duplicate them in Redis needlessly
            redis_payload = payload.copy()
            redis_payload.pop('api_key', None)
            redis_payload.pop('api_secret', None)
            
            if redis_client and redis_payload:
                # Merge with existing redis data to prevent overwriting other fields
                existing_data = redis_client.get(KEY_GLOBAL_SETTINGS)
                current_settings = json.loads(existing_data) if existing_data else {}
                current_settings.update(redis_payload)
                
                redis_client.set(KEY_GLOBAL_SETTINGS, json.dumps(current_settings))

            return JsonResponse({'status': 'updated', 'message': 'Settings saved successfully'})
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=400)

@csrf_exempt
@login_required
@require_http_methods(["GET", "POST"])
def engine_settings_view(request, engine_type):
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
    if not redis_client:
         return JsonResponse({'error': 'Redis not available'}, status=503)

    try:
        data = json.loads(request.body)
        action = data.get('action')
        
        if action == 'toggle_engine':
            side = data.get('side')
            enabled = data.get('enabled')
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
            side = data.get('side')
            key = KEY_PANIC_BULL if side == 'bull' else KEY_PANIC_BEAR
            redis_client.set(key, "1")
            return JsonResponse({'status': 'success', 'message': f'Panic signal sent to {side} engine'})

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)

@login_required
@require_http_methods(["GET"])
def get_orders(request):
    account = get_user_account(request.user)
    if not account: return JsonResponse({'bull': [], 'bear': []})

    today = dt.now().date()
    
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
    account = get_user_account(request.user)
    if not account:
        return JsonResponse({'bull': [], 'bear': []})

    today = dt.now().date()
    
    bull_qs = CashBreakoutTrade.objects.filter(account=account, status='PENDING', created_at__date=today).order_by('-created_at')
    bear_qs = CashBreakdownTrade.objects.filter(account=account, status='PENDING', created_at__date=today).order_by('-created_at')

    def serialize_scan(qs, signal_label):
        data = []
        for t in qs:
            candle_size_pct = 0.0
            if t.candle_close > 0:
                candle_size_pct = ((t.candle_high - t.candle_low) / t.candle_close) * 100
            
            ts_str = t.candle_ts.strftime('%H:%M:%S') if t.candle_ts else "--:--:--"

            data.append({
                'id': t.id,
                'symbol': t.symbol,
                'signal_strength': signal_label,
                'price': t.candle_close,
                'trigger': t.entry_level,
                'time': ts_str,
                'volume_price': f"{t.volume_price:.2f}Cr",
                'candle_size': f"{candle_size_pct:.2f}%"
            })
        return data

    return JsonResponse({
        'bull': serialize_scan(bull_qs, 'Breakout'),
        'bear': serialize_scan(bear_qs, 'Breakdown')
    })