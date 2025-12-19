import json
import logging
from datetime import datetime
from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import login_required
from kiteconnect import KiteConnect
from django.conf import settings

from .models import Account, CashBreakoutTrade, CashBreakdownTrade, MomentumBullTrade, MomentumBearTrade
from .hft_utils import get_redis_client

logger = logging.getLogger("TradingViews")

# --- DASHBOARD RENDER ---

@login_required
def home(request):
    """Main Dashboard render karega"""
    account = Account.objects.first()
    return render(request, 'trading/dashboard.html', {'account': account})

# --- ZERODHA / KITE AUTHENTICATION ---

@login_required
def kite_login(request):
    """Zerodha login page par redirect karega"""
    acc = Account.objects.first()
    if not acc or not acc.api_key:
        return JsonResponse({'status': 'error', 'message': 'API Key not found in Account model'})
    
    kite = KiteConnect(api_key=acc.api_key)
    return redirect(kite.login_url())

@login_required
def kite_callback(request):
    """Zerodha se access token lekar DB mein save karega"""
    request_token = request.GET.get('request_token')
    acc = Account.objects.first()
    kite = KiteConnect(api_key=acc.api_key)
    
    try:
        data = kite.generate_session(request_token, api_secret=acc.api_secret)
        acc.access_token = data['access_token']
        acc.save()
        # Redis mein update
        r = get_redis_client()
        r.set(f"hft:access_token:{acc.user.id}", data['access_token'])
        return redirect('/')
    except Exception as e:
        logger.error(f"Kite Login Failed: {e}")
        return JsonResponse({'status': 'error', 'message': str(e)})

# --- HFT API ENDPOINTS (Names Matched to URLs) ---

@login_required
def dashboard_stats(request):
    """PnL, Heartbeat aur Engine status supply karega"""
    r = get_redis_client()
    last_heartbeat = r.get("algo:data:heartbeat")
    
    is_live = False
    if last_heartbeat and not isinstance(r, object): # Mock check
        try:
            if int(datetime.now().timestamp()) - int(last_heartbeat) < 15:
                is_live = True
        except: pass

    status = {
        'bull': r.get("algo:engine:bull:enabled") or "0",
        'bear': r.get("algo:engine:bear:enabled") or "0",
        'mom_bull': r.get("algo:engine:mom_bull:enabled") or "0",
        'mom_bear': r.get("algo:engine:mom_bear:enabled") or "0",
    }

    pnl_data = {'total': 0.0, 'bull': 0.0, 'bear': 0.0, 'mom_bull': 0.0, 'mom_bear': 0.0}

    return JsonResponse({
        'data_connected': is_live,
        'engine_status': status,
        'pnl': pnl_data
    })

@csrf_exempt
@login_required
def control_action(request):
    """Buttons (Toggle, Panic, Ban, Exit) handle karega"""
    if request.method != "POST":
        return JsonResponse({"status": "failed"}, status=400)
        
    data = json.loads(request.body)
    action = data.get('action')
    r = get_redis_client()
    
    if action == 'toggle_engine':
        side = data.get('side')
        enabled = "1" if data.get('enabled') else "0"
        r.set(f"algo:engine:{side}:enabled", enabled)
        return JsonResponse({'status': 'success', 'side': side, 'enabled': enabled})

    elif action == 'panic_exit':
        side = data.get('side')
        r.set(f"algo:panic:{side}", "1", ex=60) 
        return JsonResponse({'status': 'panic_signal_sent'})

    elif action == 'manual_exit':
        symbol = data.get('symbol')
        r.set(f"algo:manual_exit:{symbol}", "1", ex=20)
        return JsonResponse({'status': 'exit_sent', 'symbol': symbol})

    elif action == 'ban_symbol':
        symbol = data.get('symbol').upper()
        r.sadd("algo:banned_symbols", symbol)
        return JsonResponse({'status': 'success', 'banned': symbol})

    return JsonResponse({'status': 'invalid_action'})

@csrf_exempt
@login_required
def engine_settings_view(request, side):
    """Engine specific settings (Matrix, RR, Trailing)"""
    r = get_redis_client()
    redis_key = f"algo:settings:{side}"
    
    if request.method == 'POST':
        try:
            settings_data = json.loads(request.body)
            r.set(redis_key, json.dumps(settings_data))
            acc = Account.objects.get(user=request.user)
            if side == 'bull': acc.bull_volume_settings_json = json.dumps(settings_data)
            elif side == 'bear': acc.bear_volume_settings_json = json.dumps(settings_data)
            acc.save()
            return JsonResponse({'status': 'success'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)})

    data = r.get(redis_key)
    return JsonResponse(json.loads(data) if data else {})

@login_required
def get_orders(request):
    """Active aur Closed orders database se fetch karega"""
    orders = {
        'bull': list(CashBreakoutTrade.objects.filter(user=request.user).order_by('-id')[:10].values()),
        'bear': list(CashBreakdownTrade.objects.filter(user=request.user).order_by('-id')[:10].values()),
        'mom_bull': list(MomentumBullTrade.objects.filter(user=request.user).order_by('-id')[:10].values()),
        'mom_bear': list(MomentumBearTrade.objects.filter(user=request.user).order_by('-id')[:10].values()),
    }
    return JsonResponse(orders)

@login_required
def get_scanner_data(request):
    """Nexus signals fetch karega"""
    r = get_redis_client()
    raw_signals = r.get("algo:scanner:signals")
    signals = json.loads(raw_signals) if raw_signals else []
    
    return JsonResponse({
        'bull': [s for s in signals if s.get('type') == 'BULL'],
        'bear': [s for s in signals if s.get('type') == 'BEAR'],
        'mom_bull': [s for s in signals if s.get('type') == 'MOM_BULL'],
        'mom_bear': [s for s in signals if s.get('type') == 'MOM_BEAR'],
    })

@csrf_exempt
@login_required
def global_settings_view(request):
    """Global configurations handler"""
    return JsonResponse({"status": "global_config_active"})