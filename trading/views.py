import json
import redis
import logging
from datetime import datetime, time
from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_exempt
from kiteconnect import KiteConnect
from .models import Account, CashBreakoutTrade, CashBreakdownTrade, MomentumBullTrade, MomentumBearTrade
from .forms import AccountForm

# --- Redis Connection (Nexus ke saath consistent keys) ---
r = redis.from_url(settings.REDIS_URL, decode_responses=True)
logger = logging.getLogger("Views")

def home(request):
    """Dashboard render karega"""
    account = Account.objects.first()
    return render(request, 'dashboard.html', {'account': account})

# --- Kite Connect Authentication ---

def kite_login(request):
    """Zerodha login page par redirect karega"""
    acc = Account.objects.first()
    kite = KiteConnect(api_key=acc.api_key)
    return redirect(kite.login_url())

def kite_callback(request):
    """Zerodha se access token lekar DB mein save karega"""
    request_token = request.GET.get('request_token')
    acc = Account.objects.first()
    kite = KiteConnect(api_key=acc.api_key)
    
    try:
        data = kite.generate_session(request_token, api_secret=acc.api_secret)
        acc.access_token = data['access_token']
        acc.save()
        # Nexus ko naya token milne ke liye restart ki zarurat hogi agar wo chalu hai
        return redirect('/dashboard/')
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)})

# --- Dashboard API Endpoints ---

def dashboard_stats(request):
    """PnL, Heartbeat aur Engine status supply karega"""
    # 1. Heartbeat check (Nexus har 1s update karta hai)
    last_heartbeat = r.get("algo:data:heartbeat")
    is_live = False
    if last_heartbeat:
        if int(datetime.now().timestamp()) - int(last_heartbeat) < 10:
            is_live = True

    # 2. Engine status sync
    status = {
        'bull': r.get("algo:engine:bull:enabled") or "0",
        'bear': r.get("algo:engine:bear:enabled") or "0",
        'mom_bull': r.get("algo:engine:mom_bull:enabled") or "0",
        'mom_bear': r.get("algo:engine:mom_bear:enabled") or "0",
    }

    # 3. Simple P&L Aggregation from DB (Mock logic)
    pnl_data = {
        'total': 0.00,
        'bull': 0.00,
        'bear': 0.00,
        'mom_bull': 0.00,
        'mom_bear': 0.00
    }

    return JsonResponse({
        'data_connected': is_live,
        'engine_status': status,
        'pnl': pnl_data
    })

@csrf_exempt
def engine_settings_view(request, engine_type):
    """Engine specific settings (RR, Trailing, Vol) handle karega"""
    redis_key = f"algo:settings:{engine_type}"
    
    if request.method == 'GET':
        # Redis se settings uthayein
        data = r.get(redis_key)
        return JsonResponse(json.loads(data) if data else {})
    
    elif request.method == 'POST':
        # Dashboard se aayi nayi settings Redis mein save karein
        try:
            settings_data = json.loads(request.body)
            r.set(redis_key, json.dumps(settings_data))
            # Nexus background poller ise automatically RAM mein cache kar lega
            return JsonResponse({'status': 'success'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)})

@csrf_exempt
def control_action(request):
    """Buttons (Toggle, Panic, Ban) handle karega"""
    data = json.loads(request.body)
    action = data.get('action')
    
    if action == 'toggle_engine':
        side = data.get('side') # bull, bear, etc.
        enabled = "1" if data.get('enabled') else "0"
        r.set(f"algo:engine:{side}:enabled", enabled)
        return JsonResponse({'status': 'success'})

    elif action == 'panic_exit':
        side = data.get('side')
        r.set(f"algo:panic:{side}", "1") # Nexus ise check karke square off kar dega
        return JsonResponse({'status': 'success'})

    elif action == 'ban_symbol':
        symbol = data.get('symbol')
        r.sadd("algo:banned_symbols", symbol) # Redis set mein add karein
        return JsonResponse({'status': 'success'})

    return JsonResponse({'status': 'invalid_action'})

def get_orders(request):
    """Active aur Closed orders database se fetch karega"""
    # Nexus async worker in models mein data fill karta hai
    orders = {
        'bull': list(CashBreakoutTrade.objects.order_by('-id')[:5].values()),
        'bear': list(CashBreakdownTrade.objects.order_by('-id')[:5].values()),
        'mom_bull': list(MomentumBullTrade.objects.order_by('-id')[:5].values()),
        'mom_bear': list(MomentumBearTrade.objects.order_by('-id')[:5].values()),
    }
    return JsonResponse(orders)

def get_scanner_data(request):
    """Nexus jo signals Redis mein push karta hai unhe fetch karega"""
    # Nexus 'algo:scanner:signals' key mein JSON list maintain karega
    raw_signals = r.get("algo:scanner:signals")
    signals = json.loads(raw_signals) if raw_signals else []
    
    # Dashboard panels ke hisab se filter karein (Simple mock logic)
    return JsonResponse({
        'bull': [s for s in signals if s['type'] == 'BULL'],
        'bear': [s for s in signals if s['type'] == 'BEAR'],
        'mom_bull': [s for s in signals if s['type'] == 'MOM_BULL'],
        'mom_bear': [s for s in signals if s['type'] == 'MOM_BEAR'],
    })