import json
import logging
from datetime import datetime
from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import login_required
from django.db.models import Sum
from kiteconnect import KiteConnect
from django.conf import settings

from .models import (
    Account, CashBreakoutTrade, CashBreakdownTrade, 
    MomentumBullTrade, MomentumBearTrade, BannedSymbol
)
from .hft_utils import get_redis_client

logger = logging.getLogger("TradingViews")

# --- DASHBOARD RENDER ---

@login_required
def home(request):
    """Main Dashboard render karega - Template path is 'trading/dashboard.html'"""
    account = Account.objects.filter(user=request.user).first()
    return render(request, 'trading/dashboard.html', {'account': account})

# --- ZERODHA / KITE AUTHENTICATION ---

@login_required
def kite_login(request):
    """Zerodha login page par redirect karega"""
    acc = Account.objects.filter(user=request.user).first()
    if not acc or not acc.api_key:
        return JsonResponse({'status': 'error', 'message': 'API Details missing! Modal me API Key bhariye.'})
    
    kite = KiteConnect(api_key=acc.api_key)
    return redirect(kite.login_url())

@login_required
def kite_callback(request):
    """Zerodha se access token lekar DB mein save karega"""
    request_token = request.GET.get('request_token')
    acc = Account.objects.filter(user=request.user).first()
    if not acc:
        return redirect('/?error=no_account')
        
    kite = KiteConnect(api_key=acc.api_key)
    
    try:
        data = kite.generate_session(request_token, api_secret=acc.api_secret)
        acc.access_token = data['access_token']
        acc.save()
        
        # Redis sync for Engines to pick up the new token immediately
        r = get_redis_client()
        r.set(f"hft:access_token:{acc.user.id}", data['access_token'])
        return redirect('/')
    except Exception as e:
        logger.error(f"Kite Login Failed: {e}")
        return redirect('/?error=login_failed')

# --- HFT API ENDPOINTS ---

@login_required
def dashboard_stats(request):
    """Live PnL, Heartbeat aur Sentiment supply karega"""
    r = get_redis_client()
    
    # 1. Data Connection Check (Heartbeat from Nexus Engines)
    hb = r.get("algo:data:heartbeat")
    is_live = False
    if hb:
        try:
            if int(datetime.now().timestamp()) - int(hb) < 15:
                is_live = True
        except: pass

    # 2. Engine Status Toggles
    status = {
        'bull': r.get("algo:engine:bull:enabled") or "0",
        'bear': r.get("algo:engine:bear:enabled") or "0",
        'mom_bull': r.get("algo:engine:mom_bull:enabled") or "0",
        'mom_bear': r.get("algo:engine:mom_bear:enabled") or "0",
    }

    # 3. Live P&L Aggregation from DB
    def get_pnl(model):
        res = model.objects.filter(user=request.user).aggregate(Sum('pnl'))['pnl__sum']
        return float(res) if res else 0.0

    pnl_data = {
        'bull': get_pnl(CashBreakoutTrade),
        'bear': get_pnl(CashBreakdownTrade),
        'mom_bull': get_pnl(MomentumBullTrade),
        'mom_bear': get_pnl(MomentumBearTrade),
    }
    pnl_data['total'] = sum(pnl_data.values())

    # 4. Sentiment Calculation (Based on scanner signals)
    raw_signals = r.get("algo:scanner:signals")
    signals = json.loads(raw_signals) if raw_signals else []
    bull_count = len([s for s in signals if 'BULL' in s.get('type', '')])
    bear_count = len([s for s in signals if 'BEAR' in s.get('type', '')])
    
    sentiment_pct = 0
    if (bull_count + bear_count) > 0:
        sentiment_pct = int((bull_count / (bull_count + bear_count)) * 100)
    
    return JsonResponse({
        'data_connected': is_live,
        'engine_status': status,
        'pnl': pnl_data,
        'sentiment': {
            'bull': bull_count,
            'bear': bear_count,
            'pct': sentiment_pct
        }
    })

@csrf_exempt
@login_required
def control_action(request):
    """Dashboard control actions: Toggle, Panic, Ban, Exit, Save API"""
    if request.method != "POST":
        return JsonResponse({"status": "failed"}, status=400)

    try:
        data = json.loads(request.body)
        action = data.get('action')
        r = get_redis_client()
        
        if action == 'toggle_engine':
            side = data.get('side')
            enabled = "1" if data.get('enabled') else "0"
            r.set(f"algo:engine:{side}:enabled", enabled)
            return JsonResponse({'status': 'success'})

        elif action == 'save_api':
            acc, created = Account.objects.get_or_create(user=request.user)
            acc.api_key = data.get('api_key')
            acc.api_secret = data.get('api_secret')
            acc.save()
            return JsonResponse({'status': 'success'})

        elif action == 'panic_exit':
            side = data.get('side')
            r.set(f"algo:panic:{side}", "1", ex=60) 
            return JsonResponse({'status': 'panic_sent'})

        elif action == 'manual_exit':
            symbol = data.get('symbol')
            r.set(f"algo:manual_exit:{symbol}", "1", ex=30)
            return JsonResponse({'status': 'exit_sent'})

        elif action == 'ban_symbol':
            symbol = data.get('symbol', '').upper()
            if symbol:
                BannedSymbol.objects.get_or_create(symbol=symbol, reason="Manual Ban")
                r.sadd("algo:banned_symbols", symbol)
            return JsonResponse({'status': 'success'})

    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)})

    return JsonResponse({'status': 'error', 'message': 'Invalid action'})

@csrf_exempt
@login_required
def engine_settings_view(request, engine_type):
    """
    FIX: Argument name updated from 'side' to 'engine_type' 
    to match your project-level urls.py.
    """
    r = get_redis_client()
    acc = Account.objects.get(user=request.user)
    
    if request.method == 'POST':
        try:
            settings_data = json.loads(request.body)
            # 1. Sync to Redis for HFT Engines speed
            r.set(f"algo:settings:{engine_type}", json.dumps(settings_data))
            
            # 2. Save to Postgres DB
            if engine_type == 'bull': acc.bull_volume_settings_json = settings_data
            elif engine_type == 'bear': acc.bear_volume_settings_json = settings_data
            elif engine_type == 'mom_bull': acc.mom_bull_volume_settings = settings_data
            elif engine_type == 'mom_bear': acc.mom_bear_volume_settings = settings_data
            acc.save()
            return JsonResponse({'status': 'success'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)})

    # GET: Fetch current config (Priority Redis, Fallback DB)
    data = r.get(f"algo:settings:{engine_type}")
    if data:
        return JsonResponse(json.loads(data))
    
    db_field = f"{engine_type}_volume_settings_json" if 'mom' not in engine_type else f"{engine_type}_volume_settings"
    return JsonResponse(getattr(acc, db_field) or {})

@login_required
def get_orders(request):
    """Active/Closed trades fetch karega dashboard tables ke liye"""
    def fetch_trades(model):
        return list(model.objects.filter(user=request.user, status='OPEN').order_by('-id')[:10].values(
            'symbol', 'entry_price', 'qty', 'pnl', 'status'
        ))

    return JsonResponse({
        'bull': fetch_trades(CashBreakoutTrade),
        'bear': fetch_trades(CashBreakdownTrade),
        'mom_bull': fetch_trades(MomentumBullTrade),
        'mom_bear': fetch_trades(MomentumBearTrade),
    })

@login_required
def get_scanner_data(request):
    """Redis signals fetch karega live scanner ke liye"""
    r = get_redis_client()
    raw_signals = r.get("algo:scanner:signals")
    signals = json.loads(raw_signals) if raw_signals else []
    
    return JsonResponse({
        'bull': [s for s in signals if 'BULL' in s.get('type', '') and 'MOM' not in s.get('type', '')],
        'bear': [s for s in signals if 'BEAR' in s.get('type', '') and 'MOM' not in s.get('type', '')],
        'mom_bull': [s for s in signals if 'MOM_BULL' in s.get('type', '')],
        'mom_bear': [s for s in signals if 'MOM_BEAR' in s.get('type', '')],
    })

@csrf_exempt
@login_required
def global_settings_view(request):
    """Handles global configurations"""
    if request.method == 'POST':
        return JsonResponse({'status': 'success'})
    return JsonResponse({'status': 'global_active'})