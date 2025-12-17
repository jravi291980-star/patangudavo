import json
import logging
import time
from datetime import datetime as dt
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render
from django.db.models import Q, Sum
from kiteconnect import KiteConnect

from .models import (
    Account, 
    CashBreakoutTrade, CashBreakdownTrade,
    MomentumBullTrade, MomentumBearTrade
)
from .utils import get_redis_connection, get_kite

logger = logging.getLogger(__name__)
redis_client = get_redis_connection()

# --- REDIS KEYS ---
KEY_ACCESS_TOKEN = "kite:access_token"
KEY_ENGINE_BULL_ENABLED = "algo:engine:bull:enabled"
KEY_ENGINE_BEAR_ENABLED = "algo:engine:bear:enabled"
KEY_ENGINE_MOM_BULL_ENABLED = "algo:engine:mom_bull:enabled"
KEY_ENGINE_MOM_BEAR_ENABLED = "algo:engine:mom_bear:enabled"
KEY_BLACKLIST = "algo:blacklist"
KEY_PANIC_BULL = "algo:panic:bull"
KEY_PANIC_BEAR = "algo:panic:bear"
KEY_PANIC_MOM_BULL = "algo:panic:mom_bull"
KEY_PANIC_MOM_BEAR = "algo:panic:mom_bear"
DATA_HEARTBEAT_KEY = "algo:data:heartbeat"

def get_user_account(user):
    return Account.objects.filter(user=user).first()

def home(request):
    return render(request, 'trading/dashboard.html')

@login_required
def kite_login(request):
    account = get_user_account(request.user)
    if not account or not account.api_key:
        return JsonResponse({'error': 'Account/API Key not configured'}, status=400)
    kite = KiteConnect(api_key=account.api_key)
    return redirect(kite.login_url())

@login_required
def kite_callback(request):
    request_token = request.GET.get('request_token')
    if not request_token: return JsonResponse({'error': 'No request_token'}, status=400)
    account = get_user_account(request.user)
    try:
        kite = KiteConnect(api_key=account.api_key)
        data = kite.generate_session(request_token, api_secret=account.api_secret)
        account.access_token = data['access_token']
        account.save()
        if redis_client: redis_client.set(KEY_ACCESS_TOKEN, data['access_token'])
        return redirect('/dashboard')
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)

# --- DASHBOARD API ---

@login_required
@require_http_methods(["GET"])
def dashboard_stats(request):
    """
    Returns P&L for ALL 4 Engines + System Health.
    """
    account = get_user_account(request.user)
    if not account: return JsonResponse({}, status=404)

    today = dt.now().date()
    
    # 1. Fetch Live Data for Unrealized PnL Calculation
    live_data = {}
    if redis_client:
        try: 
            raw_data = redis_client.get('live_ohlc_data')
            if raw_data:
                live_data = json.loads(raw_data)
        except: pass

    def calc_pnl(model_class, side='long'):
        # Realized
        realized = model_class.objects.filter(account=account, status='CLOSED', exit_time__date=today).aggregate(s=Sum('pnl'))['s'] or 0.0
        
        # Unrealized
        open_trades = model_class.objects.filter(account=account, status='OPEN')
        unrealized = 0.0
        for t in open_trades:
            # Use 'ltp' from live_data, fallback to entry_price to avoid crazy numbers
            ltp = float(live_data.get(t.symbol, {}).get('ltp', t.entry_price or 0))
            if ltp > 0 and t.entry_price:
                if side == 'long': unrealized += (ltp - t.entry_price) * t.quantity
                else: unrealized += (t.entry_price - ltp) * t.quantity # Short
        return realized + unrealized

    pnl_bull = calc_pnl(CashBreakoutTrade, 'long')
    pnl_bear = calc_pnl(CashBreakdownTrade, 'short')
    pnl_mom_bull = calc_pnl(MomentumBullTrade, 'long')
    pnl_mom_bear = calc_pnl(MomentumBearTrade, 'short')

    # Heartbeat Check
    last_beat = 0
    if redis_client:
        try: last_beat = int(redis_client.get(DATA_HEARTBEAT_KEY) or 0)
        except: pass
    
    def get_status(key): return (redis_client.get(key) or b'1').decode() if redis_client else '1'

    return JsonResponse({
        'pnl': {
            'total': round(pnl_bull + pnl_bear + pnl_mom_bull + pnl_mom_bear, 2),
            'bull': round(pnl_bull, 2),
            'bear': round(pnl_bear, 2),
            'mom_bull': round(pnl_mom_bull, 2),
            'mom_bear': round(pnl_mom_bear, 2),
        },
        'engine_status': {
            'bull': get_status(KEY_ENGINE_BULL_ENABLED),
            'bear': get_status(KEY_ENGINE_BEAR_ENABLED),
            'mom_bull': get_status(KEY_ENGINE_MOM_BULL_ENABLED),
            'mom_bear': get_status(KEY_ENGINE_MOM_BEAR_ENABLED),
        },
        'data_connected': (time.time() - last_beat) < 15
    })

@login_required
@require_http_methods(["GET"])
def get_orders(request):
    """
    Returns Executed Orders (OPEN/CLOSED) for ALL 4 Engines.
    """
    account = get_user_account(request.user)
    if not account: return JsonResponse({})
    today = dt.now().date()

    def fetch_orders(model_class):
        # Fetch OPEN trades OR today's CLOSED trades
        qs = model_class.objects.filter(account=account).filter(
            Q(status__in=['OPEN', 'PENDING_EXIT']) | 
            Q(status='CLOSED', created_at__date=today)
        ).order_by('-created_at')
        
        return [{
            'id': t.id, 'symbol': t.symbol, 
            'entry': t.entry_price or t.entry_level,
            'target': t.target_level, 'sl': t.stop_level,
            'status': t.status, 'pnl': round(t.pnl, 2), 'quantity': t.quantity
        } for t in qs]

    return JsonResponse({
        'bull': fetch_orders(CashBreakoutTrade),
        'bear': fetch_orders(CashBreakdownTrade),
        'mom_bull': fetch_orders(MomentumBullTrade),
        'mom_bear': fetch_orders(MomentumBearTrade)
    })

@login_required
@require_http_methods(["GET"])
def get_scanner_data(request):
    """
    Returns Pending Signals (The Scanner) for ALL 4 Engines.
    """
    account = get_user_account(request.user)
    if not account: return JsonResponse({})
    today = dt.now().date()
    
    statuses = ['PENDING', 'EXPIRED', 'FAILED_ENTRY']

    def fetch_scan(model_class, label):
        qs = model_class.objects.filter(
            account=account, created_at__date=today, status__in=statuses
        ).order_by('-created_at')
        
        data = []
        for t in qs:
            candle_sz = 0.0
            if t.candle_close > 0:
                candle_sz = ((t.candle_high - t.candle_low) / t.candle_close) * 100
            
            data.append({
                'id': t.id, 'symbol': t.symbol, 
                'signal_strength': label,
                'price': t.candle_close, 'trigger': t.entry_level,
                'time': t.candle_ts.strftime('%H:%M:%S') if t.candle_ts else "--:--",
                'volume_price': f"{t.volume_price:.2f}Cr",
                'candle_size': f"{candle_sz:.2f}%",
                'status': t.status
            })
        return data

    return JsonResponse({
        'bull': fetch_scan(CashBreakoutTrade, 'Cash BO'),
        'bear': fetch_scan(CashBreakdownTrade, 'Cash BD'),
        'mom_bull': fetch_scan(MomentumBullTrade, 'Mom Buy'),
        'mom_bear': fetch_scan(MomentumBearTrade, 'Mom Sell')
    })

@csrf_exempt
@login_required
@require_http_methods(["GET", "POST"])
def engine_settings_view(request, engine_type):
    """
    Handles Settings for all 4 engine types.
    Saves to DB and Syncs to Redis.
    """
    account = get_user_account(request.user)
    if not account or not redis_client: return JsonResponse({}, status=400)

    # Configuration Mapping
    config_map = {
        'bull': ('algo:settings:bull', 'breakout', 'bull'),
        'bear': ('algo:settings:bear', 'breakdown', 'bear'),
        'mom_bull': ('algo:settings:mom_bull', 'mom_bull', 'mom_bull'),
        'mom_bear': ('algo:settings:mom_bear', 'mom_bear', 'mom_bear'),
    }

    if engine_type not in config_map: return JsonResponse({'error': 'Invalid engine'}, status=400)
    
    redis_key, prefix, vol_prefix = config_map[engine_type]

    if request.method == "POST":
        try:
            payload = json.loads(request.body)
            # Sync to Redis for Engines
            redis_client.set(redis_key, json.dumps(payload))
            
            # Common Fields
            if 'risk_reward' in payload: setattr(account, f"{prefix}_risk_reward", payload['risk_reward'])
            if 'trailing_sl' in payload: setattr(account, f"{prefix}_trailing_sl", payload['trailing_sl'])
            if 'total_trades' in payload: setattr(account, f"{prefix}_max_trades", int(payload['total_trades']))
            
            if payload.get('start_time'): setattr(account, f"{prefix}_start_time", payload['start_time'])
            if payload.get('end_time'): setattr(account, f"{prefix}_end_time", payload['end_time'])

            # Engine Specific Fields
            if 'mom' in engine_type:
                if 'stop_loss_pct' in payload: setattr(account, f"{prefix}_stop_loss_pct", float(payload['stop_loss_pct']))
                if 'risk_per_trade' in payload: setattr(account, f"{prefix}_risk_per_trade", float(payload['risk_per_trade']))
                if 'volume_criteria' in payload: setattr(account, f"{prefix}_volume_settings", payload['volume_criteria'])
            else:
                if 'trades_per_stock' in payload: setattr(account, f"{prefix}_trades_per_stock", int(payload['trades_per_stock']))
                if 'risk_trade_1' in payload: setattr(account, f"{prefix}_risk_trade_1", float(payload['risk_trade_1']))
                if 'risk_trade_2' in payload: setattr(account, f"{prefix}_risk_trade_2", float(payload['risk_trade_2']))
                if 'risk_trade_3' in payload: setattr(account, f"{prefix}_risk_trade_3", float(payload['risk_trade_3']))
                if 'volume_criteria' in payload: 
                    field = 'bull_volume_settings_json' if engine_type == 'bull' else 'bear_volume_settings_json'
                    setattr(account, field, payload['volume_criteria'])

            # Global P&L
            if 'pnl_exit_enabled' in payload: account.pnl_exit_enabled = payload['pnl_exit_enabled']
            if 'max_profit' in payload: account.max_daily_profit = float(payload['max_profit'])
            if 'max_loss' in payload: account.max_daily_loss = float(payload['max_loss'])

            account.save()
            return JsonResponse({'status': 'saved'})
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=400)

    # GET Request
    data = redis_client.get(redis_key)
    if data:
        return JsonResponse(json.loads(data))
    else:
        # Fallback to DB if Redis empty
        resp = {
            'risk_reward': getattr(account, f"{prefix}_risk_reward"),
            'trailing_sl': getattr(account, f"{prefix}_trailing_sl"),
            'total_trades': getattr(account, f"{prefix}_max_trades"),
            'start_time': getattr(account, f"{prefix}_start_time").strftime('%H:%M'),
            'end_time': getattr(account, f"{prefix}_end_time").strftime('%H:%M'),
            'pnl_exit_enabled': account.pnl_exit_enabled,
            'max_profit': account.max_daily_profit,
            'max_loss': account.max_daily_loss,
        }
        
        if 'mom' in engine_type:
            resp['stop_loss_pct'] = getattr(account, f"{prefix}_stop_loss_pct")
            resp['risk_per_trade'] = getattr(account, f"{prefix}_risk_per_trade")
            resp['volume_criteria'] = getattr(account, f"{prefix}_volume_settings") or []
        else:
            resp['trades_per_stock'] = getattr(account, f"{prefix}_trades_per_stock")
            resp['risk_trade_1'] = getattr(account, f"{prefix}_risk_trade_1")
            resp['risk_trade_2'] = getattr(account, f"{prefix}_risk_trade_2")
            resp['risk_trade_3'] = getattr(account, f"{prefix}_risk_trade_3")
            field = 'bull_volume_settings_json' if engine_type == 'bull' else 'bear_volume_settings_json'
            resp['volume_criteria'] = getattr(account, field) or []

        return JsonResponse(resp)

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
            key_map = {
                'bull': KEY_ENGINE_BULL_ENABLED, 'bear': KEY_ENGINE_BEAR_ENABLED,
                'mom_bull': KEY_ENGINE_MOM_BULL_ENABLED, 'mom_bear': KEY_ENGINE_MOM_BEAR_ENABLED
            }
            if side in key_map:
                redis_client.set(key_map[side], "1" if enabled else "0")
                if side == 'bull': account.is_breakout_cash_active = enabled
                elif side == 'bear': account.is_breakdown_cash_active = enabled
                elif side == 'mom_bull': account.is_mom_bull_active = enabled
                elif side == 'mom_bear': account.is_mom_bear_active = enabled
                account.save()
            return JsonResponse({'status': 'success'})

        elif action == 'ban_symbol':
            redis_client.sadd(KEY_BLACKLIST, data.get('symbol'))
            return JsonResponse({'status': 'success'})

        elif action == 'unban_symbol':
            redis_client.srem(KEY_BLACKLIST, data.get('symbol'))
            return JsonResponse({'status': 'success'})
            
        elif action == 'panic_exit':
            side = data.get('side')
            key_map = {'bull': KEY_PANIC_BULL, 'bear': KEY_PANIC_BEAR, 'mom_bull': KEY_PANIC_MOM_BULL, 'mom_bear': KEY_PANIC_MOM_BEAR}
            if side in key_map: redis_client.set(key_map[side], "1")
            return JsonResponse({'status': 'success'})

        elif action == 'exit_trade':
            symbol = data.get('symbol')
            # Iterate to find the specific trade ID to force exit
            found = False
            for Model in [CashBreakoutTrade, CashBreakdownTrade, MomentumBullTrade, MomentumBearTrade]:
                trade = Model.objects.filter(account=account, symbol=symbol, status='OPEN').first()
                if trade:
                    key = ""
                    if Model == CashBreakoutTrade: key = f"breakout_force_exit_requests:{account.id}"
                    elif Model == CashBreakdownTrade: key = f"breakdown_force_exit_requests:{account.id}"
                    elif Model == MomentumBullTrade: key = f"mom_bull_force_exit_requests:{account.id}"
                    elif Model == MomentumBearTrade: key = f"mom_bear_force_exit_requests:{account.id}"
                    
                    if key:
                        redis_client.sadd(key, str(trade.id))
                        found = True
            
            if found: return JsonResponse({'status': 'success'})
            else: return JsonResponse({'error': 'Trade not found'}, 404)

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)
    return JsonResponse({'error': 'Invalid Action'}, status=400)

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