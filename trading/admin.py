import json
from django.contrib import admin
from .models import (
    Account, 
    CashBreakoutTrade, CashBreakdownTrade,
    MomentumBullTrade, MomentumBearTrade
)
from .utils import get_redis_connection

redis_client = get_redis_connection()

# Redis Keys for Settings
KEY_BULL_SETTINGS = "algo:settings:bull"
KEY_BEAR_SETTINGS = "algo:settings:bear"
KEY_MOM_BULL_SETTINGS = "algo:settings:mom_bull"
KEY_MOM_BEAR_SETTINGS = "algo:settings:mom_bear"

@admin.register(Account)
class AccountAdmin(admin.ModelAdmin):
    list_display = ('user', 'is_master', 'is_breakout_cash_active', 'is_mom_bull_active', 'updated_at')
    search_fields = ('user__username', 'api_key')
    
    fieldsets = (
        ('User & Auth', {
            'fields': ('user', 'is_master', 'api_key', 'api_secret', 'access_token')
        }),
        ('Cash Bull Engine', {
            'fields': (
                'is_breakout_cash_active', 'breakout_start_time', 'breakout_end_time', 
                'breakout_max_trades', 'breakout_trades_per_stock', 
                'breakout_risk_reward', 'breakout_trailing_sl', 
                'breakout_risk_trade_1', 'breakout_risk_trade_2', 'breakout_risk_trade_3', 
                'bull_volume_settings_json'
            )
        }),
        ('Cash Bear Engine', {
            'fields': (
                'is_breakdown_cash_active', 'breakdown_start_time', 'breakdown_end_time', 
                'breakdown_max_trades', 'breakdown_trades_per_stock', 
                'breakdown_risk_reward', 'breakdown_trailing_sl', 
                'breakdown_risk_trade_1', 'breakdown_risk_trade_2', 'breakdown_risk_trade_3', 
                'bear_volume_settings_json'
            )
        }),
        ('Momentum Bull Engine (1-Min)', {
            'fields': (
                'is_mom_bull_active', 
                'mom_bull_stop_loss_pct', 'mom_bull_risk_reward', 'mom_bull_trailing_sl',
                'mom_bull_max_trades', 'mom_bull_risk_per_trade', 
                'mom_bull_volume_settings'
            )
        }),
        ('Momentum Bear Engine (1-Min)', {
            'fields': (
                'is_mom_bear_active', 
                'mom_bear_stop_loss_pct', 'mom_bear_risk_reward', 'mom_bear_trailing_sl',
                'mom_bear_max_trades', 'mom_bear_risk_per_trade', 
                'mom_bear_volume_settings'
            )
        }),
        ('Global P&L', {
            'fields': ('pnl_exit_enabled', 'max_daily_profit', 'max_daily_loss')
        }),
    )

    def save_model(self, request, obj, form, change):
        """
        Override save to push DB changes to Redis immediately for ALL engines.
        """
        super().save_model(request, obj, form, change)
        
        if not redis_client: return

        # Helper to format time
        def fmt(t): return t.strftime('%H:%M') if t else "09:15"

        # 1. Sync Cash Bull
        bull_payload = {
            'risk_reward': obj.breakout_risk_reward,
            'trailing_sl': obj.breakout_trailing_sl,
            'total_trades': obj.breakout_max_trades,
            'trades_per_stock': obj.breakout_trades_per_stock,
            'risk_trade_1': obj.breakout_risk_trade_1,
            'risk_trade_2': obj.breakout_risk_trade_2,
            'risk_trade_3': obj.breakout_risk_trade_3,
            'start_time': fmt(obj.breakout_start_time),
            'end_time': fmt(obj.breakout_end_time),
            'pnl_exit_enabled': obj.pnl_exit_enabled,
            'max_profit': obj.max_daily_profit,
            'max_loss': obj.max_daily_loss,
            'volume_criteria': obj.bull_volume_settings_json or []
        }
        redis_client.set(KEY_BULL_SETTINGS, json.dumps(bull_payload))

        # 2. Sync Cash Bear
        bear_payload = {
            'risk_reward': obj.breakdown_risk_reward,
            'trailing_sl': obj.breakdown_trailing_sl,
            'total_trades': obj.breakdown_max_trades,
            'trades_per_stock': obj.breakdown_trades_per_stock,
            'risk_trade_1': obj.breakdown_risk_trade_1,
            'risk_trade_2': obj.breakdown_risk_trade_2,
            'risk_trade_3': obj.breakdown_risk_trade_3,
            'start_time': fmt(obj.breakdown_start_time),
            'end_time': fmt(obj.breakdown_end_time),
            'pnl_exit_enabled': obj.pnl_exit_enabled,
            'max_profit': obj.max_daily_profit,
            'max_loss': obj.max_daily_loss,
            'volume_criteria': obj.bear_volume_settings_json or []
        }
        redis_client.set(KEY_BEAR_SETTINGS, json.dumps(bear_payload))

        # 3. Sync Momentum Bull
        mom_bull_payload = {
            'stop_loss_pct': obj.mom_bull_stop_loss_pct,
            'risk_reward': obj.mom_bull_risk_reward,
            'trailing_sl': obj.mom_bull_trailing_sl,
            'max_trades': obj.mom_bull_max_trades,
            'risk_per_trade': obj.mom_bull_risk_per_trade,
            'pnl_exit_enabled': obj.pnl_exit_enabled,
            'max_profit': obj.max_daily_profit,
            'max_loss': obj.max_daily_loss,
            'volume_criteria': obj.mom_bull_volume_settings or []
        }
        redis_client.set(KEY_MOM_BULL_SETTINGS, json.dumps(mom_bull_payload))

        # 4. Sync Momentum Bear
        mom_bear_payload = {
            'stop_loss_pct': obj.mom_bear_stop_loss_pct,
            'risk_reward': obj.mom_bear_risk_reward,
            'trailing_sl': obj.mom_bear_trailing_sl,
            'max_trades': obj.mom_bear_max_trades,
            'risk_per_trade': obj.mom_bear_risk_per_trade,
            'pnl_exit_enabled': obj.pnl_exit_enabled,
            'max_profit': obj.max_daily_profit,
            'max_loss': obj.max_daily_loss,
            'volume_criteria': obj.mom_bear_volume_settings or []
        }
        redis_client.set(KEY_MOM_BEAR_SETTINGS, json.dumps(mom_bear_payload))


# --- Trade Model Registration ---

@admin.register(CashBreakoutTrade)
class CashBreakoutTradeAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'status', 'entry_level', 'quantity', 'pnl', 'created_at')
    list_filter = ('status', 'created_at')
    search_fields = ('symbol', 'entry_order_id')
    readonly_fields = ('created_at', 'updated_at')

@admin.register(CashBreakdownTrade)
class CashBreakdownTradeAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'status', 'entry_level', 'quantity', 'pnl', 'created_at')
    list_filter = ('status', 'created_at')
    search_fields = ('symbol', 'entry_order_id')
    readonly_fields = ('created_at', 'updated_at')

@admin.register(MomentumBullTrade)
class MomentumBullTradeAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'status', 'entry_level', 'target_level', 'quantity', 'pnl', 'created_at')
    list_filter = ('status', 'created_at')
    search_fields = ('symbol', 'entry_order_id')
    readonly_fields = ('created_at', 'updated_at', 'first_candle_high', 'prev_day_close')

@admin.register(MomentumBearTrade)
class MomentumBearTradeAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'status', 'entry_level', 'target_level', 'quantity', 'pnl', 'created_at')
    list_filter = ('status', 'created_at')
    search_fields = ('symbol', 'entry_order_id')
    readonly_fields = ('created_at', 'updated_at', 'first_candle_low', 'prev_day_close')