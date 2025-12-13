import json
from django.contrib import admin
from .models import Account, CashBreakoutTrade, CashBreakdownTrade
from .utils import get_redis_connection

redis_client = get_redis_connection()

KEY_BULL_SETTINGS = "algo:settings:bull"
KEY_BEAR_SETTINGS = "algo:settings:bear"

@admin.register(Account)
class AccountAdmin(admin.ModelAdmin):
    list_display = ('user', 'is_master', 'is_breakout_cash_active', 'is_breakdown_cash_active', 'updated_at')
    search_fields = ('user__username', 'api_key')
    fieldsets = (
        ('User & Auth', {'fields': ('user', 'is_master', 'api_key', 'api_secret', 'access_token')}),
        ('Bull Engine', {'fields': ('is_breakout_cash_active', 'breakout_start_time', 'breakout_end_time', 'breakout_max_trades', 'breakout_trades_per_stock', 'breakout_risk_reward', 'breakout_trailing_sl', 'breakout_risk_trade_1', 'breakout_risk_trade_2', 'breakout_risk_trade_3', 'bull_volume_settings_json')}),
        ('Bear Engine', {'fields': ('is_breakdown_cash_active', 'breakdown_start_time', 'breakdown_end_time', 'breakdown_max_trades', 'breakdown_trades_per_stock', 'breakdown_risk_reward', 'breakdown_trailing_sl', 'breakdown_risk_trade_1', 'breakdown_risk_trade_2', 'breakdown_risk_trade_3', 'bear_volume_settings_json')}),
        ('Global P&L', {'fields': ('pnl_exit_enabled', 'max_daily_profit', 'max_daily_loss')}),
    )

    def save_model(self, request, obj, form, change):
        """
        Override save to push DB changes to Redis immediately.
        Fixes Issue: Admin changes not reflecting in Frontend.
        """
        super().save_model(request, obj, form, change)
        
        if not redis_client: return

        # Helper to format time
        def fmt(t): return t.strftime('%H:%M') if t else "09:15"

        # 1. Sync Bull Settings to Redis
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
            'volume_criteria': obj.bull_volume_settings_json or [] # Specific Bull Volume
        }
        redis_client.set(KEY_BULL_SETTINGS, json.dumps(bull_payload))

        # 2. Sync Bear Settings to Redis
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
            'volume_criteria': obj.bear_volume_settings_json or [] # Specific Bear Volume
        }
        redis_client.set(KEY_BEAR_SETTINGS, json.dumps(bear_payload))

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