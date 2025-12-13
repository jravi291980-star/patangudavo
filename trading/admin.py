from django.contrib import admin
from .models import Account, CashBreakoutTrade, CashBreakdownTrade

@admin.register(Account)
class AccountAdmin(admin.ModelAdmin):
    list_display = ('user', 'is_master', 'is_breakout_cash_active', 'is_breakdown_cash_active', 'updated_at')
    search_fields = ('user__username', 'api_key')
    fieldsets = (
        ('User & Auth', {'fields': ('user', 'is_master', 'api_key', 'api_secret', 'access_token')}),
        ('Bull Engine', {'fields': ('is_breakout_cash_active', 'breakout_start_time', 'breakout_end_time', 'breakout_max_trades', 'breakout_trades_per_stock', 'breakout_risk_reward', 'breakout_trailing_sl', 'breakout_risk_trade_1', 'breakout_risk_trade_2', 'breakout_risk_trade_3')}),
        ('Bear Engine', {'fields': ('is_breakdown_cash_active', 'breakdown_start_time', 'breakdown_end_time', 'breakdown_max_trades', 'breakdown_trades_per_stock', 'breakdown_risk_reward', 'breakdown_trailing_sl', 'breakdown_risk_trade_1', 'breakdown_risk_trade_2', 'breakdown_risk_trade_3')}),
        ('Global Settings', {'fields': ('volume_settings_json', 'pnl_exit_enabled', 'max_daily_profit', 'max_daily_loss')}),
    )

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