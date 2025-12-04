from django.contrib import admin
from .models import Account, CashBreakoutTrade, CashBreakdownTrade

@admin.register(Account)
class AccountAdmin(admin.ModelAdmin):
    list_display = ('user', 'is_master', 'is_breakout_cash_active', 'is_breakdown_cash_active', 'updated_at')
    search_fields = ('user__username', 'api_key')

@admin.register(CashBreakoutTrade)
class CashBreakoutTradeAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'status', 'entry_level', 'pnl', 'created_at')
    list_filter = ('status', 'created_at')
    search_fields = ('symbol', 'entry_order_id')
    readonly_fields = ('created_at', 'updated_at')

@admin.register(CashBreakdownTrade)
class CashBreakdownTradeAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'status', 'entry_level', 'pnl', 'created_at')
    list_filter = ('status', 'created_at')
    search_fields = ('symbol', 'entry_order_id')
    readonly_fields = ('created_at', 'updated_at')