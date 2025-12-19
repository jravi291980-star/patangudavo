import json
import logging
from django.contrib import admin
from .models import (
    Account, BannedSymbol,
    CashBreakoutTrade, CashBreakdownTrade,
    MomentumBullTrade, MomentumBearTrade
)
from .hft_utils import get_redis_client

logger = logging.getLogger("AdminSync")

# Redis Keys for HFT Engines
KEY_BULL_SETTINGS = "algo:settings:bull"
KEY_BEAR_SETTINGS = "algo:settings:bear"
KEY_MOM_BULL_SETTINGS = "algo:settings:mom_bull"
KEY_MOM_BEAR_SETTINGS = "algo:settings:mom_bear"

@admin.register(Account)
class AccountAdmin(admin.ModelAdmin):
    list_display = ('user', 'breakout_max_trades', 'breakdown_max_trades', 'momentum_max_trades')
    search_fields = ('user__username', 'api_key')
    
    fieldsets = (
        ('User & Auth', {
            'fields': ('user', 'api_key', 'api_secret', 'access_token')
        }),
        ('Nexus 1: Breakout (Bull)', {
            'fields': (
                'breakout_start_time', 'breakout_end_time', 
                'breakout_max_trades', 'bull_volume_settings_json'
            )
        }),
        ('Nexus 1: Breakdown (Bear)', {
            'fields': (
                'breakdown_start_time', 'breakdown_end_time', 
                'breakdown_max_trades', 'bear_volume_settings_json'
            )
        }),
        ('Nexus 2: Momentum', {
            'fields': (
                'momentum_max_trades', 
                'mom_bull_volume_settings', 'mom_bear_volume_settings'
            )
        }),
    )

    def save_model(self, request, obj, form, change):
        """
        Overridden save: Admin mein change hote hi Redis (RAM) ko update karo.
        """
        super().save_model(request, obj, form, change)
        
        r = get_redis_client()
        # Mock connection check
        if hasattr(r, 'is_mock'): return

        def fmt(t): return t.strftime('%H:%M') if t else "09:15"

        try:
            # Note: JSONField returns Python object, no need to json.loads()
            # We use json.dumps() to store it as a string in Redis for the Engines.
            
            # 1. Sync Breakout Bull
            r.set(KEY_BULL_SETTINGS, json.dumps({
                'total_trades': obj.breakout_max_trades,
                'start_time': fmt(obj.breakout_start_time),
                'end_time': fmt(obj.breakout_end_time),
                'volume_criteria': obj.bull_volume_settings_json
            }))

            # 2. Sync Breakdown Bear
            r.set(KEY_BEAR_SETTINGS, json.dumps({
                'total_trades': obj.breakdown_max_trades,
                'start_time': fmt(obj.breakdown_start_time),
                'end_time': fmt(obj.breakdown_end_time),
                'volume_criteria': obj.bear_volume_settings_json
            }))

            # 3. Sync Momentum
            r.set(KEY_MOM_BULL_SETTINGS, json.dumps(obj.mom_bull_volume_settings))
            r.set(KEY_MOM_BEAR_SETTINGS, json.dumps(obj.mom_bear_volume_settings))

            logger.info(f"Admin Sync: Redis RAM updated for user {obj.user.username}")
        except Exception as e:
            logger.error(f"Redis Sync failed: {e}")

@admin.register(BannedSymbol)
class BannedSymbolAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'reason', 'created_at')
    search_fields = ('symbol',)

# Common Layout for all HFT Trades
class TradeAdmin(admin.ModelAdmin):
    # Added SL, Target and Order ID for full visibility
    list_display = ('symbol', 'status', 'entry_price', 'qty', 'sl_price', 'target_price', 'pnl', 'created_at')
    list_filter = ('status', 'created_at')
    search_fields = ('symbol', 'order_id')
    readonly_fields = ('created_at', 'updated_at', 'order_id')

admin.site.register(CashBreakoutTrade, TradeAdmin)
admin.site.register(CashBreakdownTrade, TradeAdmin)
admin.site.register(MomentumBullTrade, TradeAdmin)
admin.site.register(MomentumBearTrade, TradeAdmin)