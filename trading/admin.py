import json
import logging
from django.contrib import admin
from .models import (
    Account, 
    CashBreakoutTrade, CashBreakdownTrade,
    MomentumBullTrade, MomentumBearTrade
)
from .hft_utils import get_redis_client, LUA_INC_LIMIT

logger = logging.getLogger("AdminSync")

# Redis Keys (Consistency with Engines)
KEY_BULL_SETTINGS = "algo:settings:bull"
KEY_BEAR_SETTINGS = "algo:settings:bear"
KEY_MOM_BULL_SETTINGS = "algo:settings:mom_bull"
KEY_MOM_BEAR_SETTINGS = "algo:settings:mom_bear"

@admin.register(Account)
class AccountAdmin(admin.ModelAdmin):
    # list_display mein wahi fields rakhein jo models.py mein hain
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
        Override save: Admin mein change hote hi Redis (RAM) ko update karo.
        """
        super().save_model(request, obj, form, change)
        
        r = get_redis_client()
        # Agar Redis Mock hai toh sync skip karein
        if hasattr(r, 'is_mock'): return

        def fmt(t): return t.strftime('%H:%M') if t else "09:15"

        try:
            # 1. Sync Breakout (Bull)
            bull_payload = {
                'total_trades': obj.breakout_max_trades,
                'start_time': fmt(obj.breakout_start_time),
                'end_time': fmt(obj.breakout_end_time),
                'volume_criteria': json.loads(obj.bull_volume_settings_json) if obj.bull_volume_settings_json else []
            }
            r.set(KEY_BULL_SETTINGS, json.dumps(bull_payload))

            # 2. Sync Breakdown (Bear)
            bear_payload = {
                'total_trades': obj.breakdown_max_trades,
                'start_time': fmt(obj.breakdown_start_time),
                'end_time': fmt(obj.breakdown_end_time),
                'volume_criteria': json.loads(obj.bear_volume_settings_json) if obj.bear_volume_settings_json else []
            }
            r.set(KEY_BEAR_SETTINGS, json.dumps(bear_payload))

            # 3. Sync Momentum
            # Momentum ke liye simple payload
            r.set(KEY_MOM_BULL_SETTINGS, obj.mom_bull_volume_settings or "{}")
            r.set(KEY_MOM_BEAR_SETTINGS, obj.mom_bear_volume_settings or "{}")

            logger.info(f"Admin Sync: Redis RAM updated for user {obj.user.username}")
        except Exception as e:
            logger.error(f"Redis Sync failed during Admin save: {e}")

# --- Trade Model Registration ---

@admin.register(CashBreakoutTrade)
class CashBreakoutTradeAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'status', 'entry_price', 'qty', 'pnl', 'created_at')
    list_filter = ('status', 'created_at')
    readonly_fields = ('created_at',)

@admin.register(CashBreakdownTrade)
class CashBreakdownTradeAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'status', 'entry_price', 'qty', 'pnl', 'created_at')
    list_filter = ('status', 'created_at')
    readonly_fields = ('created_at',)

@admin.register(MomentumBullTrade)
class MomentumBullTradeAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'status', 'entry_price', 'qty', 'pnl', 'created_at')
    list_filter = ('status', 'created_at')

@admin.register(MomentumBearTrade)
class MomentumBearTradeAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'status', 'entry_price', 'qty', 'pnl', 'created_at')
    list_filter = ('status', 'created_at')