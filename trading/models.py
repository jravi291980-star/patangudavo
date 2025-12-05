from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone
from datetime import time

class Account(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='trading_account')
    
    # --- Kite Connect Credentials ---
    api_key = models.CharField(max_length=50, blank=True, null=True)
    access_token = models.CharField(max_length=255, blank=True, null=True)
    is_master = models.BooleanField(default=False, help_text="Is this the master account for data fetching?")

    # --- Bull Engine Config (Hard Limits) ---
    is_breakout_cash_active = models.BooleanField(default=True, verbose_name="Bull Engine Master Switch")
    breakout_start_time = models.TimeField(default=time(9, 15))
    breakout_end_time = models.TimeField(default=time(15, 15))
    breakout_max_total_trades = models.IntegerField(default=10, help_text="Max Bull trades per day")
    breakout_per_trade_sl_amount = models.FloatField(default=1000.0, help_text="Max Rupees risk per Bull trade")
    breakout_volume_price_threshold = models.FloatField(default=5000000.0, help_text="Min Volume*Price (Rupees)")
    
    # --- Bear Engine Config (Hard Limits) ---
    is_breakdown_cash_active = models.BooleanField(default=True, verbose_name="Bear Engine Master Switch")
    breakdown_start_time = models.TimeField(default=time(9, 15))
    breakdown_end_time = models.TimeField(default=time(15, 15))
    breakdown_max_total_trades = models.IntegerField(default=10, help_text="Max Bear trades per day")
    breakdown_per_trade_sl_amount = models.FloatField(default=1000.0, help_text="Max Rupees risk per Bear trade")
    breakdown_volume_price_threshold = models.FloatField(default=5000000.0, help_text="Min Volume*Price (Rupees)")

    # --- P&L Limits (DB Backup for Redis) ---
    breakout_pnl_exit_enabled = models.BooleanField(default=False)
    breakout_pnl_profit_target_amount = models.FloatField(default=5000.0)
    breakout_pnl_stop_loss_amount = models.FloatField(default=2000.0)

    breakdown_pnl_exit_enabled = models.BooleanField(default=False)
    breakdown_pnl_profit_target_amount = models.FloatField(default=5000.0)
    breakdown_pnl_stop_loss_amount = models.FloatField(default=2000.0)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.user.username} - Account"

class BaseTrade(models.Model):
    """Abstract base class for common trade fields"""
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    account = models.ForeignKey(Account, on_delete=models.CASCADE)
    symbol = models.CharField(max_length=20)
    status = models.CharField(max_length=20, choices=[
        ('PENDING', 'Pending Trigger'),
        ('PENDING_ENTRY', 'Entry Submitted'),
        ('OPEN', 'Open Position'),
        ('PENDING_EXIT', 'Exit Submitted'),
        ('CLOSED', 'Closed'),
        ('EXPIRED', 'Expired'),
        ('FAILED_ENTRY', 'Entry Failed'),
        ('FAILED_EXIT', 'Exit Failed')
    ], default='PENDING')
    
    # Candle Data
    candle_ts = models.DateTimeField(null=True, blank=True)
    candle_open = models.FloatField(default=0.0)
    candle_high = models.FloatField(default=0.0)
    candle_low = models.FloatField(default=0.0)
    candle_close = models.FloatField(default=0.0)
    candle_volume = models.IntegerField(default=0)
    volume_price = models.FloatField(default=0.0)

    # Trade Execution Data
    entry_level = models.FloatField(help_text="Trigger Price")
    stop_level = models.FloatField(help_text="Stop Loss Price")
    target_level = models.FloatField(help_text="Target Price")
    
    quantity = models.IntegerField(default=0)
    entry_order_id = models.CharField(max_length=50, null=True, blank=True)
    exit_order_id = models.CharField(max_length=50, null=True, blank=True)
    
    entry_price = models.FloatField(null=True, blank=True)
    exit_price = models.FloatField(null=True, blank=True)
    
    entry_time = models.DateTimeField(null=True, blank=True)
    exit_time = models.DateTimeField(null=True, blank=True)
    
    pnl = models.FloatField(default=0.0)
    exit_reason = models.CharField(max_length=100, null=True, blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True

class CashBreakoutTrade(BaseTrade):
    prev_day_high = models.FloatField(default=0.0)
    
    class Meta:
        verbose_name = "Bull Trade"
        ordering = ['-created_at']

class CashBreakdownTrade(BaseTrade):
    prev_day_low = models.FloatField(default=0.0)

    class Meta:
        verbose_name = "Bear Trade"
        ordering = ['-created_at']