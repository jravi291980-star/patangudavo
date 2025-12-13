from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone
from datetime import time

class Account(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='trading_account')
    
    # --- Kite Connect Credentials ---
    api_key = models.CharField(max_length=50, blank=True, null=True)
    api_secret = models.CharField(max_length=50, blank=True, null=True)
    access_token = models.CharField(max_length=255, blank=True, null=True)
    is_master = models.BooleanField(default=False, help_text="Is this the master account for data fetching?")

    # --- Bull Engine Config ---
    is_breakout_cash_active = models.BooleanField(default=True, verbose_name="Bull Engine Switch")
    breakout_start_time = models.TimeField(default=time(9, 15))
    breakout_end_time = models.TimeField(default=time(15, 15))
    breakout_max_trades = models.IntegerField(default=5)
    breakout_trades_per_stock = models.IntegerField(default=2)
    breakout_risk_reward = models.CharField(max_length=10, default="1:2")
    breakout_trailing_sl = models.CharField(max_length=10, default="1:1")
    
    # Tiered Risk (Bull)
    breakout_risk_trade_1 = models.FloatField(default=2000.0)
    breakout_risk_trade_2 = models.FloatField(default=1500.0)
    breakout_risk_trade_3 = models.FloatField(default=1000.0)

    # Bull Volume Settings (Separated)
    bull_volume_settings_json = models.JSONField(default=list, blank=True, null=True, help_text="List of 10 Bull Volume Criteria Levels")

    # --- Bear Engine Config ---
    is_breakdown_cash_active = models.BooleanField(default=True, verbose_name="Bear Engine Switch")
    breakdown_start_time = models.TimeField(default=time(9, 15))
    breakdown_end_time = models.TimeField(default=time(15, 15))
    breakdown_max_trades = models.IntegerField(default=5)
    breakdown_trades_per_stock = models.IntegerField(default=2)
    breakdown_risk_reward = models.CharField(max_length=10, default="1:2")
    breakdown_trailing_sl = models.CharField(max_length=10, default="1:1")

    # Tiered Risk (Bear)
    breakdown_risk_trade_1 = models.FloatField(default=2000.0)
    breakdown_risk_trade_2 = models.FloatField(default=1500.0)
    breakdown_risk_trade_3 = models.FloatField(default=1000.0)

    # Bear Volume Settings (Separated)
    bear_volume_settings_json = models.JSONField(default=list, blank=True, null=True, help_text="List of 10 Bear Volume Criteria Levels")

    # --- Global P&L Limits ---
    pnl_exit_enabled = models.BooleanField(default=False)
    max_daily_profit = models.FloatField(default=5000.0)
    max_daily_loss = models.FloatField(default=2000.0)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.user.username} - Account"

class BaseTrade(models.Model):
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