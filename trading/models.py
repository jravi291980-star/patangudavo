from django.db import models
from django.contrib.auth.models import User

class Account(models.Model):
    """
    Kite API credentials aur strategy ki global settings store karne ke liye.
    """
    user = models.OneToOneField(User, on_on_delete=models.CASCADE)
    
    # API Credentials
    api_key = models.CharField(max_length=100)
    api_secret = models.CharField(max_length=100)
    access_token = models.CharField(max_length=255, blank=True, null=True)
    
    # Global Limits
    breakout_max_trades = models.IntegerField(default=5) # Cash strategies ke liye
    breakdown_max_trades = models.IntegerField(default=5)
    momentum_max_trades = models.IntegerField(default=3) # Momentum ke liye
    
    # Time Ranges (MIS trading ke liye)
    breakout_start_time = models.TimeField(default="09:15")
    breakout_end_time = models.TimeField(default="15:15")
    breakdown_start_time = models.TimeField(default="09:15")
    breakdown_end_time = models.TimeField(default="15:15")
    
    # Advanced JSON Settings (Websockets aur Volume filters ke liye)
    # Isme 10-level volume filters aur tiered risk data save hota hai
    bull_volume_settings_json = models.JSONField(default=list, blank=True)
    bear_volume_settings_json = models.JSONField(default=list, blank=True)
    mom_bull_volume_settings = models.JSONField(default=list, blank=True)
    mom_bear_volume_settings = models.JSONField(default=list, blank=True)

    def __str__(self):
        return f"{self.user.username} ka Account"

class BaseTrade(models.Model):
    """
    Ek base class taaki code baar-baar na likhna pade.
    """
    STATUS_CHOICES = [
        ('OPEN', 'Open'),
        ('EXITED', 'Exited'),
        ('CANCELLED', 'Cancelled'),
    ]
    
    symbol = models.CharField(max_length=20)
    entry_price = models.FloatField()
    exit_price = models.FloatField(null=True, blank=True)
    quantity = models.IntegerField()
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='OPEN')
    
    # HFT Sync Fields
    order_id = models.CharField(max_length=50, blank=True, null=True) # Kite order ID
    initial_risk = models.FloatField(default=0.0) # Entry - StopLoss (at trigger time)
    step_size = models.FloatField(default=0.0)    # initial_risk * trailing_ratio
    sl_price = models.FloatField()                # Current Stop Loss in market
    target_price = models.FloatField()            # Current Target
    
    pnl = models.FloatField(default=0.0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True

class CashBreakoutTrade(BaseTrade):
    """Cash Bull Strategy Trades"""
    pass

class CashBreakdownTrade(BaseTrade):
    """Cash Bear Strategy Trades"""
    pass

class MomentumBullTrade(BaseTrade):
    """Momentum Bull Strategy Trades"""
    pass

class MomentumBearTrade(BaseTrade):
    """Momentum Bear Strategy Trades"""
    pass

class BannedSymbol(models.Model):
    """Symbols jinhe algo monitor nahi karega"""
    symbol = models.CharField(max_length=20, unique=True)
    reason = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.symbol