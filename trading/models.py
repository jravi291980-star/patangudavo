from django.db import models
from django.contrib.auth.models import User

class Account(models.Model):
    """
    Kite API credentials aur strategy ki global settings store karne ke liye.
    """
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    is_master = models.BooleanField(default=False, help_text="Designate as master for fetching instruments")

    # API Credentials
    api_key = models.CharField(max_length=100)
    api_secret = models.CharField(max_length=100)
    access_token = models.CharField(max_length=500, blank=True, null=True)
    
    # Global Limits
    breakout_max_trades = models.IntegerField(default=5) 
    breakdown_max_trades = models.IntegerField(default=5)
    momentum_max_trades = models.IntegerField(default=3) 
    
    # Time Ranges
    breakout_start_time = models.TimeField(default="09:15")
    breakout_end_time = models.TimeField(default="15:15")
    breakdown_start_time = models.TimeField(default="09:15")
    breakdown_end_time = models.TimeField(default="15:15")
    
    # Advanced JSON Settings (10-level volume filters aur tiered risk data)
    bull_volume_settings_json = models.JSONField(default=list, blank=True)
    bear_volume_settings_json = models.JSONField(default=list, blank=True)
    mom_bull_volume_settings = models.JSONField(default=list, blank=True)
    mom_bear_volume_settings = models.JSONField(default=list, blank=True)
    updated_at = models.DateTimeField(auto_now=True)


    def __str__(self):
        return f"{self.user.username} ka Kite Account"

class BaseTrade(models.Model):
    """
    Abstract Base Model for HFT Sync
    """
    STATUS_CHOICES = [
        ('OPEN', 'Open'),
        ('EXITED', 'Exited'),
        ('CANCELLED', 'Cancelled'),
    ]
    
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    symbol = models.CharField(max_length=20)
    entry_price = models.FloatField()
    exit_price = models.FloatField(null=True, blank=True)
    qty = models.IntegerField() # Fixed name for Admin/Nexus compatibility
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='OPEN')
    
    # HFT Sync Fields (Nexus RAM monitoring ke liye zaruri)
    order_id = models.CharField(max_length=50, blank=True, null=True) 
    initial_risk = models.FloatField(default=0.0) 
    step_size = models.FloatField(default=0.0)    
    sl_price = models.FloatField()                
    target_price = models.FloatField()            
    
    pnl = models.FloatField(default=0.0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True

class CashBreakoutTrade(BaseTrade):
    pass

class CashBreakdownTrade(BaseTrade):
    pass

class MomentumBullTrade(BaseTrade):
    pass

class MomentumBearTrade(BaseTrade):
    pass

class BannedSymbol(models.Model):
    """Symbols jinhe algo monitor nahi karega"""
    symbol = models.CharField(max_length=20, unique=True)
    reason = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.symbol