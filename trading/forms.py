from django import forms
from .models import Account

class AccountForm(forms.ModelForm):
    """
    Kite Account aur HFT Strategy settings ko dashboard se manage karne ke liye form.
    """
    class Meta:
        model = Account
        fields = [
            'api_key', 'api_secret', 'access_token',
            'breakout_max_trades', 'breakdown_max_trades', 'momentum_max_trades',
            'breakout_start_time', 'breakout_end_time', 
            'breakdown_start_time', 'breakdown_end_time',
            'bull_volume_settings_json', 'bear_volume_settings_json',
            'mom_bull_volume_settings', 'mom_bear_volume_settings'
        ]
        
        widgets = {
            # API Credentials
            'api_key': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Enter API Key'}),
            'api_secret': forms.PasswordInput(attrs={'class': 'form-control', 'render_value': True, 'placeholder': 'Enter API Secret'}),
            'access_token': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Access Token (Auto-generated)'}),

            # Trade Limits (Numbers)
            'breakout_max_trades': forms.NumberInput(attrs={'class': 'form-control'}),
            'breakdown_max_trades': forms.NumberInput(attrs={'class': 'form-control'}),
            'momentum_max_trades': forms.NumberInput(attrs={'class': 'form-control'}),

            # Time Pickers (HTML5 Time Input)
            'breakout_start_time': forms.TimeInput(attrs={'class': 'form-control', 'type': 'time'}),
            'breakout_end_time': forms.TimeInput(attrs={'class': 'form-control', 'type': 'time'}),
            'breakdown_start_time': forms.TimeInput(attrs={'class': 'form-control', 'type': 'time'}),
            'breakdown_end_time': forms.TimeInput(attrs={'class': 'form-control', 'type': 'time'}),

            # JSON Textareas for Volume & Risk Criteria
            'bull_volume_settings_json': forms.Textarea(attrs={'class': 'form-control', 'rows': 5, 'placeholder': 'Paste Bull Volume JSON here...'}),
            'bear_volume_settings_json': forms.Textarea(attrs={'class': 'form-control', 'rows': 5, 'placeholder': 'Paste Bear Volume JSON here...'}),
            'mom_bull_volume_settings': forms.Textarea(attrs={'class': 'form-control', 'rows': 5, 'placeholder': 'Paste Momentum Bull JSON here...'}),
            'mom_bear_volume_settings': forms.Textarea(attrs={'class': 'form-control', 'rows': 5, 'placeholder': 'Paste Momentum Bear JSON here...'}),
        }

    def clean_bull_volume_settings_json(self):
        """JSON format ko validate karne ke liye (Optional but recommended)"""
        data = self.cleaned_data['bull_volume_settings_json']
        # Agar field JSONField hai toh Django automatic handle kar leta hai, 
        # par custom validation yaha daali ja sakti hai.
        return data