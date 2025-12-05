from django import forms
from .models import Account

class AccountForm(forms.ModelForm):
    class Meta:
        model = Account
        fields = '__all__'
        widgets = {
            'api_key': forms.TextInput(attrs={'class': 'form-control'}),
            'access_token': forms.TextInput(attrs={'class': 'form-control'}),
            'breakout_start_time': forms.TimeInput(attrs={'type': 'time'}),
            'breakout_end_time': forms.TimeInput(attrs={'type': 'time'}),
            # Add widgets for other time fields as needed
        }