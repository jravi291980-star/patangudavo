# from django import forms
# from .models import Account

# class AccountForm(forms.ModelForm):
#     class Meta:
#         model = Account
#         fields = '__all__'
#         widgets = {
#             'api_key': forms.TextInput(attrs={'class': 'form-control'}),
#             'api_secret': forms.PasswordInput(attrs={'class': 'form-control', 'render_value': True}),
#             'access_token': forms.TextInput(attrs={'class': 'form-control'}),
#             'breakout_start_time': forms.TimeInput(attrs={'type': 'time'}),
#             'breakout_end_time': forms.TimeInput(attrs={'type': 'time'}),
#             'breakdown_start_time': forms.TimeInput(attrs={'type': 'time'}),
#             'breakdown_end_time': forms.TimeInput(attrs={'type': 'time'}),
#             'volume_settings_json': forms.Textarea(attrs={'rows': 4}),
#         }
from django import forms
from .models import Account

class AccountForm(forms.ModelForm):
    class Meta:
        model = Account
        fields = '__all__'
        widgets = {
            'api_key': forms.TextInput(attrs={'class': 'form-control'}),
            'api_secret': forms.PasswordInput(attrs={'class': 'form-control', 'render_value': True}),
            'access_token': forms.TextInput(attrs={'class': 'form-control'}),
            # Bull/Bear Times
            'breakout_start_time': forms.TimeInput(attrs={'type': 'time'}),
            'breakout_end_time': forms.TimeInput(attrs={'type': 'time'}),
            'breakdown_start_time': forms.TimeInput(attrs={'type': 'time'}),
            'breakdown_end_time': forms.TimeInput(attrs={'type': 'time'}),
            
            # JSON settings
            'bull_volume_settings_json': forms.Textarea(attrs={'rows': 4}),
            'bear_volume_settings_json': forms.Textarea(attrs={'rows': 4}),
            'mom_bull_volume_settings': forms.Textarea(attrs={'rows': 4}),
            'mom_bear_volume_settings': forms.Textarea(attrs={'rows': 4}),
        }