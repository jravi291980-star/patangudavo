from django.contrib import admin
from django.urls import path
from trading import views

urlpatterns = [
    path('admin/', admin.site.urls),
    
    # --- Dashboard API Endpoints ---
    path('api/stats/', views.dashboard_stats, name='dashboard_stats'),
    
    # Settings
    path('api/settings/global/', views.global_settings_view, name='global_settings'),
    path('api/settings/engine/<str:engine_type>/', views.engine_settings_view, name='engine_settings'),
    
    # Controls (Ban, Toggle, Panic)
    path('api/control/', views.control_action, name='control_action'),
    
    # Data
    path('api/orders/', views.get_orders, name='get_orders'),
    path('api/scanner/', views.get_scanner_data, name='get_scanner_data'),
]