# from django.contrib import admin
# from django.urls import path
# from trading import views

# urlpatterns = [
#     path('admin/', admin.site.urls),
#     path('', views.home, name='home'),  # <--- Add this line for the homepage
    
#     # --- Kite Connect Authentication ---
#     # These match the Dashboard's "Initiate Kite Login" button and Zerodha's redirect setting
#     path('api/kite/login/', views.kite_login, name='kite_login'),
#     path('api/kite/callback/', views.kite_callback, name='kite_callback'),
#     path('dashboard/', views.home, name='dashboard'),
#     # --- Dashboard API Endpoints ---
#     path('api/stats/', views.dashboard_stats, name='dashboard_stats'),
    
#     # Settings
#     path('api/settings/global/', views.global_settings_view, name='global_settings'),
#     path('api/settings/engine/<str:engine_type>/', views.engine_settings_view, name='engine_settings'),
    
#     # Controls (Ban, Toggle, Panic)
#     path('api/control/', views.control_action, name='control_action'),
    
#     # Data
#     path('api/orders/', views.get_orders, name='get_orders'),
#     path('api/scanner/', views.get_scanner_data, name='get_scanner_data'),
# ]

from django.contrib import admin
from django.urls import path
from trading import views

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', views.home, name='home'),
    path('dashboard/', views.home, name='dashboard'),

    # --- Kite Connect Authentication ---
    path('api/kite/login/', views.kite_login, name='kite_login'),
    path('api/kite/callback/', views.kite_callback, name='kite_callback'),

    # --- Dashboard API Endpoints ---
    # Used for P&L, Status, and Heartbeat
    path('api/stats/', views.dashboard_stats, name='dashboard_stats'),
    
    # Settings (Dynamic mapping for bull/bear/mom_bull/mom_bear)
    path('api/settings/global/', views.global_settings_view, name='global_settings'),
    path('api/settings/engine/<str:engine_type>/', views.engine_settings_view, name='engine_settings'),
    
    # Controls (Ban, Toggle, Panic, Exit Trade)
    path('api/control/', views.control_action, name='control_action'),
    
    # Data (Orders Table and Scanner List)
    path('api/orders/', views.get_orders, name='get_orders'),
    path('api/scanner/', views.get_scanner_data, name='get_scanner_data'),
]