import logging
from django.http import HttpResponseForbidden, HttpResponse
from django.core.cache import cache
from django.utils.deprecation import MiddlewareMixin

logger = logging.getLogger(__name__)

class SimpleIpRateLimitMiddleware(MiddlewareMixin):
    """
    A simple IP rate limiter using Django's cache.
    Limits requests to sensitive paths (like login).
    """
    def process_request(self, request):
        # Only rate limit POST requests to login
        if request.path == '/login/' and request.method == 'POST':
            ip = self.get_client_ip(request)
            key = f"rl:{ip}"
            count = cache.get(key, 0)
            
            if count >= 10:  # Limit: 10 attempts
                logger.warning(f"Rate limit exceeded for IP: {ip}")
                return HttpResponseForbidden("Too many requests. Please try again later.")
            
            cache.set(key, count + 1, timeout=60) # Reset count every 60 seconds
        return None

    def get_client_ip(self, request):
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[0]
        else:
            ip = request.META.get('REMOTE_ADDR')
        return ip

class BlockWordpressScansMiddleware(MiddlewareMixin):
    """
    Blocks common automated scans for PHP/WordPress files to reduce log noise.
    """
    def process_request(self, request):
        path = request.path.lower()
        if any(x in path for x in ['.php', 'wp-admin', 'wp-login', '.env', '.git']):
            return HttpResponse("Not Found", status=404)
        return None

class SubscriptionCheckMiddleware(MiddlewareMixin):
    """
    Placeholder for checking user subscription status.
    Currently allows all requests.
    """
    def process_request(self, request):
        # Logic to check if request.user has valid subscription
        # For now, we pass everything through.
        return None

class AdminAccessMiddleware(MiddlewareMixin):
    """
    Restricts Admin panel access to Superusers only.
    """
    def process_request(self, request):
        if request.path.startswith('/admin/'):
            if not request.user.is_authenticated:
                # Let Django admin handle the login page redirection
                return None
            
            if not request.user.is_superuser:
                return HttpResponseForbidden("Access Denied: Admins only.")
        return None