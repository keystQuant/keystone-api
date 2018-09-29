from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import path, include, re_path

urlpatterns = [
    path('admin/', admin.site.urls),

    re_path(r'^api/v1/accounts/', include('accounts.urls')),
    # re_path(r'^api/v1/portfolio/', include('portfolio.urls')),
    re_path(r'^api/v1/stocks/', include('stocks.urls')),
]

if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
