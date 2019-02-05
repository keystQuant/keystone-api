from django.urls import re_path

from .views import (
    GatewayView,

    DateAPIView,
    TickerAPIView,
    StockInfoAPIView,
    IndexAPIView,
    ETFAPIView,
    OHLCVAPIView,
    BuySellAPIView,
    MarketCapitalAPIView,
    FactorAPIView,
    MkfGroAPIView,
    MkfValAPIView,
)

urlpatterns = [
    re_path(r'^task/$', GatewayView.as_view(), name='gateway'),

    re_path(r'^date/$', DateAPIView.as_view(), name='date'),
    re_path(r'^ticker/$', TickerAPIView.as_view(), name='ticker'),
    re_path(r'^info/$', StockInfoAPIView.as_view(), name='stock-info'),
    re_path(r'^index/$', IndexAPIView.as_view(), name='index'),
    re_path(r'^etf/$', ETFAPIView.as_view(), name='etf'),
    re_path(r'^ohlcv/$', OHLCVAPIView.as_view(), name='ohlcv'),
    re_path(r'^buysell/$', BuySellAPIView.as_view(), name='buysell'),
    re_path(r'^mktcap/$', MarketCapitalAPIView.as_view(), name='mktcap'),
    re_path(r'^factor/$', FactorAPIView.as_view(), name='factor'),
    re_path(r'^mkfgro/$', MkfGroAPIView.as_view(), name='mkf-gro'),
    re_path(r'^mkfval/$', MkfValAPIView.as_view(), name='mkf-val'),
]
