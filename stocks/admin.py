from django.contrib import admin

from .models import (
    Date,
    Ticker,
    Index,
    ETF,
    OHLCV,
    BuySell,
    Factor,
)

admin.site.register(Date)
admin.site.register(Ticker)
admin.site.register(Index)
admin.site.register(ETF)
admin.site.register(OHLCV)
admin.site.register(BuySell)
admin.site.register(Factor)
