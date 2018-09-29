from django.conf import settings
from django.db import models

from accounts.models import Profile
from stocks.models import Ticker

PORTFOLIO_TYPES = (
    ('S', 'Stock'),
    ('CS', 'Cash + Stock'),
)

STATUS = (
    ('B', 'Bought'),
    ('S', 'Sold'),
)


class Portfolio(models.Model):
    user = models.ForeignKey(Profile,
                             on_delete=models.CASCADE,
                             related_name='portfolio')
    name = models.CharField(max_length=100,
                            blank=True,
                            null=True)
    capital = models.BigIntegerField(blank=True, null=True)
    portfolio_type = models.CharField(max_length=2,
                                      choices=PORTFOLIO_TYPES,
                                      blank=True,
                                      null=True)
    description = models.CharField(max_length=120,
                                   blank=True,
                                   null=True)
    created = models.DateTimeField(auto_now_add=True)
    updated = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.user.user.username


class PortfolioHistory(models.Model):
    portfolio = models.ForeignKey(Portfolio, related_name='history')
    date = models.CharField(max_length=8)
    code = models.ForeignKey(Ticker, related_name='portfolio_record')
    status = models.CharField(max_length=1, choices=STATUS)
    quantity = models.IntegerField(default=1)
    price = models.IntegerField()

    def __str__(self):
        return self.portfolio.name
