from rest_framework import serializers

from .models import (
    Date,
    Ticker,
    StockInfo,
    Index,
    ETF,
    OHLCV,
    BuySell,
    MarketCapital,
    Factor,
    MkfGro,
    MkfVal,
)


class DateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Date
        fields = '__all__'


class TickerSerializer(serializers.ModelSerializer):
    class Meta:
        model = Ticker
        fields = '__all__'


class StockInfoSerializer(serializers.ModelSerializer):
    class Meta:
        model = StockInfo
        fields = '__all__'


class IndexSerializer(serializers.ModelSerializer):
    class Meta:
        model = Index
        fields = '__all__'


class ETFSerializer(serializers.ModelSerializer):
    class Meta:
        model = ETF
        fields = '__all__'


class OHLCVSerializer(serializers.ModelSerializer):
    class Meta:
        model = OHLCV
        fields = '__all__'


class BuySellSerializer(serializers.ModelSerializer):
    class Meta:
        model = BuySell
        fields = '__all__'


class MarketCapitalSerializer(serializers.ModelSerializer):
    class Meta:
        model = MarketCapital
        fields = '__all__'


class FactorSerializer(serializers.ModelSerializer):
    class Meta:
        model = Factor
        fields = '__all__'


class MkfGroSerializer(serializers.ModelSerializer):
    class Meta:
        model = MkfGro
        fields = '__all__'


class MkfValSerializer(serializers.ModelSerializer):
    class Meta:
        model = MkfVal
        fields = '__all__'
