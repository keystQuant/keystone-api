from django.http import JsonResponse
from django.views.generic import View

from rest_framework import generics, permissions
from rest_framework.filters import SearchFilter, OrderingFilter

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
)
from .serializers import (
    DateSerializer,
    TickerSerializer,
    StockInfoSerializer,
    IndexSerializer,
    ETFSerializer,
    OHLCVSerializer,
    BuySellSerializer,
    MarketCapitalSerializer,
    FactorSerializer,
)
from .reducers import Reducers

from utils.paginations import StandardResultPagination


class GatewayView(View):
    def get(self, request):
        action_type = request.GET.get('type')
        env_type = request.GET.get('env')
        if not env_type:
            # 테스팅할 때는 local이라고 env_type을 넣어줘야한다
            env_type = 'remote'
        reducer_inst = Reducers(action_type, env_type)

        if reducer_inst.has_reducer():
            status = reducer_inst.reduce()
            if status == True:
                return JsonResponse({'status': 'DONE'}, json_dumps_params={'ensure_ascii': True})
            elif status == False:
                return JsonResponse({'status': 'FAIL'}, json_dumps_params={'ensure_ascii': True})
        else: # 리듀서가 존재하지 않는다면
            return JsonResponse({'status': 'NO ACTION: {}'.format(action_type)}, json_dumps_params={'ensure_ascii': True})


class DateAPIView(generics.ListAPIView):
    queryset = Date.objects.all().order_by('-date')
    serializer_class = DateSerializer
    permission_classes = (permissions.AllowAny,)
    pagination_class = StandardResultPagination
    filter_backends = [SearchFilter, OrderingFilter]


class TickerAPIView(generics.ListAPIView):
    queryset = Ticker.objects.all()
    serializer_class = TickerSerializer
    permission_classes = (permissions.AllowAny,)
    pagination_class = StandardResultPagination
    filter_backends = [SearchFilter, OrderingFilter]

    def get_queryset(self, *args, **kwargs):
        queryset = Ticker.objects.all()
        date_by = self.request.GET.get('date')
        code_by = self.request.GET.get('code')
        name_by = self.request.GET.get('name')
        if date_by:
            queryset = queryset.filter(date=date_by)
        if code_by:
            queryset = queryset.filter(code=code_by)
        if name_by:
            queryset = queryset.filter(name=name_by)
        return queryset


class StockInfoAPIView(generics.ListAPIView):
    queryset = StockInfo.objects.all()
    serializer_class = StockInfoSerializer
    permission_classes = (permissions.AllowAny,)
    pagination_class = StandardResultPagination
    filter_backends = [SearchFilter, OrderingFilter]

    def get_queryset(self, *args, **kwargs):
        queryset = StockInfo.objects.all().order_by('id')
        date_by = self.request.GET.get('date')
        start = self.request.GET.get('start')
        end = self.request.GET.get('end')
        code_by = self.request.GET.get('code')
        name_by = self.request.GET.get('name')
        if date_by:
            queryset = queryset.filter(date=date_by)
        if start and end and not date_by:
            queryset = queryset.filter(date__gte=start).filter(date__lte=end)
        if code_by:
            queryset = queryset.filter(code=code_by)
        if name_by:
            queryset = queryset.filter(name=name_by)
        return queryset


class IndexAPIView(generics.ListAPIView):
    queryset = Index.objects.all()
    serializer_class = IndexSerializer
    permission_classes = (permissions.AllowAny,)
    pagination_class = StandardResultPagination
    filter_backends = [SearchFilter, OrderingFilter]

    def get_queryset(self, *args, **kwargs):
        queryset = Index.objects.all().order_by('id')
        date_by = self.request.GET.get('date')
        start = self.request.GET.get('start')
        end = self.request.GET.get('end')
        code_by = self.request.GET.get('code')
        name_by = self.request.GET.get('name')
        if date_by:
            queryset = queryset.filter(date=date_by)
        if start and end and not date_by:
            queryset = queryset.filter(date__gte=start).filter(date__lte=end)
        if code_by:
            queryset = queryset.filter(code=code_by)
        if name_by:
            queryset = queryset.filter(name=name_by)
        return queryset


class ETFAPIView(generics.ListAPIView):
    queryset = ETF.objects.all()
    serializer_class = ETFSerializer
    permission_classes = (permissions.AllowAny,)
    pagination_class = StandardResultPagination
    filter_backends = [SearchFilter, OrderingFilter]

    def get_queryset(self, *args, **kwargs):
        queryset = ETF.objects.all().order_by('id')
        date_by = self.request.GET.get('date')
        start = self.request.GET.get('start')
        end = self.request.GET.get('end')
        code_by = self.request.GET.get('code')
        name_by = self.request.GET.get('name')
        if date_by:
            queryset = queryset.filter(date=date_by)
        if start and end and not date_by:
            queryset = queryset.filter(date__gte=start).filter(date__lte=end)
        if code_by:
            queryset = queryset.filter(code=code_by)
        if name_by:
            queryset = queryset.filter(name=name_by)
        return queryset


class OHLCVAPIView(generics.ListAPIView):
    queryset = OHLCV.objects.all()
    serializer_class = OHLCVSerializer
    permission_classes = (permissions.AllowAny,)
    pagination_class = StandardResultPagination
    filter_backends = [SearchFilter, OrderingFilter]

    def get_queryset(self, *args, **kwargs):
        queryset = OHLCV.objects.all().order_by('id')
        date_by = self.request.GET.get('date')
        start = self.request.GET.get('start')
        end = self.request.GET.get('end')
        code_by = self.request.GET.get('code')
        name_by = self.request.GET.get('name')
        if date_by:
            queryset = queryset.filter(date=date_by)
        if start and end and not date_by:
            queryset = queryset.filter(date__gte=start).filter(date__lte=end)
        if code_by:
            queryset = queryset.filter(code=code_by)
        if name_by:
            queryset = queryset.filter(name=name_by)
        return queryset


class BuySellAPIView(generics.ListAPIView):
    queryset = BuySell.objects.all()
    serializer_class = BuySellSerializer
    permission_classes = (permissions.AllowAny,)
    pagination_class = StandardResultPagination
    filter_backends = [SearchFilter, OrderingFilter]

    def get_queryset(self, *args, **kwargs):
        queryset = BuySell.objects.all().order_by('id')
        date_by = self.request.GET.get('date')
        start = self.request.GET.get('start')
        end = self.request.GET.get('end')
        code_by = self.request.GET.get('code')
        name_by = self.request.GET.get('name')
        if date_by:
            queryset = queryset.filter(date=date_by)
        if start and end and not date_by:
            queryset = queryset.filter(date__gte=start).filter(date__lte=end)
        if code_by:
            queryset = queryset.filter(code=code_by)
        if name_by:
            queryset = queryset.filter(name=name_by)
        return queryset


class MarketCapitalAPIView(generics.ListAPIView):
    queryset = MarketCapital.objects.all()
    serializer_class = MarketCapitalSerializer
    permission_classes = (permissions.AllowAny,)
    pagination_class = StandardResultPagination
    filter_backends = [SearchFilter, OrderingFilter]

    def get_queryset(self, *args, **kwargs):
        queryset = MarketCapital.objects.all().order_by('id')
        date_by = self.request.GET.get('date')
        start = self.request.GET.get('start')
        end = self.request.GET.get('end')
        code_by = self.request.GET.get('code')
        name_by = self.request.GET.get('name')
        if date_by:
            queryset = queryset.filter(date=date_by)
        if start and end and not date_by:
            queryset = queryset.filter(date__gte=start).filter(date__lte=end)
        if code_by:
            queryset = queryset.filter(code=code_by)
        if name_by:
            queryset = queryset.filter(name=name_by)
        return queryset


class FactorAPIView(generics.ListAPIView):
    queryset = Factor.objects.all()
    serializer_class = FactorSerializer
    permission_classes = (permissions.AllowAny,)
    pagination_class = StandardResultPagination
    filter_backends = [SearchFilter, OrderingFilter]

    def get_queryset(self, *args, **kwargs):
        queryset = Factor.objects.all().order_by('id')
        date_by = self.request.GET.get('date')
        start = self.request.GET.get('start')
        end = self.request.GET.get('end')
        code_by = self.request.GET.get('code')
        name_by = self.request.GET.get('name')
        if date_by:
            queryset = queryset.filter(date=date_by)
        if start and end and not date_by:
            queryset = queryset.filter(date__gte=start).filter(date__lte=end)
        if code_by:
            queryset = queryset.filter(code=code_by)
        if name_by:
            queryset = queryset.filter(name=name_by)
        return queryset
