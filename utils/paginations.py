from rest_framework import pagination


class UserResultPagination(pagination.PageNumberPagination):
    page_size = 50
    page_size_query_param = 'page_size'
    # max_page_size = 1000


class StandardResultPagination(pagination.PageNumberPagination):
    page_size = 100
    page_size_query_param = 'page_size'
    # max_page_size = 1000
