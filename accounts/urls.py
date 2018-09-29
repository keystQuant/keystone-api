from django.urls import re_path

from rest_framework_jwt.views import (
    obtain_jwt_token,
    refresh_jwt_token,
    verify_jwt_token,
)

from .views import (
    UserAPIView,
    UserDetailsAPIView,
    UserLoginAPIView,
    ProfileAPIView,
    ProfileDetailsAPIView,
)

urlpatterns = [
    # token maker
    re_path(r'^api-token-auth/', obtain_jwt_token),
    re_path(r'^api-token-refresh/', refresh_jwt_token),
    re_path(r'^api-token-verify/', verify_jwt_token),

    # basic user login, info urls
    re_path(r'^login/$', UserLoginAPIView.as_view(), name='login'),
    re_path(r'^user/$', UserAPIView.as_view(), name="user"),
    re_path(r'^user/(?P<username>[\w.@+-]+)/$', UserDetailsAPIView.as_view(), name="user-details"),

    # user profile related urls
    re_path(r'^profile/$', ProfileAPIView.as_view(), name="profile"),
    re_path(r'^profile/(?P<pk>[\w.@+-]+)/$', ProfileDetailsAPIView.as_view(), name="profile-details"),
]
