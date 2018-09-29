from django.contrib.auth import get_user_model
from rest_framework import generics, permissions
from rest_framework.authtoken.models import Token
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST
from rest_framework.views import APIView

from .serializers import (
    UserSerializer,
    UserLoginSerializer,
    ProfileSerializer,
)

from .models import Profile
# from utils.permissions import IsOwnerOrReadOnly
from utils.paginations import UserResultPagination, StandardResultPagination

User = get_user_model()


## Issue: None
class UserAPIView(generics.ListCreateAPIView):
    queryset = User.objects.get_queryset().order_by('-id') # shows recent user first in list
                                                           # (solves 'UnorderedObjectListWarning')
    serializer_class = UserSerializer
    pagination_class = UserResultPagination
    # doesn't need permissions, anyone should be able to create new users
    permission_classes = (permissions.AllowAny,)

## Issue: None
class UserDetailsAPIView(generics.RetrieveUpdateDestroyAPIView):
    queryset = User.objects.all()
    serializer_class = UserSerializer
    lookup_field = 'username'
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)


## Issue: None
class UserLoginAPIView(APIView):
    serializer_class = UserLoginSerializer
    permission_classes = (permissions.AllowAny,) # anyone should be able to try and logon

    def post(self, request, *args, **kwargs):
        data = request.data
        serializer = UserLoginSerializer(data=data)
        if serializer.is_valid(raise_exception=True):
            username = serializer.data['username']
            user = User.objects.filter(username=username).first()
            token = Token.objects.filter(user=user).first().key
            return Response({'token': token}, status=HTTP_200_OK)
        return Response(serializer.errors, status=HTTP_400_BAD_REQUEST)


## Issue: None
class ProfileAPIView(generics.ListCreateAPIView):
    queryset = Profile.objects.get_queryset().order_by('user') # shows profiles in alphabetical order
    serializer_class = ProfileSerializer
    pagination_class = UserResultPagination
    # shouldn't have permissions, User will automatically create profile on registration
    # since the user is not created yet, you should drop permissions

    def perform_create(self, serializer):
        serializer.save(user=self.request.user)


## Issue: None
class ProfileDetailsAPIView(generics.RetrieveUpdateDestroyAPIView):
    queryset = Profile.objects.all()
    serializer_class = ProfileSerializer
