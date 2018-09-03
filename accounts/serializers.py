from django.contrib.auth import get_user_model
from rest_framework import serializers
from rest_framework.serializers import ValidationError

from accounts.models import Profile

User = get_user_model()


class UserSerializer(serializers.HyperlinkedModelSerializer):
    profile = serializers.HyperlinkedRelatedField(view_name='accounts:profile-details', read_only=True)

    class Meta:
        model = User
        fields = ('id',
                  'username',
                  'email',
                  'password',
                  'last_login',
                  'profile',)
        read_only_fields = ('last_login',)
        extra_kwargs = {
            'password': {
                'write_only': True
            }
        }

    def create(self, validated_data):
        username = validated_data['username']
        email = validated_data['email']
        password = validated_data['password']
        user_obj = User(
            username = username,
            email = email
        )
        user_obj.set_password(password)
        user_obj.save()
        return user_obj


class UserLoginSerializer(serializers.ModelSerializer):
    username = serializers.CharField()

    class Meta:
        model = User
        fields = ('username', 'password',)
        extra_kwargs = {
            "password": {
                "write_only": True
            }
        }

    def validate(self, data):
        username = data.get('username')
        password = data['password']
        if not username:
            raise ValidationError('유저 아이디를 적어주세요.')
        user_qs = User.objects.filter(username=username)
        if user_qs.exists():
            user = user_qs.first()
            if not user.check_password(password):
                raise ValidationError('잘못된 비밀번호입니다. 다시 시도해주세요.')
            else:
                return data
        else:
            raise ValidationError('없는 유저 아이디입니다.')


class ProfileSerializer(serializers.HyperlinkedModelSerializer):
    user = UserSerializer(read_only=True)

    class Meta:
        model = Profile
        fields = ('user',
                  'name',
                  'phone',
                  'address',
                  'updated',)
