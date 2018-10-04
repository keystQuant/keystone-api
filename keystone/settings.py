import os
import raven

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SECRET_KEY = 'bfrb1tzc963g=dxdn10gvv66c7#02-j=fvd)*h*roo#^7jg^q!'

DEBUG = True

##### 새팅 상태 위에서 먼저 정의 #####
testing = os.environ.get('TRAVIS', 'False') # Travis에서 작동하는지 확인
###############################

# keyst-db-server: 45.76.218.34
ALLOWED_HOSTS = ['127.0.0.1', '127.0.1.1', '45.76.218.34']

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',

    # corsheaders
    'corsheaders',

    # Sentry: 에러 로깅
    'raven.contrib.django.raven_compat',

    # Django Restframework (API Template)
    'rest_framework',

    'accounts',
    # 'portfolio',
    'stocks',
]

### Sentry 새팅 ###
RAVEN_CONFIG = {
    'dsn': 'https://80460928d93740f5b85607eff1e4cab0:d18a1597d4b44952809b1e96ea162c86@sentry.io/1273289',
    'release': raven.fetch_git_sha(BASE_DIR),
}

MIDDLEWARE = [
    'corsheaders.middleware.CorsMiddleware', # corsheaders를 사용하려면 포함하기
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'keystone.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'keystone.wsgi.application'

if testing == 'True':
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
        }
    }
else:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql_psycopg2',
            'NAME': 'keystone',
            'USER': 'keystone',
            'PASSWORD': 'keystoneinvestmentpostgresql2018',
            'HOST': '45.76.218.34',
            'PORT': 5432,
            'TEST': {
                'NAME': 'test_keystone', # run "ALTER ROLE arbiter CREATEDB;" in psql
            },
        }
    }

### 서버 Redis 사용 ###
if testing == 'True':
    ### 실제 서비스 중에만 캐시 서버 사용 ###
    print('NO CACHE')
else:
    CACHES = {
        "default": {
            "BACKEND": "django_redis.cache.RedisCache",
            "LOCATION": "redis://:keystoneapiredispassword@redis:6379/1", # 1번 DB
            "OPTIONS": {
                "CLIENT_CLASS": "django_redis.client.DefaultClient",
            }
        }
    }

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'Asia/Seoul'
USE_I18N = True
USE_L10N = True
USE_TZ = False

### API 서버라 static은 필요없지만, rest framework에서 디버깅 용으로 사용이 필요하기 때문에 추가 ###
STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'static/')

# https://github.com/ottoyiu/django-cors-headers
CORS_ORIGIN_ALLOW_ALL = True # 외부에서 API 요청 가능하도록 새팅

REST_FRAMEWORK = {
    'DEFAULT_PERMISSION_CLASSES': (
        'rest_framework.permissions.IsAuthenticated',
    ),
    'DEFAULT_AUTHENTICATION_CLASSES': (
        'rest_framework_jwt.authentication.JSONWebTokenAuthentication',
        'rest_framework.authentication.SessionAuthentication',
        'rest_framework.authentication.BasicAuthentication',
    ),
}
