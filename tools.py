import os, sys, glob

start_path = os.getcwd()
proj_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "keystone.settings")
sys.path.append(proj_path)
os.chdir(proj_path)

from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()

### scripts ###
from utils.cache import RedisClient
from tools.backup_db import BackupDB

if sys.argv[1] == 'backup-db':
    b = BackupDB()
    b.backup()

if sys.argv[1] == 'clean-tasks':
    ## git push를 하고 서버에 업데이트된 코드를 적용시키면, 실행중이던 태스크가 멈추는 수가 있다
    ## 강제로 태스크 실행이 종료되었음을 알리기 위해 실행시킨다
    r = RedisClient()
    r.set_key('TASK_IN_PROGRESS', 'False')
    print('실행중이던 태스크가 종료되었다고 캐시 서버에 알립니다.')
