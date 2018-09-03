'''
****** TASK MANAGER USING FABRIC/PYTHON ******

RUN: "fab <taskname>"
(ex) fab shell
(the tasknames are defined below, check the codes below for more information)

###### LOCAL FAB TASKS ######
==> can run on local computer/servers
shell
runserver
static
new_static
migrate
test
lazy_commit
server_reload
restart_celery
js_gobble_reinstall
clean_known_hosts
start_browser
remove_celery_migrations

###### OPEN SHELL TASKS ######
==> opens up shell as user: arbiter, in this case
init_web_shell
init_db_shell
init_cache_shell
init_gateway_shell
init_gobble_shell
init_mined_shell
root_web_shell
root_db_shell
root_cache_shell
root_gateway_shell
root_gobble_shell
root_mined_shell
web_shell
db_shell
cache_shell
gateway_shell
gobble_shell
mined_shell

###### INITIAL DEPLOY TASKS ######
init_servers

###### SERVER SPECIFIC TASKS ######
monitor_redis
apply_changes:commit_msg
apply_ws_changes:commit_msg
ws_reinstall
apply_gobblejs_changes:commit_msg
gobblejs_reinstall
apply_all_changes:commit_msg
get_db_backups
'''

from fabric.api import *

local_ip = '127.0.0.1'


###### LOCAL FAB TASKS ######
@task
@hosts(local_ip)
def shell():
    # opens the shell
    local('python manage.py shell')

@task
@hosts(local_ip)
def runserver():
    # runs the Django server
    local('python manage.py runserver')

@task
@hosts(local_ip)
def static():
    # collects static files again
    local('python manage.py collectstatic')

@task
@hosts(local_ip)
def new_static():
    # removes static-dist directory and collects static again
    local('rm -r static-dist')
    local('mkdir static-dist')
    local('python manage.py collectstatic')

@task
@hosts(local_ip)
def migrate():
    # make migrations on DB then migrate those changes
    local('python manage.py makemigrations')
    local('python manage.py migrate')

@task
@hosts(local_ip)
def test():
    # perform Django tests
    local('python manage.py test')

@task
@hosts(local_ip)
def lazy_commit():
    # git add . > git commit then git pushes changes lazily
    with settings(warn_only=True):
        local('git add .')
        local('git commit -m "commiting lazily: minor change only"')
    local('git push')

@task
@hosts(local_ip)
def server_reload():
    # local server reload
    local('sudo systemctl restart uwsgi')
    local('sudo systemctl restart nginx')

@task
@hosts(local_ip)
def restart_celery():
    # local celery/celerybeat restart
    # first migrates all changes
    execute(migrate)
    local('sudo supervisorctl restart arbiter_celery')
    local('sudo supervisorctl restart arbiter_celerybeat')

@task
@hosts(local_ip)
def js_gobble_reinstall():
    # reinstalls js-gobble app
    local('rm -r /home/arbiter/js-gobble')
    local('sudo bash /home/arbiter/buzzz/js-gobble/setup.sh')

@task
@hosts(local_ip)
def clean_known_hosts():
    # for mac users
    # delete all known host records
    local('echo "" > /Users/abc/.ssh/known_hosts')

@task
@hosts(local_ip)
def start_browser():
    local('node ./js-gobble/START_BROWSER.js')

@task
@hosts(local_ip)
def remove_celery_migrations():
    local('rm /home/arbiter/venv/buzzz/lib/python3.5/site-packages/django_celery_beat/migrations/00*')


# ###### OPEN SHELL TASKS ######
# @task
# @hosts(root_web)
# def init_web_shell():
#     env.password = web_pw
#     open_shell()
#
# @task
# @hosts(root_db)
# def init_db_shell():
#     env.password = db_pw
#     open_shell()
#
# @task
# @hosts(root_cache)
# def init_cache_shell():
#     env.password = cache_pw
#     open_shell()
#
# @task
# @hosts(root_gateway)
# def init_gateway_shell():
#     env.password = gateway_pw
#     open_shell()
#
# @task
# @hosts(root_gobble)
# def init_gobble_shell():
#     env.password = gobble_pw
#     open_shell()
#
# @task
# @hosts(root_mined)
# def init_mined_shell():
#     env.password = mined_pw
#     open_shell()
#
# @task
# @hosts(root_web)
# def root_web_shell():
#     env.password = root_pw
#     open_shell()
#
# @task
# @hosts(root_db)
# def root_db_shell():
#     env.password = root_pw
#     open_shell()
#
# @task
# @hosts(root_cache)
# def root_cache_shell():
#     env.password = root_pw
#     open_shell()
#
# @task
# @hosts(root_gateway)
# def root_gateway_shell():
#     env.password = root_pw
#     open_shell()
#
# @task
# @hosts(root_gobble)
# def root_gobble_shell():
#     env.password = root_pw
#     open_shell()
#
# @task
# @hosts(root_mined)
# def root_mined_shell():
#     env.password = root_pw
#     open_shell()
#
# @task
# @hosts(arbiter_web)
# def web_shell():
#     env.password = arbiter_pw
#     open_shell()
#
# @task
# @hosts(arbiter_db)
# def db_shell():
#     env.password = arbiter_pw
#     open_shell()
#
# @task
# @hosts(arbiter_cache)
# def cache_shell():
#     env.password = arbiter_pw
#     open_shell()
#
# @task
# @hosts(arbiter_gateway)
# def gateway_shell():
#     env.password = arbiter_pw
#     open_shell()
#
# @task
# @hosts(arbiter_gobble)
# def gobble_shell():
#     env.password = arbiter_pw
#     open_shell()
#
# @task
# @hosts(arbiter_mined)
# def mined_shell():
#     env.password = arbiter_pw
#     open_shell()
#
#
# ###### INITIAL DEPLOY TASKS ######
# @task
# @hosts(env.hosts[1:7])
# def init_servers():
#     server_init()
#
#
# ###### SERVER SPECIFIC TASKS ######
# @task
# @hosts(root_cache)
# def monitor_redis():
#     env.password = root_pw
#     run('redis-cli -a da56038fa453c22d2c46e83179126e97d4d272d02ece83eb83a97357e842d065 monitor')
#
# @task
# @hosts(root_web)
# def apply_changes_to_web():
#     env.user = 'root'
#     env.password = root_pw
#     virtualenv = '/home/arbiter/venv/buzzz/bin/python'
#     pip = '/home/arbiter/venv/buzzz/bin/pip'
#     with cd('/home/arbiter/buzzz/'):
#         run('{} tools.py cleanmigrations'.format(virtualenv))
#         run('git pull')
#         run('{} install -r requirements.txt'.format(pip))
#         run('{} manage.py makemigrations'.format(virtualenv))
#         run('{} manage.py migrate'.format(virtualenv))
#         run('sudo systemctl restart uwsgi')
#         run('sudo systemctl restart nginx')
#
# @task
# @hosts(root_db)
# def apply_changes_to_db():
#     env.user = 'root'
#     env.password = root_pw
#     virtualenv = '/home/arbiter/venv/buzzz/bin/python'
#     pip = '/home/arbiter/venv/buzzz/bin/pip'
#     with cd('/home/arbiter/buzzz/'):
#         run("vim +\":%s/THIS_SYSTEM = 'db'/THIS_SYSTEM = 'web'/g | wq\" ./arbiter/config.py")
#         run('{} tools.py cleanmigrations'.format(virtualenv))
#         run('git pull')
#         run('{} install -r requirements.txt'.format(pip))
#         run("vim +\":%s/THIS_SYSTEM = 'web'/THIS_SYSTEM = 'db'/g | wq\" ./arbiter/config.py")
#         run('{} manage.py makemigrations'.format(virtualenv))
#         run('{} manage.py migrate'.format(virtualenv))
#         run('sudo systemctl restart uwsgi')
#         run('sudo systemctl restart nginx')
#
# @task
# @hosts(root_gateway)
# def apply_changes_to_gateway():
#     env.user = 'root'
#     env.password = root_pw
#     virtualenv = '/home/arbiter/venv/buzzz/bin/python'
#     pip = '/home/arbiter/venv/buzzz/bin/pip'
#     with cd('/home/arbiter/buzzz/'):
#         run("vim +\":%s/THIS_SYSTEM = 'gateway'/THIS_SYSTEM = 'web'/g | wq\" ./arbiter/config.py")
#         run('{} tools.py cleanmigrations'.format(virtualenv))
#         run('git pull')
#         run('{} install -r requirements.txt'.format(pip))
#         run("vim +\":%s/THIS_SYSTEM = 'web'/THIS_SYSTEM = 'gateway'/g | wq\" ./arbiter/config.py")
#         run('{} manage.py makemigrations'.format(virtualenv))
#         run('{} manage.py migrate'.format(virtualenv))
#         run('sudo systemctl restart uwsgi')
#         run('sudo systemctl restart nginx')
#
# @task
# @hosts(root_gobble)
# def apply_changes_to_gobble():
#     env.user = 'root'
#     env.password = root_pw
#     virtualenv = '/home/arbiter/venv/buzzz/bin/python'
#     pip = '/home/arbiter/venv/buzzz/bin/pip'
#     with cd('/home/arbiter/buzzz/'):
#         run("vim +\":%s/THIS_SYSTEM = 'gobble'/THIS_SYSTEM = 'web'/g | wq\" ./arbiter/config.py")
#         run('{} tools.py cleanmigrations'.format(virtualenv))
#         run('git pull')
#         run('{} install -r requirements.txt'.format(pip))
#         run("vim +\":%s/THIS_SYSTEM = 'web'/THIS_SYSTEM = 'gobble'/g | wq\" ./arbiter/config.py")
#         run('{} manage.py makemigrations'.format(virtualenv))
#         run('{} manage.py migrate'.format(virtualenv))
#         # restart Rabbitmq task queue and Celery/Celerybeat then server
#         run('sudo systemctl restart rabbitmq-server')
#         run('sudo supervisorctl restart arbiter_celery')
#         run('sudo supervisorctl restart arbiter_celerybeat')
#         run('sudo systemctl restart uwsgi')
#         run('sudo systemctl restart nginx')
#
# @task
# @hosts(root_mined)
# def apply_changes_to_mined():
#     env.user = 'root'
#     env.password = root_pw
#     virtualenv = '/home/arbiter/venv/buzzz/bin/python'
#     pip = '/home/arbiter/venv/buzzz/bin/pip'
#     with cd('/home/arbiter/buzzz/'):
#         run("vim +\":%s/THIS_SYSTEM = 'mined'/THIS_SYSTEM = 'web'/g | wq\" ./arbiter/config.py")
#         run('{} tools.py cleanmigrations'.format(virtualenv))
#         run('git pull')
#         run('{} install -r requirements.txt'.format(pip))
#         run("vim +\":%s/THIS_SYSTEM = 'web'/THIS_SYSTEM = 'mined'/g | wq\" ./arbiter/config.py")
#         run('{} manage.py makemigrations'.format(virtualenv))
#         run('{} manage.py migrate'.format(virtualenv))
#         # restart Rabbitmq task queue and Celery/Celerybeat then server
#         run('sudo systemctl restart rabbitmq-server')
#         run('sudo supervisorctl restart arbiter_celery')
#         run('sudo supervisorctl restart arbiter_celerybeat')
#         run('sudo systemctl restart uwsgi')
#         run('sudo systemctl restart nginx')
#
# @task
# @hosts(local_ip)
# def apply_changes(commit_msg):
#     # git pushes all the changes on your devlepment machines
#     # and applies those changes to your other servers
#     env.user = 'root'
#     env.password = root_pw
#     # git add . > git commit then git pushes changes lazily
#     execute(migrate)
#     execute(static)
#     with settings(warn_only=True):
#         local('git add -A')
#         local('git commit -m "{}"'.format(commit_msg))
#     local('git push')
#     execute(apply_changes_to_web)
#     execute(apply_changes_to_db)
#     execute(apply_changes_to_gateway)
#     execute(apply_changes_to_gobble)
#     execute(apply_changes_to_mined)
#
# @task
# @hosts(root_gateway)
# def apply_ws_changes(commit_msg):
#     # git pushes all the changes on your devlepment machines
#     # and applies those changes to your other servers
#     env.user = 'root'
#     env.password = root_pw
#     # git add . > git commit then git pushes changes lazily
#     execute(static)
#     with settings(warn_only=True):
#         local('git add -A')
#         local('git commit -m "{}"'.format(commit_msg))
#     local('git push')
#     # reinstall ws-server on gateway server
#     with cd('/home/arbiter/buzzz/ws-server/'):
#         run('sudo bash reinstall.sh')
#
# @task
# @hosts(root_gateway)
# def ws_reinstall():
#     env.user = 'root'
#     env.password = root_pw
#     # reinstall ws-server on gateway server
#     with cd('/home/arbiter/buzzz/ws-server/'):
#         run('sudo bash reinstall.sh')
#
# @task
# @hosts(root_gobble)
# def apply_gobblejs_changes(commit_msg):
#     # git pushes all the changes on your devlepment machines
#     # and applies those changes to your other servers
#     env.user = 'root'
#     env.password = root_pw
#     # git add . > git commit then git pushes changes lazily
#     execute(static)
#     with settings(warn_only=True):
#         local('git add -A')
#         local('git commit -m "{}"'.format(commit_msg))
#     local('git push')
#     # reinstall js-gobble on gateway server
#     with cd('/home/arbiter/buzzz/js-gobble/'):
#         run('sudo bash reinstall.sh')
#
# @task
# @hosts(root_gobble)
# def gobblejs_reinstall():
#     env.user = 'root'
#     env.password = root_pw
#     # reinstall js-gobble on gateway server
#     with cd('/home/arbiter/buzzz/js-gobble/'):
#         run('sudo bash reinstall.sh')
#
# @task
# @hosts(local_ip)
# def apply_all_changes(commit_msg):
#     # git pushes all the changes on your devlepment machines
#     # and applies those changes to your other servers
#     env.user = 'root'
#     env.password = root_pw
#     virtualenv = '/home/arbiter/venv/buzzz/bin/python'
#     # git add . > git commit then git pushes changes lazily
#     execute(static)
#     with settings(warn_only=True):
#         local('git add -A')
#         local('git commit -m "{}"'.format(commit_msg))
#     local('git push')
#     execute(apply_changes_to_web)
#     execute(apply_changes_to_db)
#     execute(apply_changes_to_gateway)
#     execute(apply_changes_to_gobble)
#     execute(apply_changes_to_mined)
#
#     execute(ws_reinstall)
#     execute(gobblejs_reinstall)
#
# @task
# @hosts(root_db)
# def get_db_backups():
#     env.user = 'root'
#     env.password = root_pw
#     get('/home/arbiter/backup/*', './backup/')
