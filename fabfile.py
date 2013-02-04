import os, sys, time, errno
from fabric.api import env, run, local, cd, sudo, get, put

try: 
    from local_settings import bak_dir
except Exception, e:
    bak_dir = '{{ Replace with server path to db backup filesystem }}'

# fabric settings
env.use_ssh_config = True

# deploy dir
dev = '{{ Replace with server path to dev filesystem }}'
prod = '{{ Replace with server path to prod filesystem }}'

# server list
env.hosts = ['voyager']

def deploy():
    repo = local("git symbolic-ref HEAD", capture=True).split(u'/')[-1]
    path = prod if repo == 'master' else dev
    with cd(path):
        run('git pull origin {0}'.format(repo))

def push(m='0'):
    repo = local("git symbolic-ref HEAD", capture=True).split(u'/')[-1]
    local('git commit -a -m"{0}"'.format(m))
    local('git push origin {0}'.format(repo))
    deploy()

def bak():
    t = time.time()
    with cd(bak_dir):
        run('mongodump --host 127.0.0.1:27017 --db dev -o {0}-dev'.format(t))
        run('mongodump --host 127.0.0.1:27017 --db main -o {0}-main'.format(t))
        cleanbaks()
        run('rsync -r -a -v -e "ssh -l {{ Replace with server user }}" --delete . {{ Replace with FQDN and filepath of remote server in format: example.com:/path/to/redundant/baks }}')
    return t

def cleanbaks():
    with cd(bak_dir):
        baks = run('ls -l', combine_stderr=False)
        baks = baks.split('\n')
        baks = [b.split(' ')[-1].strip('\r') for i, b in enumerate(baks) if b and i > 0]
        num_baks = len(baks)
        if num_baks > 40:
            baks.sort(key=lambda b: b.split('-')[0])
            num_to_rm = num_baks - 40
            rm_list = baks[:num_to_rm]
            run('rm -rf {0}'.format(' '.join(rm_list)))

def lsbak():
    with cd(bak_dir):
        baks = run('ls -l', combine_stderr=False)
        baks = baks.split('\n')
        baks = [b.split(' ')[-1].strip('\r') for i, b in enumerate(baks) if b and i > 0]
        grouped = []
        dbnames = []
        for b in baks:
            t, dbname = b.split('-')
            if dbname not in dbnames:
                dbnames.append(dbname)
            tobj = time.localtime(float(t))
            if (tobj, t) not in grouped:
                grouped.append((tobj, t))
        sys.stdout.write('There are {0} backups\n'.format(len(grouped)))
        sys.stdout.write('---------------------\n')
        for tobj, t in grouped:
            sys.stdout.write('{0} --> {1}'.format(time.strftime('%a, %b %d, %Y @ %I:%M:%S %p', tobj), ' '.join(['{0}-{1}'.format(t, db) for db in dbnames])))
            sys.stdout.write('\n')

def dev2local():
    t = bak()
    dump_dir = 'dump'
    try:
        os.makedirs(os.path.join(os.getcwd(), dump_dir))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise e
    with cd(bak_dir):
        get('{0}-{1}/{1}'.format(t, 'dev'), dump_dir)
    db_from_dev = os.path.join(dump_dir, 'dev')
    local('mongorestore -h 127.0.0.1:3002 --db meteor --drop {0}'.format(db_from_dev))
    local('rm -rf {0}'.format(db_from_dev))

def prod2local():
    t = bak()
    dump_dir = 'dump'
    try:
        os.makedirs(os.path.join(os.getcwd(), dump_dir))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise e
    with cd(bak_dir):
        get('{0}-{1}/{1}'.format(t, 'main'), dump_dir)
    db_from_prod = os.path.join(dump_dir, 'main')
    local('mongorestore -h 127.0.0.1:3002 --db meteor --drop {0}'.format(db_from_prod))
    local('rm -rf {0}'.format(db_from_prod))

def dev2prod():
    t = bak()
    with cd(bak_dir):
        run('mongorestore --host 127.0.0.1:27017 --db main --drop {0}-dev/dev'.format(t))

def prod2dev():
    t = bak()
    with cd(bak_dir):
        run('mongorestore --host 127.0.0.1:27017 --db dev --drop {0}-main/main'.format(t))
