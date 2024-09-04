import os
import sys
import re
import logging
from urllib.parse import urlparse
from datetime import datetime, timedelta
import mimetypes
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import string
import random
import subprocess
import shlex
import tomllib
from argparse import Namespace
from glob import glob
import threading

from flask import Flask, send_file as _send_file, redirect, request, url_for
from werkzeug.exceptions import NotFound
from flask_autoindex import AutoIndex
from flask_httpauth import HTTPDigestAuth, HTTPTokenAuth, MultiAuth
from flask_apscheduler import APScheduler
import requests
from requests.exceptions import HTTPError
from requests import status_codes
from dateutil import parser, tz
import click
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, \
    EVENT_TYPE_MOVED, EVENT_TYPE_DELETED, EVENT_TYPE_CREATED, EVENT_TYPE_MODIFIED

privacy = os.getenv('FASTLY_PRIVACY')

DEF_TZ = tz.gettz('Asia/Shanghai')
DEF_TEXT_TYPES = ['.m3u', '.m3u8', '.yml', '.yaml']
DEF_CONFIGS = {
    'dist_dir': 'dist',
    'list_dir': 'list',
    'auth_usr': 'fastly',
    'auth_pwd': ''.join(random.choices(string.ascii_letters + string.digits, k=32)),
    'auth_token': ''.join(random.choices(string.ascii_letters + string.digits, k=32)),
    'sync': {},
    'download_max_workers': 25,
    'download_timeout': (5, 30),
    'SECRET_KEY': ''.join(random.choices(string.ascii_letters + string.digits, k=64)),
}

class Config(Namespace):
    def __init__(self, app):
        data = self._load(app, os.getenv('FASTLY_CONFIG', 'config.toml'))
        super().__init__(**data)

    def _load(self, app: Flask, cf):
        app.logger.debug(f'Config: {cf}')
        with open(cf, 'rb') as fp:
            data = tomllib.load(fp)
        for k, v in DEF_CONFIGS.items():
            data.setdefault(k, v)
        data.update(dist_dir=os.path.join(app.root_path, data['dist_dir']),
                    list_dir=os.path.join(app.root_path, data['list_dir']))
        app.config.from_mapping(data)
        return data

    def __getattr__(self, name):
        return self.get(name)

    def get(self, name, default=None):
        return self.__dict__.get(name, default)

def dispatch_github_action(app, config, *args, **kwargs):
    app.logger.info('github action')
    return False
    if not config.github_token or not config.github_repository:
        app.logger.error('Github token or repository missing.')
        return False
    headers = {
        'Accept': 'application/vnd.github.everest-preview+json',
        'Authorization': f'token {config.github_token}'
    }
    try:
        res = requests.post(f'https://api.github.com/repos/{config.github_repository}/dispatches',
                            json={'event_type': 'download'}, headers=headers)
        res.raise_for_status()
        if res.status_code == 204:
            app.logger.info('Github Action job dispatched.')
            return True
    except:
        app.logger.error('Dispatch Github-action job fail.')
    return False

class WatchHandler(FileSystemEventHandler):
    def __init__(self, app, config):
        super().__init__()
        self.app = app
        self.config = config

    def on_any_event(self, event):
        active_events = [EVENT_TYPE_MOVED, EVENT_TYPE_DELETED, EVENT_TYPE_CREATED, EVENT_TYPE_MODIFIED]
        if event.is_directory or event.event_type not in active_events:
            return None

        app.logger.info(f'File Event[{event.event_type}]: {event.src_path}')
        # 添加到一次任务延时执行，防止多次响应
        app.apscheduler.add_job('dowonload_when_list_change', dispatch_github_action,
                                     trigger='date', run_date=datetime.now() + timedelta(seconds=3),
                                     args=(self.app, self.config), kwargs={'event': 'WATCH'},
                                     replace_existing=True)

def start_watch_list(app, config):
    def watch_process(app, config):
        observer = Observer()
        event_handler = WatchHandler(app, config)
        observer.schedule(event_handler, config.list_dir, recursive=True)
        observer.start()
        app.logger.info(f'Watch: {config.list_dir}')

    thread = threading.Thread(target=watch_process, args=[app, config], daemon=True)
    thread.start()
    return thread

# ===
app = Flask(__name__)
config = Config(app)
auto_index = AutoIndex(app, browse_root=config.dist_dir, add_url_rules=False)

# AUTH
_digest_auth = HTTPDigestAuth()
_digest_auth.get_password_callback = lambda u: config.auth_pwd if u == config.auth_usr else None
_token_auth = HTTPTokenAuth()
_token_auth.verify_token_callback = lambda t: config.auth_usr if t == config.auth_token else None
auth = MultiAuth(_digest_auth, _token_auth)

scheduler = APScheduler(app=app)
if config.watch_list:
    start_watch_list(app, config)
scheduler.start()

logging.basicConfig(
        format='[%(asctime)s][%(levelname)s] %(message)s',
        handlers=[logging.StreamHandler()])
if not app.debug:
    app.logger.setLevel(logging.INFO)

for ext in DEF_TEXT_TYPES:
    mimetypes.add_type('text/plain', ext)

# app.logger.info(f'AUTH: USR: {config.auth_usr} PWD: {config.auth_pwd} TOKEN: {config.auth_token}')

def _re_subs(s, *reps):
    d = (None, '', 0, re.IGNORECASE)
    for rep in reps:
        rep = list(rep) + [d[i] for i in range(len(rep), len(d))]
        rep.insert(2, s)
        # print(rep)
        s = re.sub(*rep)
    return s

def calc_hash(*args):
    m = hashlib.sha256()
    for s in args:
        m.update(s.encode())
    return m.hexdigest()[0:12]

def is_url(s):
    parts = urlparse(s)
    return parts.scheme and parts.netloc

def clear_scheme(url):
    p = urlparse(url)
    return re.sub(fr'^{p.scheme}:/+', '', url)

# FIX: 地址中有query时还有问题
URL2PATH_TEST = [
    ('http://example.com/path/to/file.ext',                 'example.com/path/to/file.ext'),
    ('http://example.com:port/path/to/file.ext',            'example.com/port/path/to/file.ext'),
    ('http://example.com:port/path/to/file space name.ext', 'example.com/port/path/to/file-space-name.ext'),
    ('http://example.com/',                                 'example.com/73d986e00906'),
    ('http://example.com/path/to/dir/',                     'example.com/path/to/dir/25d5ecf6aa8d'),
    ('http://example.com/path/to/dir/?a=1&b=2',             'example.com/path/to/dir/8702068bde16'),
]
def to_path(s):
    parts = urlparse(s)
    netloc = '/'.join(parts.netloc.split(':'))                     # 网址端口转为子目录
    path = _re_subs(parts.path,
                        (r'^/+', ),                         # 去除开头的/
                        (r'[ :]+', '-')                     # 空格 : => -
                    )
    relpath = os.path.join(netloc, path)
    if parts.query:                                         # 地址包含查询
        name = calc_hash(relpath, parts.query)
        relpath = os.path.join(relpath, name)
    elif relpath.endswith('/'):                             # 地址以目录方式结束
        relpath = os.path.join(relpath, calc_hash(relpath))
    return relpath

def get_abspath(relpath, mkdir=True):
    abspath = os.path.join(config.dist_dir, relpath)
    if mkdir and not os.path.isdir(os.path.dirname(abspath)):
        os.makedirs(os.path.dirname(abspath))
    return abspath

def send_file(url_or_path):
    relpath = to_path(url_or_path)
    abspath = get_abspath(relpath, False)
    if os.path.isfile(abspath):
        app.logger.debug(f'{url_or_path} => {relpath}')
        return _send_file(abspath)
    app.logger.warning(f'NotFound: {url_or_path} => {relpath}')
    raise NotFound()

def read_urls(content):
    valids = []
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        valids.append(line)
    return valids

def get_urls():
    urls = set()
    for f in glob('*.txt', root_dir=config.list_dir):
        try:
            with open(os.path.join(config.list_dir, f)) as fp:
                fp.read
                urls.update(read_urls(fp.read()))
        except Exception as e:
            logging.error(f'read list fail: {f} {e}')
    return list(urls)

def download_single(url):
    try:
        res = requests.get(url, allow_redirects=True, timeout=config.download_timeout)
        res.raise_for_status()
        relpath = to_path(url)
        abspath = get_abspath(relpath)
        with open(abspath, 'wb') as fp:
            fp.write(res.content)
        try:
            modified = parser.parse(res.headers.get('last-modified'))
            modified_local = modified.astimezone(tz=DEF_TZ)
            os.utime(abspath, (modified_local.timestamp(), modified_local.timestamp()))
        except:
            pass
        if not privacy:
            app.logger.info(f'DL({res.elapsed.total_seconds():.2f}): {url} => {relpath}')
        return True
    except Exception as e:
        if privacy:
            app.logger.error(f'DL {type(e).__name__}: {e}')
        else:
            app.logger.error(f'DL {type(e).__name__}: {url} {e}')
    return False

def download(urls):
    success_count = 0
    start = datetime.now()
    with ThreadPoolExecutor(max_workers=config.download_max_workers) as executor:
        futures = [executor.submit(download_single, url) for url in urls]
        for future in as_completed(futures):
            if future.result():
                success_count += 1
    dt = datetime.now() - start
    app.logger.info(f'done. {dt.total_seconds():.3f}s {success_count}/{len(urls)}')

def download_local():
    app.logger.info('Download local list...')
    urls = get_urls()
    download(urls)

def download_remote(remote, token):
    app.logger.info(f'Download remote list...')
    try:
        headers = {}
        if token:
            headers['Authorization'] = f'Bearer {token}'
        res = requests.get(remote, headers=headers)
        res.raise_for_status()
        urls = read_urls(res.content.decode())
        download(urls)
    except Exception as e:
        if isinstance(e, HTTPError):
            app.logger.error(f'Download remote fail: {e.response.status_code}')
        else:
            app.logger.error(f'Download remote fail: {e.__class__.__name__}')

@app.route('/')
def index():
    if 'q' in request.args:
        return redirect(url_for('serve_file', target=request.args.get('q')))
    return redirect('https://lins.ren')

@app.route('/<path:target>')
def serve_file(target):
    parts = urlparse(target)
    if parts.scheme:
        # 如果是网址去除前缀 重定向
        target = re.sub(fr'^{parts.scheme}:/+', '', target)
        return redirect(url_for('serve_file', target=target))
    return send_file(target)

@app.route('/-/list/')
@app.route('/-/list/<path:path>')
@auth.login_required
def autoindex(path='.'):
    return auto_index.render_autoindex(path)

@app.route('/-/urls/')
@auth.login_required
def dump_urls():
    return '\n'.join(get_urls())

@app.cli.command('download')
@click.option('--remote', default=None)
@click.option('--token', default=None)
def cmd_download(**kwargs):
    remote = kwargs.get('remote')
    if remote:
        return download_remote(remote, kwargs.get('token'))
    return download_local()

@app.cli.command('sync')
@click.option('--force', is_flag=True)
@click.option('--host')
@click.option('--port')
@click.option('--usr')
@click.option('--key')
@click.option('--path')
def cmd_sync(**kwargs):
    host = kwargs.get('host') or os.getenv('DST_HOST') or config.sync.get('host')
    port = kwargs.get('port') or os.getenv('DST_PORT') or config.sync.get('port', 22)
    usr = kwargs.get('usr') or os.getenv('DST_USER') or config.sync.get('user', 'root')
    key = kwargs.get('key') or os.getenv('DST_KEY') or config.sync.get('key')
    dest = kwargs.get('path') or  os.getenv('DST_PATH') or config.sync.get('path')

    extra_args = '--delete --delete-excluded' if kwargs.get('force') else ''
    rsh = f'ssh -p {port}'
    if key:
        rsh += f' -i {key}'
    cmd = f'rsync -rlth --checksum --ignore-errors --stats {extra_args} -e "{rsh}" "{config.dist_dir}/" "{usr}@{host}:{dest}"'
    try:
        app.logger.info('sync to server...')
        proc = subprocess.Popen(shlex.split(cmd))
        proc.wait()
    except Exception as e:
        logging.error(f'sync fail: {e}')
        raise SystemExit(code=1)

@app.cli.command('action')
def cmd_github_action():
    dispatch_github_action()


@app.cli.command('test')
@click.option('--caps', is_flag=True, help='uppercase the output')
def cmd_test(caps):
    print(caps)
    return
    # with open('config.ini', 'rb') as fp:
    #     print(load_ini_file(fp))

    # print(calc_hash('example.com/path/to/dir/', 'a=1&b=2'))
    for url, path in URL2PATH_TEST:
        p = to_path(url)
        try:
            assert path == p
            assert to_path(path) == path
        except AssertionError:
            app.logger.error(f'URL2PATH: {url} {path} {p}')
