import os
import sys
import re
import shutil
import tempfile
import logging
from urllib.parse import urlparse
from datetime import datetime, timedelta
import time
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
import json
from base64 import b64decode, b64encode
import zlib

from flask import Flask, Blueprint, send_file as _send_file, redirect, request, url_for, jsonify
from werkzeug.exceptions import NotFound
from flask_autoindex import AutoIndex
from flask_httpauth import HTTPDigestAuth, HTTPTokenAuth, MultiAuth
from flask_apscheduler import APScheduler
import requests
from requests.exceptions import HTTPError
from dateutil import parser, tz
import click
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, \
    EVENT_TYPE_MOVED, EVENT_TYPE_DELETED, EVENT_TYPE_CREATED, EVENT_TYPE_MODIFIED

# 隐私模式 Github Action下默认开启
privacy = os.getenv('FASTLY_PRIVACY') or os.getenv('GITHUB_ACTION')

DEF_TZ = tz.gettz('Asia/Shanghai')
DEF_TEXT_TYPES = ['.m3u', '.m3u8', '.yml', '.yaml']
DEF_MIN_DOWNLOAD_INTERVAL = 10
DEF_JOB_METHOD = 'batch'
DEF_JOB_DESC = 'fastly'
DEF_CONFIGS = {
    'dist_dir': 'dist',
    'list_dir': 'list',
    'auth_usr': 'fastly',
    'auth_pwd': ''.join(random.choices(string.ascii_letters + string.digits, k=32)),
    'auth_token': ''.join(random.choices(string.ascii_letters + string.digits, k=32)),
    'sync': {},
    'download_max_workers': 25,
    'download_timeout': 60,     # 秒
    'download_interval': -1,   # 分
    'download_at_start': False,
    'mimetypes': {
        'text/plain': ['.m3u', '.m3u8', '.yml', '.yaml', '.toml']
    },
    'verbose': False,
    'SECRET_KEY': ''.join(random.choices(string.ascii_letters + string.digits, k=64)),
}


class Config(Namespace):
    def __init__(self):
        data = self._load(os.getenv('FASTLY_CONFIG', 'config.toml'))
        super().__init__(**data)

    def _load(self, cf):
        with open(cf, 'rb') as fp:
            data = tomllib.load(fp)
        for k, v in DEF_CONFIGS.items():
            data.setdefault(k, v)
        data.update(dist_dir=os.path.abspath(data['dist_dir']),
                    list_dir=os.path.abspath(data['list_dir']))

        data.update(_internal=Namespace(job_url=None))
        download_interval = data['download_interval']
        download_interval = max(download_interval, DEF_MIN_DOWNLOAD_INTERVAL) if download_interval > 0 else download_interval
        data.update(download_interval=download_interval)

        return data

    def init_app(self, app):
        # 载入到Flask，由于Flask.config的规定只有全大写的配置才会被读入
        app.config.from_mapping(self.__dict__)

    def __getattr__(self, name):
        return self.get(name)

    def get(self, name, default=None):
        return self.__dict__.get(name, default)

class WatchHandler(FileSystemEventHandler):
    def __init__(self, app):
        super().__init__()
        self.app = app

    def on_any_event(self, event):
        active_events = [EVENT_TYPE_MOVED, EVENT_TYPE_DELETED, EVENT_TYPE_CREATED, EVENT_TYPE_MODIFIED]
        if event.is_directory or event.event_type not in active_events:
            return None

        logger.info(f'File Event[{event.event_type}]: {event.src_path}')
        # 添加到一次任务延时执行，防止多次响应
        scheduler.add_job('dowonload_when_list_change', dispatch_github_action,
                          trigger='date', run_date=delay_timestamp(3),
                          args=(self.app, ), kwargs={'event': 'WATCH'}, replace_existing=True)

def create_auth():
    _digest_auth = HTTPDigestAuth()
    _digest_auth.get_password_callback = lambda u: config.auth_pwd if u == config.auth_usr else None
    _token_auth = HTTPTokenAuth()
    _token_auth.verify_token_callback = lambda t: config.auth_usr if t == config.auth_token else None

    return MultiAuth(_digest_auth, _token_auth)

# ===
main = Blueprint('main', __name__)
logger = logging.getLogger(__name__)
config = Config()
auto_index = AutoIndex(main, browse_root=config.dist_dir, add_url_rules=False)
auth = create_auth()
scheduler = APScheduler()

def create_app():
    app = Flask(__name__)
    app.register_blueprint(main)
    app.cli.add_command(cmd_download, 'download')

    for hdl in app.logger.handlers:
        app.logger.removeHandler(hdl)
    hdl = logging.StreamHandler()
    hdl.setFormatter(logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s'))
    app.logger.addHandler(hdl)
    if app.debug or config.verbose:
        app.logger.setLevel(logging.DEBUG)
    else:
        app.logger.setLevel(logging.INFO)
        logging.getLogger('apscheduler').setLevel(logging.DEBUG)

    config.init_app(app)
    scheduler.init_app(app)

    if config.watch_list:
        start_watch_list(app)
    if config.download_interval > 0:
        logger.info(f'Auto download interval: {config.download_interval}m')
        scheduler.add_job('auto_download', dispatch_github_action,
                          trigger='interval', minutes=config.download_interval,
                          args=(app, ), replace_existing=True)
    if config.download_at_start:
        logger.info(f'Download immediately in: 5s')
        scheduler.add_job('dowonload_at_start', dispatch_github_action,
                                trigger='date', run_date=delay_timestamp(5),
                                args=(app, ), replace_existing=True)
    scheduler.start()

    for t, exts in config.mimetypes.items():
        for ext in exts:
            mimetypes.add_type(t, ext)

    return app


def dispatch_github_action(app, *args, **kwargs):
    with app.app_context():
        if not config.github_token or not config.github_repository:
            logger.error('Github token or repository missing.')
            return False
        if not config.job_url:
            logger.warning('job_url missing.')
            return
        if config._internal.job_url and config.job_url != config._internal.job_url:
            logger.warning(f'job_url mismatching: {config.job_url} {config._internal.job_url}')
        headers = {
            'Accept': 'application/vnd.github+json',
            'Authorization': f'token {config.github_token}'
        }
        event_type = 'download'
        payload = {
            'job': config.job_url,
            'token': config.auth_token
        }
        # print(payload)
        # return
        try:
            #REF: https://docs.github.com/en/rest/repos/repos?apiVersion=2022-11-28#create-a-repository-dispatch-event
            res = requests.post(f'https://api.github.com/repos/{config.github_repository}/dispatches',
                                json={'event_type': event_type, 'client_payload': payload}, headers=headers)
            res.raise_for_status()
            if res.status_code == 204:
                logger.info(f'Github Action job dispatched: {event_type} {payload}')
                return True
        except:
            logger.error(f'Dispatch Github-action job fail: {event_type} {payload}')
    return False


def delay_timestamp(seconds):
    return datetime.now() + timedelta(seconds=seconds)


def start_watch_list(app):
    def watch_process(app):
        observer = Observer()
        event_handler = WatchHandler(app)
        observer.schedule(event_handler, config.list_dir, recursive=True)
        observer.start()
        logger.info(f'Watch: {config.list_dir}')

    thread = threading.Thread(target=watch_process, args=[app], daemon=True)
    thread.start()
    return thread


# bytes => compress => b64encode => str
def encode(data):
    return b64encode(zlib.compress(data)).decode()


# [str =>] bytes => b64decode => decompress => bytes
def decode(data):
    if isinstance(data, str):
        data = data.encode()
    return zlib.decompress(b64decode(data))


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
        logger.debug(f'{url_or_path} => {relpath}')
        return _send_file(abspath)
    logger.warning(f'NotFound: {url_or_path} => {relpath}')
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


# save: 是否保存为文件 True则保存文件并返回文件路径 否则返回bytes
def download_single(data, save=True):
    data = {'url': data} if not isinstance(data, dict) else data
    url = data.get('url')
    ua = data.get('ua')
    headers = data.get('headers', {})
    try:
        start = time.perf_counter()
        if ua:
            headers['User-Agent'] = ua
        # requests 的timeout跟通常理解的有差异，指的是连续没有数据下载的时长
        # 所有有可能在下载速度很慢的情况下，会超出timeout的时间
        # https://docs.python-requests.org/en/latest/user/quickstart/#timeouts
        # 使用stream=True 自行判断超时时间
        res = requests.get(url, timeout=config.download_timeout, stream=True, headers=headers)
        res.raise_for_status()

        # 使用临时文件 防止下载不完整或覆盖旧文件
        tmp = tempfile.mktemp()
        raw = b''
        for chunk in res.iter_content(1024):
            if time.perf_counter() - start > config.download_timeout:
                raise requests.exceptions.Timeout()
            raw += chunk

        relpath = to_path(url)
        abspath = get_abspath(relpath)
        if save:
            with open(abspath, 'wb') as fp:
                fp.write(raw)

            try:
                modified = parser.parse(res.headers.get('last-modified'))
                modified_local = modified.astimezone(tz=DEF_TZ)
                os.utime(abspath, (modified_local.timestamp(), modified_local.timestamp()))
            except:
                pass

        elapsed = time.perf_counter() - start
        if not privacy:
            logger.info(f'DL({elapsed:.2f}): {url} => {relpath if save else "<RAW>"}')
        return True, url, abspath if save else (raw, res.headers), data
    except Exception as e:
        if privacy:
            logger.error(f'DL {type(e).__name__}: {e}')
        else:
            logger.error(f'DL {type(e).__name__}: {url} {e}')
    return False, url, None, None


def batch_download(urls, save=True):
    with ThreadPoolExecutor(max_workers=config.download_max_workers) as executor:
        futures = [executor.submit(download_single, url, save=save) for url in urls]
        for future in as_completed(futures):
            ret, url, dest, org = future.result()
            yield ret, url, dest, org


def download(urls):
    start = datetime.now()
    success = []
    fail = []
    for ret, url, dest, _ in batch_download(urls):
        (success if ret else fail).append(url)
    dt = datetime.now() - start
    logger.info(f'done. {dt.total_seconds():.3f}s {len(success)}/{len(urls)}')
    return success, fail


def download_local():
    logger.info('Download local list...')
    urls = get_urls()
    download(urls)


def download_remote(remote, token):
    try:
        headers = {}
        if token:
            headers['Authorization'] = f'Bearer {token}'
        res = requests.get(remote, headers=headers)
        res.raise_for_status()
        data = res.json()
        desc = data.get('desc')
        desc = f':{desc}' if desc else ''
        if data.get('method') == 'one':
            logger.info(f'Download remote[one_by_one{desc}]...')
            download_remote_one_by_one(token, data)
        else:
            logger.info(f'Download remote[batch{desc}]...')
            download_remote_batch(token, data)
    except Exception as e:
        if isinstance(e, HTTPError):
            logger.error(f'Download remote fail: {e.response.status_code}')
        else:
            logger.error(f'Download remote fail: {e.__class__.__name__}')
        return


def download_remote_batch(token, data):
    success, fail = download(data.get('urls', []))
    sync_ret = rsync_to_server(**data.get('sync'))
    callback(data.get('callback'), token, {'result': [sync_ret, success, fail], 'method': 'batch'})


def download_remote_one_by_one(token, data):
    urls = data.get('urls', [])
    start = time.perf_counter()
    success_count = 0
    for ret, url, ret_data, req in batch_download(urls, save=False):
        if not ret:
            continue
        success_count += 1
        raw, headers = ret_data
        callback(data.get('callback'), token, {'raw': encode(raw),
                                               'headers': dict(headers),
                                               'request': req,
                                               'method': 'one'})

    logger.info(f'done. {time.perf_counter() - start:.3f}s {success_count}/{len(urls)}')


def callback(url, token, data):
    try:
        headers = {}
        if token:
            headers['Authorization'] = f'Bearer {token}'
        res = requests.post(url, headers=headers, json=data)
        res.raise_for_status()
        data = res.json()
        if data.get('code') != 0:
            raise ValueError()
        logger.info(f'callback done: {data.get("code", None)}')
        return True
    except Exception as e:
        if isinstance(e, HTTPError):
            logger.error(f'Callback fail: {e.response.status_code}')
        else:
            logger.error(f'Callback fail: {e}')
    return False


def rsync_to_server(**kwargs):
    host = kwargs.get('host') or os.getenv('DST_HOST') or config.sync.get('host')
    port = kwargs.get('port') or os.getenv('DST_PORT') or config.sync.get('port', 22)
    usr = kwargs.get('usr') or os.getenv('DST_USER') or config.sync.get('user', 'root')
    key = kwargs.get('key') or os.getenv('DST_KEY') or config.sync.get('key')
    dest = kwargs.get('path') or  os.getenv('DST_PATH') or config.sync.get('path')

    if not host or not port or not usr or not dest:
        logger.error(f'Sync fail: incomplete server information')
        return False

    logger.info('sync to server...')
    cmd = f'ssh-keyscan -p {port} {host}'
    try:
        with open(os.path.expanduser('~/.ssh/known_hosts'), 'ab') as fp:
            output = subprocess.check_output(shlex.split(cmd), stderr=subprocess.DEVNULL)
            fp.write(output)
    except Exception as e:
        logger.error(f'ssh-keyscan fail: {e}')

    rsh = f'ssh -p {port}'
    if key:
        rsh += f' -i {key}'
    cmd = f'rsync -rlth --checksum --ignore-errors --stats -e "{rsh}" "{config.dist_dir}/" "{usr}@{host}:{dest}"'
    try:
        proc = subprocess.Popen(shlex.split(cmd))
        proc.wait()
        logger.info('sync done...')
        return proc.returncode == 0
    except Exception as e:
        logger.error(f'sync fail: {e}')
    return False


def update_metadata(success, fail):
    success = success or []
    fail = fail or []
    metadata_file = os.path.join(config.dist_dir, 'metadata.json')
    try:
        with open(metadata_file) as fp:
            pre_data = json.load(fp)
    except:
        pre_data = {}

    def _get_datetime():
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def _gen_job_data(url, downloaded):
        relpath = to_path(url)
        exists = os.path.isfile(get_abspath(relpath))
        last_update = _get_datetime() if downloaded else None
        if not downloaded and exists:
            for j in pre_data.get('job', []):
                if j.get('remote') == url and j.get('last_update'):
                    last_update = j.get('last_update')
                    break
        return {'remote': url, 'local': relpath,
                'exists': exists,
                'last_update': last_update}

    data = {'job': []}
    for url in success:
        data['job'].append(_gen_job_data(url, True))
    for url in fail:
        data['job'].append(_gen_job_data(url, False))

    data['last_update'] = _get_datetime()

    with open(metadata_file, 'w') as fp:
        json.dump(data, fp, indent=2)

    return data


@main.before_request
def before_request():
    if not config.job_url:
        config.job_url = url_for('main.view_job', _external=True)
    if not config._internal.job_url:
        config._internal.job_url = url_for('main.view_job', _external=True)


@main.route('/')
def index():
    if 'q' in request.args:
        return redirect(url_for('main.view_serve_file', target=request.args.get('q')))
    return ' ', 404


@main.route('/<path:target>')
def view_serve_file(target):
    parts = urlparse(target)
    if parts.scheme:
        # 如果是网址去除前缀 重定向
        target = re.sub(fr'^{parts.scheme}:/+', '', target)
        return redirect(url_for('main.view_serve_file', target=target))
    return send_file(target)


@main.route('/-/file/')
@main.route('/-/file/<path:path>')
@auth.login_required
def autoindex(path='.'):
    return auto_index.render_autoindex(path)


@main.route('/-/raw/')
@auth.login_required
def dump_urls():
    return '\n'.join(get_urls())


@main.route('/-/job/')
@auth.login_required
def view_job():
    sync_data = config.sync.copy()
    sync_data.pop('key', None)
    if not sync_data.get('path'):
        sync_data['path'] = config.dist_dir
    return jsonify(code=0, urls=get_urls(),
                   method=DEF_JOB_METHOD,
                   desc=DEF_JOB_DESC,
                   callback=url_for('main.view_callback', _external=True),
                   sync=sync_data)


def callback_save_batch(data):
    result, success, fail = data.get('result')
    if not result:
        logger.warning('CB: remote download sync fail')
        return False
    update_metadata(success, fail)
    return True


def callback_save_one(data):
    print(data)
    req = data.get('request', {})
    url = req.get('url')
    if not url:
        return False
    try:
        raw = decode(data.get('raw'))
        relpath = to_path(url)
        abspath = get_abspath(relpath)
        with open(abspath, 'wb') as fp:
            fp.write(raw)
    except Exception as e:
        logger.error(f'callback fail one: {e}')
        return False
    return True

# 回报
# 数据: [[],[]]
@main.route('/-/callback/', methods=['POST'])
@auth.login_required
def view_callback():
    try:
        data = request.json
    except Exception as e:
        logger.warning(f'CB data fail: {request.remote_addr} {e}')
        return jsonify(code=1)

    ret = callback_save_one(data) if data.get('method') == 'one' else callback_save_batch(data)
    return jsonify(code=0 if ret else 1)


@click.command()
@click.option('--remote', default=None)
@click.option('--token', default=None)
def cmd_download(remote, token):
    if remote:
        return download_remote(remote, token)
    return download_local()


# @cli.command()
# @click.option('--force', is_flag=True)
# @click.option('--host')
# @click.option('--port')
# @click.option('--usr')
# @click.option('--key')
# @click.option('--path')
# def cmd_sync(**kwargs):
#     if not rsync_to_server(**kwargs):
#         raise SystemExit(code=1)


# TODO
# @click.command('clean')
# def cmd_clean():
#     pass
