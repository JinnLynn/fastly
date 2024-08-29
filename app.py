import os
import re
import logging
from urllib.parse import urlparse
from datetime import datetime
import mimetypes
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import string
import random

from flask import Flask, send_file as _send_file, redirect, request, url_for
from werkzeug.exceptions import NotFound
from flask_autoindex import AutoIndex
from flask_httpauth import HTTPDigestAuth
import requests
from dateutil import parser, tz

DIST = os.getenv('FASTLY_DIST', './dist')
LIST = os.getenv('FASTLY_LIST', './list.txt')
AUTH_PWD = os.getenv('FASTLY_AUTH', ''.join(random.choices(string.ascii_letters + string.digits, k=24)))

DEF_AUTH_USR = 'fastly'
DEF_TZ = tz.gettz('Asia/Shanghai')
DEF_DOWNLOAD_TIMEOUT = (5, 30)
DEF_TEXT_TYPES = ['.m3u', '.m3u8', '.yml', '.yaml']

# ===

app = Flask(__name__)
app.config['SECRET_KEY'] = ''.join(random.choices(string.ascii_letters + string.digits, k=64))
auto_index = AutoIndex(app, browse_root=DIST, add_url_rules=False)
auth = HTTPDigestAuth()

logging.basicConfig(
        format='[%(asctime)s][%(levelname)s] %(message)s',
        handlers=[logging.StreamHandler()])
if not app.debug:
    app.logger.setLevel(logging.INFO)

for ext in DEF_TEXT_TYPES:
    mimetypes.add_type('text/plain', ext)

app.logger.info(f'AUTH: {DEF_AUTH_USR} {AUTH_PWD}')

@auth.get_password
def get_pw(usr):
    if usr == DEF_AUTH_USR:
        return AUTH_PWD
    return None

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
    abspath = os.path.join(DIST, relpath)
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

def download_single(url):
    try:
        res = requests.get(url, allow_redirects=True, timeout=DEF_DOWNLOAD_TIMEOUT)
        res.raise_for_status()
        relpath = to_path(url)
        abspath = get_abspath(relpath)
        with open(abspath, 'wb') as fp:
            fp.write(res.content)
        try:
            # modified = datetime.strptime(res.headers.get('last-modified'), '%a, %d %b %Y %H:%M:%S %Z')
            modified = parser.parse(res.headers.get('last-modified'))
            modified_local = modified.astimezone(tz=DEF_TZ)
            os.utime(abspath, (modified_local.timestamp(), modified_local.timestamp()))
            app.logger.debug(f'file modified: {modified_local}')
        except:
            pass
        app.logger.info(f'DL({res.elapsed.total_seconds():.2f}): {url} => {relpath}')
        return True
    except Exception as e:
        app.logger.error(f'DL {type(e).__name__}: {url} {e}')
    return False

def download():
    urls = set()
    with open(LIST) as fp:
        for line in fp.readlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            urls.add(line)

    success_count = 0
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(download_single, url) for url in urls]
        for future in as_completed(futures):
            if future.result():
                success_count += 1
    app.logger.info(f'done. {success_count}/{len(urls)}')

@app.route('/')
def index():
    if 'q' in request.args:
        return redirect(url_for('serve_file', target=request.args.get('q')))
    return redirect('https://lins.ren')

@app.route('/raw/list/')
@app.route('/raw/list/<path:path>')
@auth.login_required
def autoindex(path='.'):
    return auto_index.render_autoindex(path)

@app.route('/<path:target>')
def serve_file(target):
    parts = urlparse(target)
    if parts.scheme:
        # 如果是网址去除前缀 重定向
        target = re.sub(fr'^{parts.scheme}:/+', '', target)
        return redirect(url_for('serve_file', target=target))
    return send_file(target)

@app.cli.command('download')
def cmd_download():
    download()

@app.cli.command('test')
def cmd_test():
    # print(calc_hash('example.com/path/to/dir/', 'a=1&b=2'))
    for url, path in URL2PATH_TEST:
        p = to_path(url)
        try:
            assert path == p
            assert to_path(path) == path
        except AssertionError:
            app.logger.error(f'URL2PATH: {url} {path} {p}')
