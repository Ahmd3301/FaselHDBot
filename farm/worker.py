#!/usr/bin/env python3
"""worker.py — عامل التنزيل/الرفع: يأخذ مهام من Main API وينفذها ببوته الخاص.

- اكتشاف API: polling artifact باسم api-url من نفس الـ run (GITHUB_TOKEN).
- حلقة: /status -> /claim -> استخراج fresh -> تحميل N_m3u8DL-RE بالجودة المطلوبة
  -> تقارير تقدم (/progress) -> رفع tg_upload.py عبر سيرفر البوت المحلي الخاص
  -> /done -> تنظيف. stdlib فقط (+ سكربتات المشروع).
env: WORKER_NAME, WORKER_TOKEN, CHANNEL_ID, API_ID, API_HASH,
     GITHUB_REPOSITORY, GITHUB_RUN_ID, REPO_DIR
"""
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request

REPO = os.environ.get('REPO_DIR', os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.insert(0, os.path.join(REPO, 'scripts'))
from nm3u8_progress import parse_log, du_bytes  # noqa

NAME = os.environ.get('WORKER_NAME', 'w1')
WTOKEN = os.environ['WORKER_TOKEN']
CHANNEL = os.environ.get('CHANNEL_ID', '-1003864899881')
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
PART_MAX_INFO = 'split>1.9GB->PARTs'
API = {'base': ''}


def sh(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=kw.get('timeout', 600))


def api(method, payload=None, get=False):
    url = API['base'] + method
    if get:
        with urllib.request.urlopen(url, timeout=30) as r:
            return json.loads(r.read().decode())
    data = json.dumps(payload or {}).encode()
    req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())


def discover_api():
    repo = os.environ['GITHUB_REPOSITORY']
    run = os.environ['GITHUB_RUN_ID']
    for _ in range(60):  # حتى ~15 دقيقة
        try:
            r = sh(['gh', 'api', f'repos/{repo}/actions/runs/{run}/artifacts',
                    '--jq', '.artifacts[] | select(.name=="api-url") | .id'])
            aid = (r.stdout or '').strip().split('\n')[0]
            if aid:
                os.makedirs('/tmp/apiurl', exist_ok=True)
                sh(['gh', 'api', f'repos/{repo}/actions/runs/{run}/artifacts/{aid}/zip'],
                   timeout=120)
                # تنزيل عبر gh run download أبسط:
                sh(['gh', 'run', 'download', run, '-n', 'api-url', '-D', '/tmp/apiurl',
                    '-R', repo], timeout=120)
                p = '/tmp/apiurl/api-url.txt'
                if os.path.exists(p):
                    url = open(p).read().strip()
                    if url.startswith('http'):
                        return url
        except Exception:
            pass
        time.sleep(15)
    raise RuntimeError('API discovery timeout')


def extract_master(page_url):
    r = sh(['node', os.path.join(REPO, 'exFaselHD1234.js'), page_url], timeout=180)
    info = {}
    for line in (r.stdout or '').split('\n'):
        if line.startswith(('Name:', 'Poster:', 'Thumbnail:', 'Link:')):
            k, _, v = line.partition(':')
            info[k.strip().lower()] = v.strip()
    return info


def pick_quality(master_url, quality):
    want = {'1080p': '1920x1080', '720p': '1280x720', '360p': '640x360'}.get(quality)
    req = urllib.request.Request(master_url, headers={'User-Agent': UA})
    with urllib.request.urlopen(req, timeout=60) as r:
        text = r.read().decode('utf8', 'replace')
    if '#EXT-X-STREAM-INF' not in text:
        return master_url
    vs = []
    for m in re.finditer(r'#EXT-X-STREAM-INF:([^\n]+)\n([^\n]+)', text):
        a, u = m.group(1), m.group(2).strip()
        bw = int(re.search(r'BANDWIDTH=(\d+)', a).group(1))
        res = re.search(r'RESOLUTION=(\d+x\d+)', a).group(1)
        vs.append((bw, res, urllib.parse.urljoin(master_url, u)))
    vs.sort(reverse=True)
    return next((v for v in vs if v[1] == want), vs[0])[2]


def post_progress(tid, phase, pct=0, eta='--:--:--', speed='?'):
    try:
        api('/progress', {'task': tid, 'phase': phase, 'pct': pct, 'eta': eta, 'speed': speed})
    except Exception:
        pass


def run_task(t):
    tid = t['id']
    workdir = f'/tmp/farm_{tid}'
    os.makedirs(workdir, exist_ok=True)
    os.chdir(workdir)
    info = extract_master(t['url'])
    stream = pick_quality(info.get('link') or t.get('link'), t.get('quality', '1080p'))
    logf = open('nm.log', 'w')
    p = subprocess.Popen(
        ['N_m3u8DL-RE', stream, '--thread-count', '16', '-mt',
         '--tmp-dir', './tmp', '--save-dir', './dl', '--save-name', 'v',
         '-H', f'User-Agent: {UA}', '-H', 'Referer: https://www.fasel-hd.co/',
         '--no-log', '--log-level', 'INFO'],
        stdout=logf, stderr=subprocess.STDOUT)
    last_post = 0
    while p.poll() is None:
        time.sleep(5)
        pct, _, _, _ = parse_log('nm.log')
        mb = du_bytes('./tmp') / 1024 / 1024
        if time.time() - last_post > 15:
            post_progress(tid, 'dl', pct, eta='…', speed=f'{mb:.0f}MB')
            last_post = time.time()
    logf.close()
    if p.returncode != 0:
        files = [f for f in os.listdir('./dl')] if os.path.exists('./dl') else []
        if not files:
            raise RuntimeError('download failed')
    import glob
    f = sorted(glob.glob('./dl/*'), key=os.path.getsize)[-1]
    post_progress(tid, 'conv')
    # Thumbnail المستخرجة من الموقع كغلاف للفيديو
    thumb_arg = []
    if t.get('thumb'):
        try:
            req = urllib.request.Request(t['thumb'], headers={'User-Agent': UA, 'Referer': 'https://www.fasel-hd.co/'})
            with urllib.request.urlopen(req, timeout=60) as r:
                open('thumb.jpg', 'wb').write(r.read())
            thumb_arg = ['--thumb', 'thumb.jpg']
        except Exception as e:
            print(f'thumb skip: {str(e)[:100]}', flush=True)
    # الرفع عبر سكربت المشروع وبوت العامل وسيرفره المحلي
    up = subprocess.Popen(
        [sys.executable, os.path.join(REPO, 'scripts', 'tg_upload.py'),
         '--file', f, '--api-base', 'http://127.0.0.1:8081',
         '--chat-id', CHANNEL, '--caption', f"{t.get('name', '')} {t.get('quality', '')}",
         '--out', './tg'] + thumb_arg, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, env={**os.environ, 'TG_TOKEN': WTOKEN})
    last_post = 0
    last_pct = 0
    thumb_status = 'none'
    while True:
        line = up.stdout.readline()
        if not line and up.poll() is not None:
            break
        if 'THUMB=' in line:
            thumb_status = line.split('THUMB=')[1].strip()[:80]
        m = re.search(r'\[([■□]+)\]\s*(\d+)%', line)
        if m and time.time() - last_post > 15:
            last_pct = int(m.group(2))
            eta = re.search(r'ETA:\s*([0-9:]+)', line)
            sp = re.search(r'Speed:\s*([0-9]+ MB/s)', line)
            post_progress(tid, 'up', last_pct, eta.group(1) if eta else '…', sp.group(1) if sp else '?')
            last_post = time.time()
    up.wait()
    rep = json.load(open('./tg/upload-report.json'))
    if not rep.get('all_ok'):
        raise RuntimeError('upload failed')
    # message_ids من channel العامل — الرئيسي يعيد توجيهها للمستخدم
    api('/done', {'task': tid, 'msgs': [x['message_id'] for x in rep['parts']],
                  'sizes': [x['bytes'] for x in rep['parts']],
                  'parts': len(rep['parts']), 'quality': t.get('quality'),
                  'thumb': thumb_status})
    os.chdir('/tmp')


def main():
    API['base'] = discover_api()
    print(f'WORKER {NAME} api={API["base"]}', flush=True)

    def heartbeat():
        while True:
            try:
                api('/heartbeat', {'worker': NAME})
            except Exception:
                pass
            time.sleep(60)

    import threading
    threading.Thread(target=heartbeat, daemon=True).start()
    while True:
        try:
            st = api('/status', get=True)
            if st.get('shutdown'):
                print('shutdown flag — exit', flush=True)
                break
        except Exception:
            time.sleep(10)
            continue
        try:
            c = api('/claim', {'worker': NAME})
        except Exception:
            time.sleep(10)
            continue
        t = (c or {}).get('task')
        if not t:
            time.sleep(10)
            continue
        try:
            run_task(t)
        except Exception as e:
            err = str(e)[:200]
            print(f'task {t.get("id")} failed: {err}', flush=True)
            try:
                api('/fail', {'task': t.get('id'), 'error': err})
            except Exception:
                pass
            time.sleep(5)


if __name__ == '__main__':
    main()
