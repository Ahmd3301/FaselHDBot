#!/usr/bin/env python3
"""main_bot.py — البوت الرئيسي: واجهة المستخدم + منسق العمال + API + كاش Redis.

- يعمل ضد https://api.telegram.org مباشرة (long polling) — لا يحتاج سيرفر محلياً.
- سيرفر API (منفذ 8000) للعمال عبر Cloudflare Quick Tunnel.
- كاش Redis: done:<pageid>:<quality> -> file_ids (إرسال فوري ⚡ بدون bandwidth).
- قبل النهاية: BGSAVE + استبدال ميديا الرسالة 12 (dump.rdb) + الرابط في الرسالة 13.
- stdlib فقط (urllib) + redis_mini + node للمستخرج.

env:
  MAIN_TOKEN, CHANNEL_ID, PORT=8000, API_PUBLIC_URL (Tunnel, يُكتب في رسالة 13),
  MSG_LINK_ID=13, MSG_DUMP_ID=12, MAX_RUNTIME_S=17400, MAX_ACTIVE=4
"""
import json
import os
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from redis_mini import RedisMini  # noqa

TOKEN = os.environ['MAIN_TOKEN']
CHANNEL = os.environ.get('CHANNEL_ID', '-1003864899881')
PORT = int(os.environ.get('PORT', '8000'))
API_URL = os.environ.get('API_PUBLIC_URL', '')
MSG_LINK = int(os.environ.get('MSG_LINK_ID', '13'))
MSG_DUMP = int(os.environ.get('MSG_DUMP_ID', '12'))
MAX_RUNTIME = int(os.environ.get('MAX_RUNTIME_S', '17400'))
MAX_ACTIVE = 4
START_T = time.time()
API = f'https://api.telegram.org/bot{TOKEN}'
shutdown = {'flag': False}

STR = {
    'start': {'ar': 'أرسل رابط fasel-hd (مثال https://www.fasel-hd.co/?p=228502)',
              'en': 'Send a fasel-hd link (e.g. https://www.fasel-hd.co/?p=228502)'},
    'busy': {'ar': 'مشغول: 4 عمليات جارية. انتظر انتهاء إحداها.',
             'en': 'Busy: 4 jobs running. Wait for one to finish.'},
    'back': {'ar': 'رجوع', 'en': 'Back'},
    'st_analysis': {'ar': '01\nتحليل...', 'en': '01\nAnalysis...'},
    'st_convert': {'ar': '03\nتحويل...', 'en': '03\nConverting...'},
}
QUALITIES = [('1080p',), ('720p',), ('360p',)]


def tg(method, params=None, timeout=120):
    data = urllib.parse.urlencode(params or {}).encode()
    req = urllib.request.Request(f'{API}/{method}', data=data,
                                 headers={'Content-Type': 'application/x-www-form-urlencoded'})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def tg_file(method, params, file_path, field='document', timeout=3600):
    boundary = '----mainbot' + os.urandom(8).hex()
    body = b''
    for k, v in params.items():
        body += f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'.encode()
    fn = os.path.basename(file_path)
    body += (f'--{boundary}\r\nContent-Disposition: form-data; name="{field}"; filename="{fn}"\r\n'
             f'Content-Type: application/octet-stream\r\n\r\n').encode()
    with open(file_path, 'rb') as f:
        payload = body + f.read() + f'\r\n--{boundary}--\r\n'.encode()
    req = urllib.request.Request(f'{API}/{method}', data=payload,
                                 headers={'Content-Type': f'multipart/form-data; boundary={boundary}'})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def event(r, text):
    """سجل أحداث حي (آخر 50) — يظهر في /diag."""
    try:
        r.rpush('events', time.strftime('%H:%M:%S', time.gmtime()) + ' ' + text[:160])
        # قص بسيط عند التجاوز
        q = r.cmd('LRANGE', 'events', '0', '-1') or []
        if len(q) > 50:
            r.delete('events')
            for x in q[-50:]:
                r.rpush('events', x)
    except Exception:
        pass


def bar(pct):
    f = min(10, max(0, int(pct // 10)))
    return '■' * f + '□' * (10 - f)


def lang_of(r, uid):
    return r.get(f'lang:{uid}') or 'ar'


def pageid_of(url):
    import re
    m = re.search(r'[?&]p=(\d+)', url)
    return m.group(1) if m else 'x'


def extract_info(page_url, attempts=3, wait_s=12):
    """يشغّل المستخرج ويعيد dict {name, poster, thumbnail, link} (سريع تفاعلياً)."""
    import time as _t
    repo = os.environ.get('REPO_DIR', os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    last = {}
    for _ in range(attempts):
        try:
            p = subprocess.run(['node', os.path.join(repo, 'exFaselHD1234.js'), page_url],
                               capture_output=True, text=True, timeout=120)
            info = {}
            for line in (p.stdout or '').split('\n'):
                if line.startswith(('Name:', 'Poster:', 'Thumbnail:', 'Link:', 'Episode:')):
                    k, _, v = line.partition(':')
                    info[k.strip().lower()] = v.strip()
            if info.get('link', '').startswith('http'):
                return info
            last = info
        except Exception:
            pass
        _t.sleep(wait_s)
    return last


def quality_keyboard(task_id, pageid, r, lang):
    rows = []
    for (q,) in QUALITIES:
        done = r.get(f'done:{pageid}:{q}')
        label = f'• {q}' + (' ⚡' if done else '')
        rows.append([{'text': label, 'callback_data': f'q:{task_id}:{q}'}])
    kb = []
    for i in range(0, len(rows), 2):
        kb.append(rows[i] + (rows[i + 1] if i + 1 < len(rows) else []))
    kb.append([{'text': STR['back'][lang], 'callback_data': f'b:{task_id}'}])
    return {'inline_keyboard': kb}


# ---------------- API للعمال ----------------

class Handler(BaseHTTPRequestHandler):
    rdb = None

    def _json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == '/health':
            return self._json({'ok': True})
        if self.path == '/status':
            return self._json({'shutdown': shutdown['flag']})
        if self.path == '/diag':
            r = self.rdb
            try:
                qn = r.llen('queue')
                aids = r.smembers('active')
                acts = []
                for tid in aids:
                    raw = r.get(f'task:{tid}')
                    if raw:
                        t = json.loads(raw)
                        acts.append({'id': tid, 'status': t.get('status'),
                                     'worker': t.get('worker'), 'progress': t.get('progress')})
                hbs = {w: r.get(f'hb:{w}') for w in ('w1', 'w2', 'w3', 'w4')}
                evts = r.cmd('LRANGE', 'events', '0', '-1') or []
                return self._json({'queue': qn, 'active': acts, 'heartbeats': hbs,
                                   'events': evts[-15:],
                                   'uptime_s': int(time.time() - START_T)})
            except Exception as e:
                return self._json({'ok': False, 'error': str(e)[:200]}, 500)
        return self._json({'ok': False}, 404)

    def do_POST(self):
        try:
            n = int(self.headers.get('Content-Length', 0))
            req = json.loads(self.rfile.read(n).decode() or '{}')
        except Exception:
            return self._json({'ok': False, 'error': 'bad json'}, 400)
        r = self.rdb
        try:
            if self.path == '/claim':
                raw = r.rpop('queue')
                if not raw:
                    return self._json({'task': None})
                t = json.loads(raw)
                t['status'] = 'claimed'
                t['worker'] = req.get('worker', '?')
                r.set(f"task:{t['id']}", json.dumps(t))
                r.sadd('active', t['id'])
                r.sadd('claimed_ids', t['id'])
                event(r, f"claim {t['id']} {t.get('quality')} by {t['worker']}")
                return self._json({'task': t})
            if self.path == '/progress':
                tid = req.get('task')
                raw = r.get(f'task:{tid}')
                if not raw:
                    return self._json({'ok': False}, 404)
                t = json.loads(raw)
                t['progress'] = {k: req.get(k) for k in ('phase', 'pct', 'eta', 'speed')}
                r.set(f'task:{tid}', json.dumps(t))
                edit_progress(t)
                return self._json({'ok': True})
            if self.path == '/heartbeat':
                r.set(f"hb:{req.get('worker', '?')}", time.strftime('%H:%M:%S', time.gmtime()))
                return self._json({'ok': True})
            if self.path == '/fail':
                tid = req.get('task')
                raw = r.get(f'task:{tid}')
                if not raw:
                    return self._json({'ok': False}, 404)
                t = json.loads(raw)
                t['status'] = 'queued'
                t.pop('worker', None)
                r.set(f"task:{tid}", json.dumps(t))
                r.srem('active', tid)
                r.srem('claimed_ids', tid)
                r.rpush('queue', json.dumps(t))
                try:
                    tg('editMessageText', {'chat_id': t['user'], 'message_id': t['msg'],
                                           'text': f"{t.get('name', '')}\n⚠️ worker failed ({str(req.get('error', ''))[:80]}) — requeued"})
                except Exception:
                    pass
                return self._json({'ok': True})
            if self.path == '/done':
                tid = req.get('task')
                raw = r.get(f'task:{tid}')
                if not raw:
                    return self._json({'ok': False}, 404)
                t = json.loads(raw)
                t['status'] = 'done'
                t['result'] = {k: req.get(k) for k in ('msgs', 'sizes', 'parts', 'quality', 'thumb')}
                r.set(f"task:{tid}", json.dumps(t))
                r.srem('active', tid)
                r.srem('claimed_ids', tid)
                event(r, f"done {tid} msgs={req.get('msgs')} thumb={req.get('thumb')}")
                finalize_task(t)
                return self._json({'ok': True})
        except Exception as e:
            return self._json({'ok': False, 'error': str(e)[:200]}, 500)
        return self._json({'ok': False}, 404)

    def log_message(self, *a):
        pass


def edit_progress(t):
    lang = t.get('lang', 'ar')
    pr = t.get('progress', {}) or {}
    phase, pct = pr.get('phase', 'dl'), float(pr.get('pct') or 0)
    eta, speed = pr.get('eta', '--:--:--'), pr.get('speed', '?')
    if phase == 'dl':
        txt = f"02\nDownloading...\n[{bar(pct)}] {pct:.0f}% ETA {eta}"
    elif phase == 'conv':
        txt = STR['st_convert'][lang]
    else:
        txt = f"04\nUploading...\n[{bar(pct)}] {pct:.0f}% ETA {eta}"
    try:
        tg('editMessageText', {'chat_id': t['user'], 'message_id': t['msg'], 'text': txt})
    except Exception:
        pass


def finalize_task(t):
    lang = t.get('lang', 'ar')
    res = t.get('result', {}) or {}
    msgs = res.get('msgs') or []
    # كاش فوري للرابط+الجودة
    r = Handler.rdb
    if msgs:
        r.set(f"done:{t['pageid']}:{t['quality']}",
              json.dumps({'msgs': msgs, 'sizes': res.get('sizes'),
                          'name': t.get('name'), 'thumb': t.get('thumb'),
                          'quality': t.get('quality'), 'parts': res.get('parts')}))
    # إرسال الفيديوهات للمستخدم (forward من القناة — فوري وبدون bandwidth)
    for mid in msgs:
        try:
            tg('forwardMessage', {'chat_id': t['user'], 'from_chat_id': CHANNEL, 'message_id': mid})
        except Exception:
            pass
    # الرسالة النهائية: ملخص + thumbnail
    q, sizes = t.get('quality'), res.get('sizes') or []
    total_mb = sum(sizes) / 1024 / 1024 if sizes else 0
    txt = (f"{t.get('name', '')}\n---\nPart: 01/{len(msgs) or 1}\n"
           f"Quality: {q}\nSize: {total_mb:.0f} MB")
    try:
        if t.get('thumb'):
            tg('editMessageMedia', {'chat_id': t['user'], 'message_id': t['msg'],
                                    'media': json.dumps({'type': 'photo', 'media': t['thumb'], 'caption': txt})})
        else:
            tg('editMessageText', {'chat_id': t['user'], 'message_id': t['msg'], 'text': txt})
    except Exception:
        pass


# ---------------- البوت ----------------

def handle_update(r, up):
    if 'callback_query' in up:
        cq = up['callback_query']
        uid = cq['from']['id']
        lang = lang_of(r, uid)
        data = cq.get('data', '')
        try:
            tg('answerCallbackQuery', {'callback_query_id': cq['id']})
        except Exception:
            pass
        if data.startswith('lang:'):
            r.set(f'lang:{uid}', data.split(':')[1])
            try:
                tg('sendMessage', {'chat_id': uid, 'text': STR['start'][data.split(':')[1]]})
            except Exception:
                pass
            return
        if data.startswith('b:'):
            tid = data.split(':')[1]
            raw = r.get(f'task:{tid}')
            if raw:
                t = json.loads(raw)
                if t.get('status') == 'queued':
                    # إزالة من الطابور
                    q = r.cmd('LRANGE', 'queue', '0', '-1') or []
                    rest = [x for x in q if json.loads(x).get('id') != tid]
                    r.delete('queue')
                    for x in rest:
                        r.rpush('queue', x)
                    r.delete(f'task:{tid}')
            try:
                tg('editMessageText', {'chat_id': uid, 'message_id': cq['message']['message_id'],
                                       'text': STR['start'][lang]})
            except Exception:
                pass
            return
        if data.startswith('q:'):
            _, tid, q = data.split(':')
            raw = r.get(f'task:{tid}')
            if not raw:
                return
            t = json.loads(raw)
            if t.get('status') != 'new':
                return
            if r.scard('active') >= MAX_ACTIVE and not r.get(f"done:{t['pageid']}:{q}"):
                try:
                    tg('answerCallbackQuery', {'callback_query_id': cq['id'], 'text': STR['busy'][lang], 'show_alert': True})
                except Exception:
                    pass
                return
            t['quality'] = q
            # ⚡ كاش؟ إرسال فوري
            hit = r.get(f"done:{t['pageid']}:{q}")
            if hit:
                d = json.loads(hit)
                event(r, f"instant {tid} {q} (cached)")
                for mid in d.get('msgs', []):
                    try:
                        tg('forwardMessage', {'chat_id': uid, 'from_chat_id': CHANNEL, 'message_id': mid})
                    except Exception:
                        pass
                try:
                    tg('editMessageText', {'chat_id': uid, 'message_id': cq['message']['message_id'],
                                           'text': f"{d.get('name', '')}\n⚡ cached — sent instantly"})
                except Exception:
                    pass
                r.delete(f'task:{tid}')
                return
            t['status'] = 'queued'
            t['user'] = uid
            t['msg'] = cq['message']['message_id']
            t['lang'] = lang
            r.set(f"task:{tid}", json.dumps(t))
            r.rpush('queue', json.dumps(t))
            event(r, f"queued {tid} {q}")
            try:
                tg('editMessageText', {'chat_id': uid, 'message_id': cq['message']['message_id'],
                                       'text': STR['st_analysis'][lang]})
            except Exception:
                pass
            return
        return
    if 'message' in up:
        m = up['message']
        uid = m['from']['id']
        txt = (m.get('text') or '').strip()
        if txt == '/start':
            kb = {'inline_keyboard': [[{'text': 'العربية', 'callback_data': 'lang:ar'},
                                       {'text': 'English', 'callback_data': 'lang:en'}]]}
            try:
                tg('sendMessage', {'chat_id': uid, 'text': '🌐 / Language', 'reply_markup': json.dumps(kb)})
                tg('sendMessage', {'chat_id': uid, 'text': STR['start'][lang_of(r, uid)]})
            except Exception:
                pass
            return
        if txt == '/language':
            kb = {'inline_keyboard': [[{'text': 'العربية', 'callback_data': 'lang:ar'},
                                       {'text': 'English', 'callback_data': 'lang:en'}]]}
            try:
                tg('sendMessage', {'chat_id': uid, 'text': '🌐', 'reply_markup': json.dumps(kb)})
            except Exception:
                pass
            return
        if 'fasel-hd.co' in txt and ('?p=' in txt or '/episodes/' in txt or '/movies/' in txt):
            lang = lang_of(r, uid)
            stop_typing = {'flag': False}

            def typing_loop():
                while not stop_typing['flag']:
                    try:
                        tg('sendChatAction', {'chat_id': uid, 'action': 'upload_photo'}, timeout=15)
                    except Exception:
                        pass
                    for _ in range(8):
                        if stop_typing['flag']:
                            break
                        time.sleep(0.5)

            th = threading.Thread(target=typing_loop, daemon=True)
            th.start()
            try:
                info = extract_info(txt.split()[0])
            finally:
                stop_typing['flag'] = True
            if not info.get('link'):
                try:
                    tg('sendMessage', {'chat_id': uid, 'text': 'Link: ERROR — try again later'})
                except Exception:
                    pass
                return
            tid = f"{uid}_{int(time.time())}"
            t = {'id': tid, 'pageid': pageid_of(txt), 'url': txt.split()[0],
                 'name': info.get('name', ''), 'poster': info.get('poster'),
                 'thumb': info.get('thumbnail'), 'link': info.get('link'),
                 'status': 'new', 'lang': lang}
            r.set(f'task:{tid}', json.dumps(t))
            event(r, f"link {t['pageid']} {t['name'][:40]} thumb={'Y' if t['thumb'] else 'N'}")
            cap = t['name']
            kb = quality_keyboard(tid, t['pageid'], r, lang)
            # رسالة واحدة فقط (صورة) — لا حذف ولا رسائل جديدة بعدها، كل تحديث تعديل
            try:
                if t.get('poster'):
                    tg('sendPhoto', {'chat_id': uid, 'photo': t['poster'], 'caption': cap,
                                     'reply_markup': json.dumps(kb)})
                else:
                    tg('sendMessage', {'chat_id': uid, 'text': cap,
                                       'reply_markup': json.dumps(kb)})
            except Exception:
                pass
            return
        if txt in ('/diag', '/status'):
            try:
                qn = r.llen('queue')
                active = r.smembers('active')
                hbs = []
                for w in ('w1', 'w2', 'w3', 'w4'):
                    hbs.append(f"{w}: {r.get(f'hb:{w}') or '—'}")
                evts = r.cmd('LRANGE', 'events', '0', '-1') or []
                tg('sendMessage', {'chat_id': uid,
                                   'text': f"queue={qn} active={len(active)}\n" + '\n'.join(hbs) +
                                           '\nevents:\n' + '\n'.join(evts[-8:])})
            except Exception:
                pass
            return


def poll_loop(r):
    import concurrent.futures as cf
    pool = cf.ThreadPoolExecutor(max_workers=8)

    def safe(up):
        try:
            handle_update(r, up)
        except Exception as e:
            try:
                event(r, f'update_err {str(e)[:100]}')
            except Exception:
                pass

    offset = 0
    while not shutdown['flag']:
        try:
            res = tg('getUpdates', {'offset': offset, 'timeout': 30}, timeout=60)
            for up in res.get('result', []):
                offset = up['update_id'] + 1
                pool.submit(safe, up)
        except Exception:
            time.sleep(3)


def main():
    r = RedisMini()
    r.connect()
    Handler.rdb = r
    # إعادة أي مهام عالقة claimed (من دورة سابقة) إلى queued
    try:
        for tid in r.smembers('claimed_ids'):
            raw = r.get(f'task:{tid}')
            if raw:
                t = json.loads(raw)
                if t.get('status') == 'claimed':
                    t['status'] = 'queued'
                    r.set(f'task:{tid}', json.dumps(t))
                    r.rpush('queue', json.dumps(t))
            r.srem('claimed_ids', tid)
        r.delete('active')
    except Exception as e:
        print('requeue: ' + str(e)[:100], flush=True)
    # إعلان رابط الـ API في الرسالة 13 (تعديل، لا رسالة جديدة)
    if API_URL:
        try:
            tg('editMessageText', {'chat_id': CHANNEL, 'message_id': MSG_LINK,
                                   'text': f'FaselHD Farm API\n{API_URL}\nUpdated: {time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())}'})
            print('MSG13 updated', flush=True)
        except Exception as e:
            print('MSG13 edit failed: ' + str(e)[:150], flush=True)
            try:
                s = tg('sendMessage', {'chat_id': CHANNEL, 'text': f'FaselHD Farm API\n{API_URL}'})
                print('MSG13 new id: ' + str(s['result']['message_id']), flush=True)
            except Exception as e2:
                print('MSG13 send failed: ' + str(e2)[:150], flush=True)
    srv = ThreadingHTTPServer(('127.0.0.1', PORT), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    threading.Thread(target=poll_loop, args=(r,), daemon=True).start()
    print(f'MAIN up (port {PORT})', flush=True)
    t0 = time.time()
    while time.time() - t0 < MAX_RUNTIME and not shutdown['flag']:
        time.sleep(10)
    shutdown['flag'] = True
    print('MAIN shutting down — BGSAVE + dump backup', flush=True)
    try:
        r.bgsave()
        time.sleep(5)
    except Exception as e:
        print('BGSAVE: ' + str(e)[:100], flush=True)


if __name__ == '__main__':
    main()
