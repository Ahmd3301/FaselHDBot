# FaselHDBot — هيكل الملفات وكل الأكواد

> وُلّد آلياً من الملفات الفعلية — الكود مطابق حرفياً لما يعمل في الـ repo.

## شجرة المشروع

```
FaselHDBot/
├── farm/
│   ├── main_bot.py      # البوت الرئيسي + API + مهام + كاش
│   ├── worker.py        # العامل (claim/download/upload/report)
│   └── redis_mini.py    # عميل Redis RESP (stdlib فقط)
├── scripts/
│   ├── exFaselHD1234.js # مستخرج fasel-hd (اسم/Poster/Thumbnail/رابط)
│   ├── tg_upload.py     # الرفع sendVideo + تقسيم + thumbnail + شريط
│   ├── nm3u8_progress.py# شريط التنزيل الحقيقي (لوج الأداة + tmp-dir)
│   └── monitor.py       # مراقبة CPU/RAM/شبكة
├── .github/workflows/
│   └── faselhd-farm.yml # الدورة: 5 jobs + tunnel + نسخ + إعادة تشغيل
└── docs/
    ├── 01-IDEA.md
    ├── 02-HOW-IT-WORKS.md
    └── 03-STRUCTURE-AND-CODE.md (هذا الملف)
```

## `farm/redis_mini.py` ‏(108 سطر)

```python
"""redis_mini.py — عميل Redis minimal ببروتوكول RESP (stdlib فقط).

يدعم: PING/SET/GET/DEL/EXPIRE/HSET/HGET/HGETALL/HDEL/SADD/SMEMBERS/SREM/
SCARD/RPUSH/LPUSH/RPOP/LLEN/BGSAVE/LASTSAVE/FLUSHDB/QUIT.
كافٍ لاحتياجات main-bot (مهام + كاش + لغة) دون أي مكتبات خارجية.
"""
import socket


class RedisMini:
    def __init__(self, host='127.0.0.1', port=6379, timeout=10):
        self.host, self.port, self.timeout = host, port, timeout
        self.sock = None
        self._buf = b''

    def connect(self):
        self.sock = socket.create_connection((self.host, self.port), self.timeout)

    def close(self):
        try:
            self.cmd('QUIT')
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass

    def _send(self, data: bytes):
        self.sock.sendall(data)

    def _readline(self):
        while b'\r\n' not in self._buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError('redis closed connection')
            self._buf += chunk
        line, self._buf = self._buf.split(b'\r\n', 1)
        return line

    def _readbulk(self, n):
        while len(self._buf) < n + 2:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError('redis closed connection')
            self._buf += chunk
        data, self._buf = self._buf[:n], self._buf[n + 2:]
        return data

    def _readreply(self):
        line = self._readline()
        t, payload = line[:1], line[1:]
        if t == b'+':
            return payload.decode()
        if t == b'-':
            raise RuntimeError('redis: ' + payload.decode())
        if t == b':':
            return int(payload)
        if t == b'$':
            n = int(payload)
            if n < 0:
                return None
            return self._readbulk(n).decode()
        if t == b'*':
            n = int(payload)
            if n < 0:
                return None
            out = []
            for _ in range(n):
                h = self._readline()
                if h[:1] != b'$':
                    raise RuntimeError('unexpected redis reply: ' + h.decode())
                m = int(h[1:])
                out.append(None if m < 0 else self._readbulk(m).decode())
            return out
        raise RuntimeError('unknown redis reply: ' + line.decode(errors='replace'))

    def cmd(self, *args):
        parts = [f'*{len(args)}\r\n'.encode()]
        for a in args:
            b = str(a).encode()
            parts.append(f'${len(b)}\r\n'.encode() + b + b'\r\n')
        self._send(b''.join(parts))
        return self._readreply()

    # مختصرات
    def ping(self): return self.cmd('PING')
    def set(self, k, v): return self.cmd('SET', k, v)
    def get(self, k): return self.cmd('GET', k)
    def delete(self, *ks): return self.cmd('DEL', *ks)
    def expire(self, k, s): return self.cmd('EXPIRE', k, s)
    def hset(self, k, f, v): return self.cmd('HSET', k, f, v)
    def hget(self, k, f): return self.cmd('HGET', k, f)
    def hdel(self, k, *fs): return self.cmd('HDEL', k, *fs)
    def hgetall(self, k):
        import itertools
        raw = self.cmd('HGETALL', k) or []
        return dict(itertools.batched(raw, 2)) if hasattr(itertools, 'batched') else dict(zip(raw[::2], raw[1::2]))
    def sadd(self, k, *ms): return self.cmd('SADD', k, *ms)
    def smembers(self, k): return self.cmd('SMEMBERS', k) or []
    def srem(self, k, *ms): return self.cmd('SREM', k, *ms)
    def scard(self, k): return self.cmd('SCARD', k)
    def rpush(self, k, *vs): return self.cmd('RPUSH', k, *vs)
    def rpop(self, k): return self.cmd('RPOP', k)
    def llen(self, k): return self.cmd('LLEN', k)
    def bgsave(self): return self.cmd('BGSAVE')
    def lastsave(self): return self.cmd('LASTSAVE')
```

## `farm/main_bot.py` ‏(542 سطر)

```python
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
```

## `farm/worker.py` ‏(228 سطر)

```python
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
```

## `scripts/exFaselHD1234.js` ‏(439 سطر)

```javascript
const https = require('https');
const http = require('http');
const vm = require('vm');
const readline = require('readline');

process.on('uncaughtException', (e) => { console.error('Error: ' + (e.message || e)); process.exit(1); });
process.on('unhandledRejection', (e) => { console.error('Error: ' + (e.message || e)); process.exit(1); });

const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';

const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 50 });
const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 50 });

function fetchUrl(url, retries = 2) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const agent = url.startsWith('https') ? httpsAgent : httpAgent;
    const req = mod.get(url, {
      agent,
      headers: { 'User-Agent': UA, 'Accept': 'text/html,*/*', 'Accept-Language': 'ar,en-US;q=0.7' }
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        let loc = res.headers.location;
        if (loc.startsWith('/')) { const p = new URL(url); loc = p.protocol + '//' + p.host + loc; }
        res.resume();
        return resolve(fetchUrl(loc, retries));
      }
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => {
        if (retries > 0 && (d.includes('Just a moment') || d.includes('cf-browser-verification'))) {
          setTimeout(() => resolve(fetchUrl(url, retries - 1)), 2000 + Math.random() * 3000);
        } else resolve(d);
      });
    });
    req.on('error', (e) => reject(e));
    req.setTimeout(20000, () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

function cleanName(name) {
  let n = name
    .replace(/&#\d+;/g, '')
    .replace(/<[^>]+>/g, '')
    .replace(/مسلسل\s*/g, '')
    .replace(/فيلم\s*/g, '')
    .replace(/ الموسم\s*\S+/g, '')
    .replace(/[–\-]\s*الحلقة\s*\d+/g, '')
    .replace(/الحلقة\s*\d+/g, '')
    .replace(/مترجم/g, '')
    .replace(/[–\-]\s*فاصل إعلاني.*$/i, '')
    .replace(/فاصل إعلاني.*$/i, '')
    .replace(/&#8211;/g, '')
    .replace(/\s+/g, ' ')
    .trim();
  return n;
}

function parseName(html) {
  const h1 = html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i);
  if (h1) return cleanName(h1[1]);
  const h3 = html.match(/<div class="h3"[^>]*>([\s\S]*?)<\/div>/i);
  if (h3) return cleanName(h3[1]);
  const title = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  if (title) return cleanName(title[1]);
  return 'Unknown';
}

// NEW: poster من صفحة الحلقة (schema.org itemprop=image) — مثال:
// https://static.faselhdcdn.com/wp-content/uploads/2023/11/23de45b7....jpg
function parsePoster(html) {
  let m = html.match(/<meta[^>]+itemprop=["']image["'][^>]+content=["']([^"']+)["']/i)
       || html.match(/<meta[^>]+content=["']([^"']+)["'][^>]+itemprop=["']image["']/i);
  if (m) return m[1];
  const imgs = [...html.matchAll(/<img[^>]+src=["']([^"']+)["'][^>]*>/gi)].map(x => x[1]);
  const hit = imgs.find(s => /faselhdcdn\.com\/wp-content\/uploads\//i.test(s)
    && !/logo|favicon|themes\//i.test(s));
  return hit || null;
}

// NEW: thumbnail من صفحة المشغّل (poster وسم <video>) — مثال:
// https://img.scdns.io/thumb/d721cfdb..../large.jpg
function parseThumbnail(playerHtml) {
  if (!playerHtml) return null;
  let m = playerHtml.match(/<video[^>]+poster=["']([^"']+)["']/i)
       || playerHtml.match(/<video[^>]+poster='([^']+)'/i);
  if (m) return m[1];
  m = playerHtml.match(/https?:\/\/img\.scdns\.io\/thumb\/[^"'<>\s]+/i);
  return m ? m[0] : null;
}

// NEW: Extract episode number from Arabic text like "الحلقة 1" or "الحلقة 01"
function parseEpisodeNumber(html) {
  const m = html.match(/الحلقة\s*(\d+)/i);
  if (m) return String(m[1]).padStart(2, '0');
  return null;
}

function parsePlayerUrls(html) {
  const urls = [];
  let rx = /player_iframe\.location\.href\s*=\s*'([^']+)'/g;
  let m;
  while ((m = rx.exec(html)) !== null) urls.push(decodeURIComponent(m[1]));
  rx = /(?:data-src|src)\s*=\s*"([^"]*video_player[^"]*)"/g;
  while ((m = rx.exec(html)) !== null) urls.push(decodeURIComponent(m[1]));
  return [...new Set(urls)];
}

function parseEpisodeLinks(html) {
  const links = [];
  let rx = /<a\s+href="([^"]*\/episodes\/[^"]*)"[^>]*>\s*الحلقة\s*\d+\s*<\/a>/gi;
  let m;
  while ((m = rx.exec(html)) !== null) links.push(m[1]);
  if (links.length > 0) return links;
  const rx2 = /<a\s+href="([^"]*\/asian-episodes\/[^"]*)"[^>]*>\s*الحلقة\s*\d+\s*<\/a>/gi;
  while ((m = rx2.exec(html)) !== null) links.push(m[1]);
  if (links.length > 0) return links;
  const epAllMatch = html.match(/<div class="epAll"[^>]*>([\s\S]*?)<\/div>\s*<\/div>/i);
  if (epAllMatch) {
    let rx3 = /<a\s+href="([^"]+)"[^>]*>/g;
    while ((m = rx3.exec(epAllMatch[1])) !== null) links.push(m[1]);
  }
  return links;
}

function parseSeasonList(html) {
  const seasons = [];
  const nums = [];
  let rx = /<div class="title">موسم\s*(\d+)<\/div>/g;
  let m;
  while ((m = rx.exec(html)) !== null) nums.push(parseInt(m[1]));
  const posts = [];
  rx = /onclick="window\.location\.href\s*=\s*'\/\?p=(\d+)'/g;
  while ((m = rx.exec(html)) !== null) posts.push(m[1]);
  for (let i = 0; i < Math.min(nums.length, posts.length); i++) {
    seasons.push({ num: nums[i], postId: posts[i] });
  }
  return seasons;
}

function decodeM3u8(playerHtml, playerUrl) {
  const scriptRx = /<script[^>]*>([\s\S]*?)<\/script>/g;
  const candidates = [];
  let sm;
  while ((sm = scriptRx.exec(playerHtml)) !== null) {
    const s = sm[1];
    // أي سكربت inline كبير قد يبني رابط الـ m3u8 (المشغّل الجديد يوزعه على سكربتين)
    if (s.length > 2000) candidates.push(s);
  }
  if (candidates.length === 0) return null;

  let captured = '';
  const HlsCls = function () {};
  HlsCls.prototype.loadSource = function (url) { captured += 'loadSource("' + url + '")'; };
  HlsCls.prototype.attachMedia = function () {};
  HlsCls.isSupported = () => true;
  const sandbox = {
    document: {
      write: (...args) => { captured += args.join(''); },
      writeln: (...args) => { captured += args.join('') + '\n'; },
      getElementById: (id) => id === 'video' ? { canPlayType: () => 'maybe', set src(v) { captured += '<video src="' + v + '">'; }, get src() { return ''; } } : null,
      createElement: (tag) => tag === 'video' ? { canPlayType: () => 'maybe', set src(v) { captured += '<video src="' + v + '">'; }, get src() { return ''; } } : { setAttribute: () => {}, appendChild: () => {}, addEventListener: () => {}, get src() { return ''; }, set src(v) { captured += '<e src="' + v + '">'; } },
      createTextNode: () => ({}),
      querySelectorAll: () => [],
      querySelector: () => null,
      body: { appendChild: () => {} },
      head: { appendChild: () => {} },
    },
    window: {
      location: { href: playerUrl, hostname: new URL(playerUrl).hostname, search: '' },
      addEventListener: () => {},
      setTimeout: (fn) => { try { fn(); } catch(e) {} },
      setInterval: () => ({}),
      Hls: HlsCls,
      navigator: { userAgent: UA },
      console: { log: () => {}, error: () => {}, warn: () => {} },
      atob: (s) => Buffer.from(s, 'base64').toString('binary'),
      btoa: (s) => Buffer.from(s, 'binary').toString('base64'),
    },
    location: { href: playerUrl, hostname: new URL(playerUrl).hostname, search: '' },
    navigator: { userAgent: UA },
    Hls: HlsCls,
    setTimeout: (fn) => { try { fn(); } catch(e) {} },
    console: { log: () => {}, error: () => {}, warn: () => {} },
  };

  try {
    const ctx = vm.createContext(sandbox);
    for (const code of candidates) {
      try { vm.runInContext(code, ctx, { timeout: 5000 }); } catch (e) {}
    }
  } catch (e) {}

  const urls = [];
  let rx = /data-url="([^"]*\.m3u8[^"]*)"/g;
  let m;
  while ((m = rx.exec(captured)) !== null) urls.push(m[1]);
  rx = /<video[^>]*src="([^"]*\.m3u8[^"]*)"/g;
  while ((m = rx.exec(captured)) !== null) urls.push(m[1]);
  rx = /loadSource\s*\(\s*['"]([^'"]*\.m3u8[^'"]*)['"]/gi;
  while ((m = rx.exec(captured)) !== null) urls.push(m[1]);
  rx = /\bsrc\s*=\s*"([^"]*\.m3u8[^"]*)"/gi;
  while ((m = rx.exec(captured)) !== null) urls.push(m[1]);
  const uniq = [...new Set(urls)];
  // فضّل master.m3u8 عند وجوده (يحمل كل الجودات)، وإلا اترك المباشرة كما هي
  uniq.sort((a, b) => ((b.includes('master.m3u8') ? 1 : 0) - (a.includes('master.m3u8') ? 1 : 0)));
  return uniq;
}

// يجلب كل صفحات المشغّل الصالحة (يتخطى Token Expired! وأي صفحة ميتة)
async function fetchWorkingPlayer(playerUrls) {
  for (const pu of playerUrls) {
    try {
      const playerHtml = await fetchUrl(pu);
      if (playerHtml && playerHtml.length >= 500) return { html: playerHtml, url: pu };
    } catch (e) {}
  }
  return null;
}

// يجلب كل الصفحات الصالحة دفعة واحدة (للـ Thumbnail الذي قد يكون في سيرفر دون غيره)
async function fetchAllWorkingPlayers(playerUrls) {
  const out = [];
  for (const pu of playerUrls) {
    try {
      const playerHtml = await fetchUrl(pu);
      if (playerHtml && playerHtml.length >= 500) out.push({ html: playerHtml, url: pu });
    } catch (e) {}
  }
  return out;
}

// يجرب كل روابط المشغّل بالترتيب حتى أول رابط m3u8 صالح
// (الموقع يعرض عدة سيرفرات وقد يكون أولها ميتاً: Token Expired!)
async function tryPlayers(playerUrls) {
  const found = await fetchWorkingPlayer(playerUrls);
  if (!found) return null;
  // جرّب الصفحة الشغالة أولاً ثم بقية الروابط كاحتياط
  const ordered = [found, ...playerUrls.filter(u => u !== found.url).map(u => ({ url: u }))];
  for (const item of ordered) {
    try {
      const html = item.html || await fetchUrl(item.url);
      if (!html || html.length < 500) continue;
      const urls = decodeM3u8(html, item.url);
      if (urls && urls.length > 0) return urls[0];
    } catch (e) {}
  }
  return null;
}

async function resolveEpisode(epUrl) {
  try {
    const html = await fetchUrl(epUrl);
    const playerUrls = parsePlayerUrls(html);
    if (playerUrls.length === 0) return null;
    return await tryPlayers(playerUrls);
  } catch (e) { return null; }
}

async function showUrls(label, epUrl) {
  const url = await resolveEpisode(epUrl);
  if (url) {
    console.log('Link ' + label + ': ' + url);
  } else {
    console.log('Link ' + label + ': ERROR');
  }
}

(async () => {
  try {
  const url = process.argv[2];
  if (!url) {
    console.log('Usage: node exFaselHD.cjs <URL>');
    process.exit(1);
  }

  // ============================================================
  // Direct Link Mode: when the URL contains ?p= or &p= (single post page)
  // Extract name, episode number, and master m3u8 link, then exit immediately.
  // ============================================================
  const isPostUrl = /[?&]p=\d+/.test(url);

  if (isPostUrl) {
    const html = await fetchUrl(url);
    const name = parseName(html);
    const episode = parseEpisodeNumber(html);
    const poster = parsePoster(html);
    const playerUrls = parsePlayerUrls(html);

    if (playerUrls.length === 0) {
      console.log('Name: ' + name);
      if (episode) console.log('Episode: ' + episode);
      if (poster) console.log('Poster: ' + poster);
      console.log('Link: ERROR (no player found)');
      process.exit(1);
    }

    const working = await fetchAllWorkingPlayers(playerUrls);
    let thumbnail = null;
    for (const w of working) {
      thumbnail = parseThumbnail(w.html);
      if (thumbnail) break;
    }
    let m3u8Link = null;
    for (const w of working) {
      try {
        const urls = decodeM3u8(w.html, w.url);
        if (urls && urls.length > 0) { m3u8Link = urls[0]; break; }
      } catch (e) {}
    }

    console.log('Name: ' + name);
    if (episode) console.log('Episode: ' + episode);
    if (poster) console.log('Poster: ' + poster);
    if (thumbnail) console.log('Thumbnail: ' + thumbnail);

    if (m3u8Link) {
      console.log('Link: ' + m3u8Link);
    } else {
      console.log('Link: ERROR (no m3u8 found)');
    }

    process.exit(0);
  }
  // ============================================================

  const html = await fetchUrl(url);

  const parsedUrl = new URL(url);
  const baseHost = parsedUrl.hostname;

  const name = parseName(html);
  const seasonList = parseSeasonList(html);
  const epLinks = parseEpisodeLinks(html);
  const playerUrls = parsePlayerUrls(html);
  const hasSeasonsInUrl = url.includes('/seasons/');
  const pageType = (hasSeasonsInUrl && seasonList.length > 0) || seasonList.length > 1 ? 'seasons' : (epLinks.length > 0 ? 'series' : 'movie');

  console.log('Name:');
  console.log(name);

  if (pageType === 'seasons') {
    const allEps = [];
    const seasonCounts = {};

    const promises = seasonList.map(async (s) => {
      let eps = [];
      if (s.postId) {
        try {
          const sh = await fetchUrl(`https://${baseHost}/?p=${s.postId}`);
          eps = parseEpisodeLinks(sh);
        } catch (e) { console.error('Error: ' + e.message); }
      }
      return { s, eps };
    });

    const results = await Promise.all(promises);

    for (const r of results.sort((a, b) => a.s.num - b.s.num)) {
      seasonCounts[r.s.num] = r.eps.length;
      const sKey = 'S' + String(r.s.num).padStart(2, '0');
      for (let i = 0; i < r.eps.length; i++) {
        allEps.push({ url: r.eps[i], season: r.s.num, episode: i + 1, label: sKey + 'Eps' + String(i + 1).padStart(2, '0') });
      }
    }

    const seasonLabel = seasonList.map(s => 'S' + String(s.num).padStart(2, '0') + '{' + (seasonCounts[s.num] || 0) + '}').join(', ');
    console.log('seasons: ' + seasonList.length + ' (' + seasonLabel + ', )');
    console.log('Eps: ' + allEps.length);
    if (allEps.length > 0) {
      await showUrls(allEps[0].label, allEps[0].url);
    }

    if (process.stdin.isTTY) {
      const ri = readline.createInterface({ input: process.stdin, output: process.stdout });
      (function loop() {
        ri.question('Enter the number of any episode to reveal the link> ', async (ans) => {
          const n = parseInt(ans);
          if (isNaN(n) || n < 1 || n > allEps.length) { ri.close(); return; }
          await showUrls(allEps[n - 1].label, allEps[n - 1].url);
          loop();
        });
      })();
    } else {
      let _d = '';
      process.stdin.setEncoding('utf8');
      process.stdin.on('data', c => _d += c);
      process.stdin.on('end', async () => {
        const lines = _d.trim().split('\n');
        for (const line of lines) {
          const n = parseInt(line.trim());
          if (isNaN(n) || n < 1 || n > allEps.length) break;
          await showUrls(allEps[n - 1].label, allEps[n - 1].url);
        }
      });
    }

  } else if (pageType === 'series') {
    console.log('Eps: ' + epLinks.length);
    if (epLinks.length > 0) {
      await showUrls('Eps01', epLinks[0]);
    }
    if (process.stdin.isTTY) {
      const ri = readline.createInterface({ input: process.stdin, output: process.stdout });
      (function loop() {
        ri.question('Enter the number of any episode to reveal the link> ', async (ans) => {
          const n = parseInt(ans);
          if (isNaN(n) || n < 1 || n > epLinks.length) { ri.close(); return; }
          const label = 'Eps' + String(n).padStart(2, '0');
          await showUrls(label, epLinks[n - 1]);
          loop();
        });
      })();
    } else {
      let _d = '';
      process.stdin.setEncoding('utf8');
      process.stdin.on('data', c => _d += c);
      process.stdin.on('end', async () => {
        const lines = _d.trim().split('\n');
        for (const line of lines) {
          const n = parseInt(line.trim());
          if (isNaN(n) || n < 1 || n > epLinks.length) break;
          const label = 'Eps' + String(n).padStart(2, '0');
          await showUrls(label, epLinks[n - 1]);
        }
      });
    }

  } else {
    const link = playerUrls.length > 0 ? await tryPlayers(playerUrls) : null;
    console.log('Link:');
    console.log(link || 'ERROR');
  }
  } catch (e) {
    console.error('Error: ' + e.message);
    process.exit(1);
  }
})();
```

## `scripts/tg_upload.py` ‏(381 سطر)

```python
#!/usr/bin/env python3
"""
tg_upload.py — رفع ملف MP4 إلى تليجرام عبر Local Bot API Server **كفيديو** (sendVideo).

- الإرسال بـ sendVideo (قابل للتشغيل داخل تليجرام) مع duration/width/height من ffprobe.
- إن تجاوز الحجم PART_MAX (1.9GB) يُقسَّم بـ ffmpeg (-c copy) إلى أجزاء MP4
  صالحة ومستقلة (كل جزء ≤1.9GB) وإعادة التجميع بـ concat demuxer.
- شريط تقدم حقيقي أثناء الرفع (نسبة من البايتات المرسلة فعلاً + سرعة + ETA +
  متوسط CPU/RAM من /proc).
- يقرأ الملف من القرص مباشرة (streaming multipart) دون تحميله في الذاكرة.
- يحترم Rate Limit: عند 429 ينام retry_after ثم يعيد (حتى 5 محاولات).

الاستخدام:
  TG_TOKEN=... python3 scripts/tg_upload.py --file movie.mp4 \
      --api-base http://127.0.0.1:8081 --chat-id "-100..." \
      --caption "Oppenheimer 1080p" --out ./tg-results
"""
import argparse
import hashlib
import http.client
import json
import os
import subprocess
import sys
import time
import urllib.parse

PART_MAX = 1900 * 1024 * 1024  # 1.9 GiB — بهامش أمان تحت سقف 2000MB للسيرفر المحلي
CHUNK = 1024 * 1024            # 1 MiB لكل قراءة أثناء الرفع
RETRY_429_MAX = 5
GAP_BETWEEN_PARTS = 5          # ثوانٍ بين الأجزاء (أمان من Flood)
PRINT_EVERY_S = 2.0            # تحديث شريط التقدم


# ---------- موارد النظام (Linux /proc, stdlib فقط) ----------

def read_cpu():
    try:
        with open('/proc/stat') as f:
            p = f.readline().split()
        v = list(map(int, p[1:8]))
        return sum(v), v[3] + v[4]
    except Exception:
        return None


def cpu_pct_since(prev):
    cur = read_cpu()
    if cur is None or prev is None or prev[0] is None:
        return 0.0, cur
    dt = cur[0] - prev[0]
    pct = (1 - (cur[1] - prev[1]) / dt) * 100 if dt else 0.0
    return max(0.0, min(100.0 * os.cpu_count(), pct)), cur


def mem_used_mb():
    try:
        info = {}
        with open('/proc/meminfo') as f:
            for line in f:
                k, v = line.split(':', 1)
                info[k.strip()] = int(v.strip().split()[0])
        return (info['MemTotal'] - info['MemAvailable']) / 1024
    except Exception:
        return 0.0


# ---------- شريط التقدم ----------

def bar(pct):
    filled = min(10, max(0, int(pct // 10)))
    return '■' * filled + '□' * (10 - filled)


def fmt_eta(sec):
    if sec is None or sec < 0 or sec == float('inf'):
        return '--:--:--'
    sec = int(sec)
    return f'{sec // 3600:02d}:{(sec % 3600) // 60:02d}:{sec % 60:02d}'


class Progress:
    def __init__(self, title, total):
        self.title = title
        self.total = total
        self.sent = 0
        self.t0 = time.time()
        self.last_print = self.t0  # أول طباعة بعد PRINT_EVERY_S ثانية من البيانات الفعلية
        self.cpu_prev = read_cpu()
        self.cpu_sum = 0.0
        self.cpu_n = 0
        self.mem_sum = 0.0
        self.mem_n = 0
        self.last_bytes = 0
        self.last_t = self.t0

    def sample_res(self):
        pct, self.cpu_prev = cpu_pct_since(self.cpu_prev)
        self.cpu_sum += pct
        self.cpu_n += 1
        self.mem_sum += mem_used_mb()
        self.mem_n += 1

    def update(self, nbytes, force=False):
        self.sent += nbytes
        now = time.time()
        if not force and now - self.last_print < PRINT_EVERY_S:
            return
        self.last_print = now
        self.sample_res()
        pct = min(100.0, self.sent / self.total * 100) if self.total else 0
        dt = now - self.last_t or 1e-6
        speed = (self.sent - self.last_bytes) / dt / 1024 / 1024  # MB/s
        self.last_bytes, self.last_t = self.sent, now
        eta = (self.total - self.sent) / (speed * 1024 * 1024) if speed > 0 else None
        cpu_avg = self.cpu_sum / self.cpu_n if self.cpu_n else 0
        mem_avg = self.mem_sum / self.mem_n if self.mem_n else 0
        print(f'{self.title}\n[{bar(pct)}] {pct:.0f}%\n'
              f'ETA: {fmt_eta(eta)}\nSpeed: {speed:.0f} MB/s\n'
              f'Average CPU Usage: {cpu_avg:.1f}%\n'
              f'Average RAM Usage: {mem_avg:.0f} MB', flush=True)

    def finish(self):
        self.update(0, force=True)
        return {'cpu_avg': round(self.cpu_sum / self.cpu_n, 1) if self.cpu_n else 0,
                'ram_avg_mb': round(self.mem_sum / self.mem_n) if self.mem_n else 0}


# ---------- فحص الفيديو ----------

def probe(path):
    try:
        r = subprocess.run(
            ['ffprobe', '-v', 'error', '-select_streams', 'v:0',
             '-show_entries', 'stream=width,height,avg_frame_rate,duration',
             '-show_entries', 'format=duration',
             '-of', 'json', path],
            capture_output=True, text=True, timeout=120)
        j = json.loads(r.stdout or '{}')
        s = (j.get('streams') or [{}])[0]
        dur = float(s.get('duration') or (j.get('format') or {}).get('duration') or 0)
        fps = s.get('avg_frame_rate', '0/0')
        return {'width': int(s.get('width') or 0),
                'height': int(s.get('height') or 0),
                'duration': dur, 'fps': fps}
    except Exception:
        return {'width': 0, 'height': 0, 'duration': 0, 'fps': ''}


def split_mp4(path, out_dir, part_max=PART_MAX):
    """تقسيم بـ ffmpeg (-c copy) إلى أجزاء MP4 صالحة، كل جزء ≤ الحد."""
    size = os.path.getsize(path)
    if size <= part_max:
        return [(path, size, 1, 1)]
    info = probe(path)
    total_dur = info['duration']
    if total_dur <= 0:
        raise RuntimeError('cannot probe duration for splitting')
    parts, start, idx = [], 0.0, 1
    while start < total_dur - 1 and idx <= 12:
        out = os.path.join(out_dir, f'{os.path.basename(path)}.vpart{idx:02d}.mp4')
        r = subprocess.run(
            ['ffmpeg', '-hide_banner', '-y', '-ss', f'{start:.3f}', '-i', path,
             '-c', 'copy', '-fs', str(part_max), '-movflags', '+faststart', out],
            capture_output=True, text=True, timeout=1800)
        if r.returncode != 0 or not os.path.exists(out):
            raise RuntimeError('ffmpeg split failed: ' + (r.stderr or '')[-500:])
        d = probe(out)['duration'] or 0
        if d <= 0:
            raise RuntimeError('split produced zero-duration part')
        parts.append((out, os.path.getsize(out), idx, None))
        start += d
        idx += 1
    parts = [(p, s, i, len(parts)) for (p, s, i, _) in parts]
    return parts


# ---------- رفع streaming ----------

def make_thumb(src, out_dir):
    """ضغط صورة الغلاف إلى JPEG صغير (<200KB شرط تليجرام) وإرجاع مساره أو None."""
    try:
        out = os.path.join(out_dir, 'thumb_small.jpg')
        for w, q in ((320, 6), (256, 8), (192, 10)):
            r = subprocess.run(
                ['ffmpeg', '-hide_banner', '-loglevel', 'error', '-y', '-i', src,
                 '-vframes', '1', '-vf', f'scale={w}:-1', '-q:v', str(q), out],
                capture_output=True, text=True, timeout=120)
            if r.returncode == 0 and os.path.exists(out) and os.path.getsize(out) <= 200 * 1024:
                return out
        return out if os.path.exists(out) and os.path.getsize(out) <= 200 * 1024 else None
    except Exception:
        return None


class StreamingMultipart:
    def __init__(self, fields, file_field, file_path, file_name, thumb_path=None):
        self.boundary = '----tgup' + hashlib.md5(os.urandom(16)).hexdigest()
        self.fields = fields
        self.file_field = file_field
        self.file_path = file_path
        self.file_name = file_name
        self.thumb_path = thumb_path
        self.thumb_bytes = b''
        if thumb_path and os.path.exists(thumb_path):
            with open(thumb_path, 'rb') as f:
                self.thumb_bytes = f.read()
        self.file_size = os.path.getsize(file_path)
        pre = b''
        for k, v in fields.items():
            pre += ('--' + self.boundary + '\r\n').encode()
            pre += (f'Content-Disposition: form-data; name="{k}"\r\n\r\n').encode()
            pre += str(v).encode() + b'\r\n'
        pre += ('--' + self.boundary + '\r\n').encode()
        pre += (f'Content-Disposition: form-data; name="{file_field}"; '
                f'filename="{file_name}"\r\n'
                f'Content-Type: video/mp4\r\n\r\n').encode()
        self.pre = pre
        self.post = ('\r\n--' + self.boundary + '--\r\n').encode()
        thumb_part = b''
        if self.thumb_bytes:
            thumb_part = ('--' + self.boundary + '\r\n').encode()
            thumb_part += ('Content-Disposition: form-data; name="thumbnail"; '
                           'filename="thumb.jpg"\r\nContent-Type: image/jpeg\r\n\r\n').encode()
            thumb_part += self.thumb_bytes + b'\r\n'
        self.thumb_part = thumb_part
        self.total = len(pre) + self.file_size + len(self.post) + len(thumb_part)

    def body_iter(self, progress=None):
        yield self.pre
        if progress:
            progress.update(len(self.pre))
        with open(self.file_path, 'rb') as f:
            while True:
                b = f.read(CHUNK)
                if not b:
                    break
                yield b
                if progress:
                    progress.update(len(b))
        if self.thumb_part:
            yield self.thumb_part
            if progress:
                progress.update(len(self.thumb_part))
        yield self.post
        if progress:
            progress.update(len(self.post))


def post_video(api_base, token, fields, file_path, progress, timeout=3600, thumb_path=None):
    u = urllib.parse.urlparse(api_base)
    mp = StreamingMultipart(fields, 'video', file_path,
                            os.path.basename(file_path), thumb_path)
    progress.total = mp.total
    conn = http.client.HTTPConnection(u.hostname, u.port or 80, timeout=timeout)
    conn.putrequest('POST', f'/bot{token}/sendVideo')
    conn.putheader('Content-Type', f'multipart/form-data; boundary={mp.boundary}')
    conn.putheader('Content-Length', str(mp.total))
    conn.endheaders()
    t0 = time.time()
    for piece in mp.body_iter(progress):
        conn.send(piece)
    resp = conn.getresponse()
    data = resp.read()
    dt = time.time() - t0
    progress.finish()
    try:
        return json.loads(data.decode('utf8', 'replace')), dt, mp.total
    except Exception:
        return {'ok': False, 'error_code': resp.status,
                'description': data[:300].decode('utf8', 'replace')}, dt, mp.total


def main():
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument('--file', required=True)
    ap.add_argument('--api-base', default='http://127.0.0.1:8081')
    ap.add_argument('--token', default=os.environ.get('TG_TOKEN', ''))
    ap.add_argument('--chat-id', required=True)
    ap.add_argument('--caption', default='')
    ap.add_argument('--out', default='./tg-results')
    ap.add_argument('--thumb', default=None,
                    help='صورة غلاف (تُضغط <200KB وتُرفق كـ thumbnail)')
    ap.add_argument('--part-max', type=int, default=PART_MAX)
    a = ap.parse_args()
    if not a.token:
        print('FATAL: missing bot token (use --token or TG_TOKEN)', flush=True)
        return 1
    os.makedirs(a.out, exist_ok=True)
    total_size = os.path.getsize(a.file)
    meta0 = probe(a.file)
    print(f"SOURCE size={total_size} bytes ({total_size/1024/1024:.1f} MB) "
          f"{meta0['width']}x{meta0['height']} dur={meta0['duration']:.0f}s",
          flush=True)
    parts = split_mp4(a.file, a.out, a.part_max)
    print(f'PARTS count={len(parts)} method=sendVideo (limit {a.part_max} bytes each)',
          flush=True)
    results, t_all0 = [], time.time()
    thumb_small = make_thumb(a.thumb, a.out) if a.thumb else None
    print(f'THUMB={thumb_small or "none"}', flush=True)
    for path, size, idx, total in parts:
        meta = probe(path)
        cap = (f"{a.caption}\nPART {idx}/{total} ({size/1024/1024:.0f} MB)"
               if total > 1 else a.caption)
        print(f'Uploading VIDEO PART {idx}/{total}...', flush=True)

        def send_with_title(thumb=None):
            progress = Progress(f'Uploading VIDEO PART {idx}/{total}...', 1)
            fields = {'chat_id': a.chat_id, 'caption': cap[:1024],
                      'supports_streaming': 'true'}
            if meta['duration'] > 0:
                fields['duration'] = str(int(meta['duration']))
            if meta['width'] > 0:
                fields['width'] = str(meta['width'])
                fields['height'] = str(meta['height'])
            for attempt in range(1, RETRY_429_MAX + 1):
                res, dt, sent = post_video(a.api_base, a.token, fields,
                                           path, progress, thumb_path=thumb)
                if res.get('ok'):
                    vid = res['result'].get('video', {})
                    return {'ok': True, 'method': 'sendVideo',
                            'message_id': res['result'].get('message_id'),
                            'file_id': vid.get('file_id'),
                            'file_size': vid.get('file_size'),
                            'seconds': round(dt, 1),
                            'mbps': round(sent * 8 / dt / 1e6, 1) if dt > 0 else 0,
                            'res_cpu_avg': round(progress.cpu_sum / progress.cpu_n, 1) if progress.cpu_n else 0,
                            'res_ram_avg': round(progress.mem_sum / progress.mem_n) if progress.mem_n else 0}
                desc = str(res.get('description', ''))
                if res.get('error_code') == 429 or 'retry' in desc.lower():
                    wait = 30
                    try:
                        wait = int(res.get('parameters', {}).get('retry_after', 30))
                    except Exception:
                        pass
                    print(f'  429 flood control, sleeping {wait}s (attempt {attempt})',
                          flush=True)
                    time.sleep(wait + 2)
                    # إعادة العدّاد لشريط جديد بعد الانتظار
                    progress = Progress(f'Uploading VIDEO PART {idx}/{total}... (retry)', 1)
                    continue
                return {'ok': False, 'error': desc[:300], 'seconds': round(dt, 1)}
            return {'ok': False, 'error': 'retries exhausted on 429'}

        r = send_with_title(thumb_small)
        r.update({'part': idx, 'of': total, 'bytes': size,
                  'name': os.path.basename(path),
                  'width': meta['width'], 'height': meta['height'],
                  'duration': round(meta['duration'])})
        results.append(r)
        if r['ok']:
            print(f"  RESULT_PART_{idx}=OK method=sendVideo message_id={r['message_id']} "
                  f"in {r['seconds']}s = {r['mbps']} Mbps", flush=True)
        else:
            print(f"  RESULT_PART_{idx}=FAIL {r.get('error')}", flush=True)
            break
        if idx < total:
            time.sleep(GAP_BETWEEN_PARTS)
    wall = time.time() - t_all0
    ok_all = all(r['ok'] for r in results) and len(results) == len(parts)
    summary = {'file': os.path.basename(a.file), 'method': 'sendVideo',
               'total_bytes': total_size,
               'rejoin': 'ffmpeg -f concat -safe 0 -i <(for f in *.vpart*.mp4; '
                         'do echo "file \'$f\'"; done) -c copy movie.mp4',
               'parts': results, 'all_ok': ok_all,
               'wall_seconds': round(wall, 1),
               'wall_minutes': round(wall / 60, 2)}
    with open(os.path.join(a.out, 'upload-report.json'), 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"RESULT_UPLOAD_ALL={'OK' if ok_all else 'FAIL'} "
          f"parts={len(results)}/{len(parts)} in {wall/60:.2f} min", flush=True)
    return 0 if ok_all else 1


if __name__ == '__main__':
    sys.exit(main())
```

## `scripts/nm3u8_progress.py` ‏(164 سطر)

```python
#!/usr/bin/env python3
"""
nm3u8_progress.py — شريط تقدم *حقيقي* لتنزيل N_m3u8DL-RE، من مصدرين حقيقيين:

1) النسبة %: من مخرجات الأداة نفسها (nm3u8dlre.log) — الأداة تطبع مراحلها
   الحقيقية حتى مع إعادة التوجيه (تُصفّر ألوان ANSI فقط ولا تنهار على Linux؛
   الانهيار الموثق في Issue #746 خاص بـ macOS، ولم يحدث في أي تشغيل Ubuntu).
2) البايتات/السرعة: من مجلد --tmp-dir الخاص بالأداة (du حقيقي)، لا من عدّاد
   الشبكة العام (/proc/net/dev) الذي يخلط كل حركة الـ Runner.
3) الـ ETA: من معدل تقدم % الحقيقي (المتبقي ÷ معدل النسبة/ثانية) — لا تقديرات حجم.
4) CPU/RAM: متوسطات حقيقية من /proc.

الصيغة:
Downloading (N_m3u8DL-RE: 45%)...
[■■■■□□□□□□□] 45%
Downloaded: 1311 MB
ETA: 00:02:10
Speed: 24 MB/s
Average CPU Usage: 8.1%
Average RAM Usage: 1190 MB

- ■ = 10% منجزة (من نسبة الأداة نفسها)، □ = 10% متبقية.
- طباعة كل 5s + كتابة progress JSON كل 1s + طبعة أخيرة عند SIGTERM.
- لا يقدّر الإجمالي أبداً: يعرض المنزَّل الحقيقي والنسبة الحقيقية فقط.

الاستخدام (خلفية):
  python3 scripts/nm3u8_progress.py --log nm3u8dlre.log --tmpdir ./dl-tmp \
      --out ./dl-progress.json & echo $! > /tmp/nmprog.pid
  ... N_m3u8DL-RE --tmp-dir ./dl-tmp ...
  kill -TERM $(cat /tmp/nmprog.pid)
"""
import argparse
import json
import os
import re
import signal
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))
from tg_upload import bar, fmt_eta, read_cpu, cpu_pct_since, mem_used_mb  # noqa

PCT_RE = re.compile(r'(\d{1,3})%')
SEG_RE = re.compile(r'(\d+)\s+Segments')
RATE_RE = re.compile(r'(\d+)\s+Kbps')


def du_bytes(path):
    total = 0
    try:
        for root, _, files in os.walk(path):
            for f in files:
                try:
                    total += os.path.getsize(os.path.join(root, f))
                except OSError:
                    pass
    except OSError:
        pass
    return total


def parse_log(path):
    """أقصى نسبة وأي بيانات وصفية من لوج الأداة (متسامح مع الملف النامي)."""
    pct, segs, kbps, done = 0, 0, 0, False
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                for v in PCT_RE.findall(line):
                    pct = max(pct, min(100, int(v)))
                m = SEG_RE.search(line)
                if m:
                    segs = max(segs, int(m.group(1)))
                m = RATE_RE.search(line)
                if m:
                    kbps = max(kbps, int(m.group(1)))
                if 'Done' in line and ('INFO' in line or ': Done' in line):
                    done = True
    except FileNotFoundError:
        pass
    return pct, segs, kbps, done


def main():
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument('--log', default='nm3u8dlre.log')
    ap.add_argument('--tmpdir', default='./dl-tmp')
    ap.add_argument('--out', default='./dl-progress.json')
    ap.add_argument('--title', default='Downloading (N_m3u8DL-RE)...')
    ap.add_argument('--poll', type=float, default=1.0)
    ap.add_argument('--print-every', type=float, default=5.0)
    a = ap.parse_args()

    stop = {'flag': False}
    signal.signal(signal.SIGTERM, lambda s, f: stop.update(flag=True))
    signal.signal(signal.SIGINT, lambda s, f: stop.update(flag=True))

    cpu_prev = read_cpu()
    cpu_sum = cpu_n = 0
    mem_sum = mem_n = 0
    max_bytes = 0
    last_speed = 0.0
    hist = []  # (t, pct) لحساب معدل التقدم
    last_print = 0.0
    prev_bytes, prev_t = 0, time.time()
    state = {}

    def snapshot(final=False):
        nonlocal cpu_prev, cpu_sum, cpu_n, mem_sum, mem_n
        nonlocal max_bytes, last_speed, prev_bytes, prev_t, last_print
        now = time.time()
        pct, segs, kbps, done = parse_log(a.log)
        b = du_bytes(a.tmpdir)
        max_bytes = max(max_bytes, b)  # الدمج الجزئي قد يحذف ملفات مؤقتاً
        dt = now - prev_t or 1e-6
        inst = max(0.0, (b - prev_bytes) / dt / 1024 / 1024)
        if inst > 0:
            last_speed = inst
        prev_bytes, prev_t = b, now
        p, cpu_prev = cpu_pct_since(cpu_prev)
        cpu_sum += p
        cpu_n += 1
        mem_sum += mem_used_mb()
        mem_n += 1
        hist.append((now, pct))
        hist[:] = [(t, v) for (t, v) in hist if now - t <= 60]
        rate = 0.0
        if len(hist) >= 2 and hist[-1][0] > hist[0][0]:
            rate = (hist[-1][1] - hist[0][1]) / (hist[-1][0] - hist[0][0])
        eta = (100 - pct) / rate if rate > 0.01 else None
        state.update({
            'pct_tool': pct, 'segments_total': segs, 'bitrate_kbps': kbps,
            'downloaded_mb': round(max_bytes / 1024 / 1024, 1),
            'speed_mbps_now': round(last_speed * 8, 1),
            'speed_mbs': round(last_speed, 1),
            'eta': fmt_eta(eta),
            'cpu_avg': round(cpu_sum / cpu_n, 1),
            'ram_avg_mb': round(mem_sum / mem_n),
            'tool_done': done, 'ts': round(now, 1),
        })
        with open(a.out, 'w') as f:
            json.dump(state, f)
        if final or now - last_print >= a.print_every:
            last_print = now
            tag = ' (final)' if final else ''
            print(f"{a.title}{tag}\n[{bar(pct)}] {pct:.0f}%\n"
                  f"Downloaded: {max_bytes / 1024 / 1024:.0f} MB\n"
                  f"ETA: {fmt_eta(eta)}\nSpeed: {last_speed:.0f} MB/s\n"
                  f"Average CPU Usage: {cpu_sum / cpu_n:.1f}%\n"
                  f"Average RAM Usage: {mem_sum / mem_n:.0f} MB", flush=True)

    while not stop['flag']:
        time.sleep(a.poll)
        if not stop['flag']:
            snapshot()
    snapshot(final=True)


if __name__ == '__main__':
    main()
```

## `scripts/monitor.py` ‏(155 سطر)

```python
#!/usr/bin/env python3
"""
monitor.py — مراقبة موارد الـ Runner أثناء مرحلة (تحميل / تحويل) ثم تلخيصها.

يعمل بدون أي مكتبات خارجية (stdlib فقط) عبر /proc (Linux).
يقيس كل ثانية: CPU% للنظام، ذاكرة مستخدمة، وسرعة الشبكة (rx/tx Mbps).

الاستخدام داخل GitHub Actions:
  python3 scripts/monitor.py --out /tmp/mon_dl.csv & echo $! > /tmp/mon.pid
  ... (مرحلة التحميل) ...
  kill $(cat /tmp/mon.pid)
  python3 scripts/monitor.py --summarize /tmp/mon_dl.csv --label DOWNLOAD
"""
import argparse
import csv
import json
import sys
import time


def read_stat():
    with open('/proc/stat') as f:
        p = f.readline().split()
    vals = list(map(int, p[1:8]))  # user nice system idle iowait irq softirq
    total = sum(vals)
    idle = vals[3] + vals[4]
    return total, idle


def read_mem():
    info = {}
    with open('/proc/meminfo') as f:
        for line in f:
            k, v = line.split(':', 1)
            info[k.strip()] = int(v.strip().split()[0])  # kB
    total_mb = info['MemTotal'] / 1024
    used_mb = (info['MemTotal'] - info['MemAvailable']) / 1024
    return used_mb, total_mb


def pick_iface():
    best, best_rx = 'eth0', -1
    try:
        with open('/proc/net/dev') as f:
            for line in f:
                if ':' not in line:
                    continue
                name, rest = line.split(':', 1)
                name = name.strip()
                if name == 'lo':
                    continue
                rx = int(rest.split()[0])
                if rx > best_rx:
                    best, best_rx = name, rx
    except FileNotFoundError:
        pass
    return best


def read_net(iface):
    with open('/proc/net/dev') as f:
        for line in f:
            if ':' not in line:
                continue
            name, rest = line.split(':', 1)
            if name.strip() == iface:
                nums = rest.split()
                return int(nums[0]), int(nums[8])  # rx_bytes, tx_bytes
    return 0, 0


def sample_loop(out_path, interval):
    iface = pick_iface()
    t_prev, i_prev = read_stat()
    rx_prev, tx_prev = read_net(iface)
    t0 = time.time()
    with open(out_path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['t_s', 'cpu_pct', 'mem_used_mb', 'mem_total_mb',
                    'rx_mbps', 'tx_mbps', 'iface'])
        while True:
            time.sleep(interval)
            now = time.time()
            dt = now - t0
            t_cur, i_cur = read_stat()
            d_total = t_cur - t_prev
            cpu = (1 - (i_cur - i_prev) / d_total) * 100 if d_total else 0
            t_prev, i_prev = t_cur, i_cur
            mem_used, mem_total = read_mem()
            rx, tx = read_net(iface)
            el = interval
            rx_mbps = (rx - rx_prev) * 8 / el / 1e6
            tx_mbps = (tx - tx_prev) * 8 / el / 1e6
            rx_prev, tx_prev = rx, tx
            w.writerow([f"{dt:.1f}", f"{cpu:.1f}", f"{mem_used:.0f}",
                        f"{mem_total:.0f}", f"{rx_mbps:.2f}",
                        f"{tx_mbps:.2f}", iface])
            f.flush()


def summarize(path, label):
    rows = list(csv.DictReader(open(path)))
    if not rows:
        print(f"MON_{label}=no samples")
        return
    cpu = [float(r['cpu_pct']) for r in rows]
    mem = [float(r['mem_used_mb']) for r in rows]
    rx = [float(r['rx_mbps']) for r in rows]
    tx = [float(r['tx_mbps']) for r in rows]
    dur_s = float(rows[-1]['t_s'])
    total_rx_mb = sum(x * 1.0 for x in rx) / 8  # Mbps*s -> MB
    avg = lambda a: sum(a) / len(a)
    out = {
        'label': label,
        'duration_s': round(dur_s, 1),
        'duration_min': round(dur_s / 60, 2),
        'cpu_avg_pct': round(avg(cpu), 1),
        'cpu_max_pct': round(max(cpu), 1),
        'ram_avg_mb': round(avg(mem)),
        'ram_max_mb': round(max(mem)),
        'ram_total_mb': round(float(rows[0]['mem_total_mb'])),
        'net_rx_avg_mbps': round(avg(rx), 1),
        'net_rx_max_mbps': round(max(rx), 1),
        'net_rx_total_mb': round(total_rx_mb, 1),
        'net_tx_avg_mbps': round(avg(tx), 2),
        'samples': len(rows),
        'iface': rows[0]['iface'],
    }
    print(json.dumps(out))
    print(f"MON_{label}_DURATION_MIN={out['duration_min']} "
          f"({dur_s:.0f}s, {len(rows)} samples)")
    print(f"MON_{label}_CPU_AVG={out['cpu_avg_pct']}% "
          f"MAX={out['cpu_max_pct']}%")
    print(f"MON_{label}_RAM_AVG={out['ram_avg_mb']}MB "
          f"MAX={out['ram_max_mb']}MB / {out['ram_total_mb']}MB")
    print(f"MON_{label}_NET_RX_AVG={out['net_rx_avg_mbps']}Mbps "
          f"MAX={out['net_rx_max_mbps']}Mbps TOTAL={out['net_rx_total_mb']}MB")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', default='/tmp/mon.csv')
    ap.add_argument('--interval', type=float, default=1.0)
    ap.add_argument('--summarize', default=None)
    ap.add_argument('--label', default='PHASE')
    a = ap.parse_args()
    if a.summarize:
        summarize(a.summarize, a.label)
    else:
        sample_loop(a.out, a.interval)


if __name__ == '__main__':
    sys.exit(main())
```

## `.github/workflows/faselhd-farm.yml` ‏(435 سطر)

```yaml
name: FaselHD Farm (main + 4 workers)

# مزرعة 5 Runners في job واحدة (5 ساعات/دورة):
#  main: بوت @Videoplayeronlinebot + Redis + API + Quick Tunnel (رابطه يُكتب في رسالة 13)
#  w1..w4: عمال، كل عامل ببوته وتوكنه وسيرفره، يأخذ المهام من الـ API
# الأسرار: MAIN_TOKEN=TELEGRAM_BOT_TOKEN, TG_W1..W4_TOKEN, API_ID/HASH, CHAT_ID
# عند نهاية الدورة: dump.rdb يُحفظ (artifact + رسالة 12) وتُشغَّل دورة جديدة ذاتياً.

on:
  workflow_dispatch:
    inputs:
      runtime_minutes:
        description: 'Main bot runtime in minutes (debug: use 25)'
        required: false
        default: '290'
        type: string
      auto_cycle:
        description: 'Self-retrigger next cycle at end'
        required: false
        default: true
        type: boolean

permissions:
  contents: read
  actions: write

# منع دورتين متوازيتين أبداً (مثيل واحد للبوت الرئيسي)
concurrency:
  group: faselhd-farm-singleton
  cancel-in-progress: false

env:
  CHANNEL_ID: '-1003864899881'

jobs:
  main:
    name: Main bot + API (Runner 01)
    runs-on: ubuntu-latest
    timeout-minutes: 300
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '22'

      - name: Install redis-server
        run: |
          sudo apt-get update -qq && sudo apt-get install -y -qq redis-server curl jq
          redis-server --version

      - name: Restore redis dump (previous cycle)
        continue-on-error: true
        run: |
          mkdir -p /tmp/redisdata
          RID=$(gh api repos/${{ github.repository }}/actions/workflows/faselhd-farm.yml/runs \
            --jq '[.workflow_runs[] | select(.conclusion=="success")][0].id' -R ${{ github.repository }} 2>/dev/null || true)
          if [ -n "$RID" ] && [ "$RID" != "null" ]; then
            gh run download "$RID" -n redis-dump -D /tmp/redisdata -R ${{ github.repository }} 2>&1 | tail -1 || true
          else echo "no previous successful cycle (fresh Redis)"; fi
          ls -la /tmp/redisdata 2>/dev/null || true

      - name: Start redis
        run: |
          redis-server --port 6379 --dir /tmp/redisdata --dbfilename dump.rdb --daemonize yes --save '900 1'
          sleep 2
          redis-cli ping

      - name: Restore cloudflared cache
        id: cf-cache
        uses: actions/cache@v4
        with:
          path: ~/tools-cf
          key: Linux-cloudflared-v1

      - name: Setup cloudflared + Quick Tunnel
        run: |
          mkdir -p ~/tools-cf
          [ -x ~/tools-cf/cloudflared ] || curl -sL -o ~/tools-cf/cloudflared \
            https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64
          chmod +x ~/tools-cf/cloudflared
          nohup ~/tools-cf/cloudflared tunnel --no-autoupdate --url http://127.0.0.1:8000 > tunnel.log 2>&1 &
          URL=""
          for i in $(seq 1 30); do
            URL=$(grep -oP 'https://[a-z0-9-]+\.trycloudflare\.com' tunnel.log | head -1 || true)
            [ -n "$URL" ] && break
            sleep 4
          done
          [ -n "$URL" ] || { echo "tunnel failed"; tail -20 tunnel.log; exit 1; }
          echo "TUNNEL_URL=$URL"
          echo "$URL" > api-url.txt

      - name: Publish api-url artifact (workers rendezvous)
        uses: actions/upload-artifact@v4
        with:
          name: api-url
          path: api-url.txt
          retention-days: 1

      - name: Run main bot
        timeout-minutes: 295
        run: |
          SECS=$((${{ inputs.runtime_minutes }} * 60))
          REPO_DIR="$PWD" API_PUBLIC_URL="$(cat api-url.txt)" MAIN_TOKEN="${{ secrets.TELEGRAM_BOT_TOKEN }}" \
          CHANNEL_ID="${{ env.CHANNEL_ID }}" PORT=8000 MAX_RUNTIME_S=$SECS \
          timeout ${SECS}s python3 farm/main_bot.py | tee main-bot.log
        env:
          MAIN_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}

      - name: Backup dump.rdb (artifact + message 12)
        if: always()
        run: |
          redis-cli BGSAVE || true; sleep 5
          ls -la /tmp/redisdata/dump.rdb || echo "no dump"
          python3 - <<'PY'
          import json, os, uuid, urllib.parse, urllib.request
          tok = os.environ['MAIN_TOKEN']
          p = '/tmp/redisdata/dump.rdb'
          if os.path.exists(p) and os.path.getsize(p) > 100:
              bound = uuid.uuid4().hex
              def part(name, value):
                  return (f'--{bound}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n').encode()
              body = part('chat_id', os.environ['CHAT']) + part('message_id', '12')
              body += (f'--{bound}\r\nContent-Disposition: form-data; name="media"\r\n\r\n'
                       + json.dumps({"type": "document", "media": "attach://dump"}) + '\r\n').encode()
              body += (f'--{bound}\r\nContent-Disposition: form-data; name="dump"; filename="dump.rdb"\r\n'
                       + 'Content-Type: application/octet-stream\r\n\r\n').encode()
              with open(p, 'rb') as f:
                  payload = body + f.read() + f'\r\n--{bound}--\r\n'.encode()
              req = urllib.request.Request(f'https://api.telegram.org/bot{tok}/editMessageMedia', data=payload,
                                           headers={'Content-Type': f'multipart/form-data; boundary={bound}'})
              try:
                  r = urllib.request.urlopen(req, timeout=300).read().decode()
                  print('MSG12 edit: ' + r[:200])
              except Exception as e:
                  print('MSG12 edit failed: ' + str(e)[:300])
          else:
              print('no dump to backup')
          PY
        env:
          MAIN_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
          CHAT: ${{ env.CHANNEL_ID }}

      - name: Upload redis dump artifact
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: redis-dump
          path: /tmp/redisdata/dump.rdb
          if-no-files-found: warn
          retention-days: 90

  w1:
    name: Worker 01 (Runner 02)
    runs-on: ubuntu-latest
    timeout-minutes: 300
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '22'
      - name: Restore tools cache
        uses: actions/cache@v4
        with:
          path: ~/tools
          key: Linux-tools-nm3u8dlre-ffmpegstatic-v2
          restore-keys: |
            Linux-tools-
      - name: Restore Bot API image cache
        uses: actions/cache@v4
        with:
          path: ~/docker-cache
          key: Linux-docker-tgbotapi-v1
          restore-keys: |
            Linux-docker-tgbotapi-
      - name: Setup tools + image
        run: |
          mkdir -p ~/tools ~/docker-cache
          if [ ! -x "$HOME/tools/N_m3u8DL-RE" ]; then
            ASSET=$(curl -sL https://api.github.com/repos/nilaoda/N_m3u8DL-RE/releases/latest \
              | jq -r '.assets[] | select(.name | test("linux-x64.*\\.tar\\.gz$")) | .browser_download_url' | head -1)
            curl -sL -o /tmp/nm.tar.gz "$ASSET" && tar xzf /tmp/nm.tar.gz -C "$HOME/tools" && chmod +x "$HOME/tools/N_m3u8DL-RE"
          fi
          if [ ! -x "$HOME/tools/ffmpeg" ]; then
            curl -sL -o /tmp/ff.tar.xz "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz"
            tar xJf /tmp/ff.tar.xz -C /tmp && cp /tmp/ffmpeg-*-amd64-static/ffmpeg /tmp/ffmpeg-*-amd64-static/ffprobe "$HOME/tools/" && chmod +x "$HOME/tools/ffmpeg" "$HOME/tools/ffprobe"
          fi
          echo "$HOME/tools" >> "$GITHUB_PATH"
          if docker image inspect aiogram/telegram-bot-api:latest >/dev/null 2>&1; then echo IMG=present;
          elif [ -f "$HOME/docker-cache/tgbotapi.tar" ]; then docker load -i "$HOME/docker-cache/tgbotapi.tar" >/dev/null && echo IMG=cache;
          else docker pull aiogram/telegram-bot-api:latest && docker save -o "$HOME/docker-cache/tgbotapi.tar" aiogram/telegram-bot-api:latest && echo IMG=pulled; fi
      - name: Start worker Bot API server
        run: |
          docker run -d --name tgbotapi -p 8081:8081 \
            -e TELEGRAM_API_ID="${{ secrets.TELEGRAM_API_ID }}" \
            -e TELEGRAM_API_HASH="${{ secrets.TELEGRAM_API_HASH }}" \
            -e TELEGRAM_LOCAL=1 \
            aiogram/telegram-bot-api:latest --local >/dev/null
          for i in $(seq 1 30); do
            curl -s -o /dev/null "http://127.0.0.1:8081/bot${{ secrets.TG_W1_TOKEN }}/getMe" && break
            sleep 2
          done
          echo WORKER_API_READY
      - name: Run worker (up to 290 min)
        timeout-minutes: 295
        run: |
          export PATH="$HOME/tools:$PATH"
          SECS=$((${{ inputs.runtime_minutes }} * 60))
          REPO_DIR="$PWD" WORKER_NAME=w1 WORKER_TOKEN="${{ secrets.TG_W1_TOKEN }}" \
          CHANNEL_ID="${{ env.CHANNEL_ID }}" \
          timeout ${SECS}s python3 farm/worker.py | tee worker-w1.log
        env:
          GH_TOKEN: ${{ github.token }}
      - name: Cleanup
        if: always()
        run: |
          docker stop tgbotapi 2>/dev/null || true; docker rm tgbotapi 2>/dev/null || true
          rm -rf /tmp/farm_* /tmp/apiurl

  w2:
    name: Worker 02 (Runner 03)
    runs-on: ubuntu-latest
    timeout-minutes: 300
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '22'
      - name: Restore tools cache
        uses: actions/cache@v4
        with:
          path: ~/tools
          key: Linux-tools-nm3u8dlre-ffmpegstatic-v2
          restore-keys: |
            Linux-tools-
      - name: Restore Bot API image cache
        uses: actions/cache@v4
        with:
          path: ~/docker-cache
          key: Linux-docker-tgbotapi-v1
          restore-keys: |
            Linux-docker-tgbotapi-
      - name: Setup tools + image
        run: |
          mkdir -p ~/tools ~/docker-cache
          if [ ! -x "$HOME/tools/N_m3u8DL-RE" ]; then
            ASSET=$(curl -sL https://api.github.com/repos/nilaoda/N_m3u8DL-RE/releases/latest \
              | jq -r '.assets[] | select(.name | test("linux-x64.*\\.tar\\.gz$")) | .browser_download_url' | head -1)
            curl -sL -o /tmp/nm.tar.gz "$ASSET" && tar xzf /tmp/nm.tar.gz -C "$HOME/tools" && chmod +x "$HOME/tools/N_m3u8DL-RE"
          fi
          if [ ! -x "$HOME/tools/ffmpeg" ]; then
            curl -sL -o /tmp/ff.tar.xz "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz"
            tar xJf /tmp/ff.tar.xz -C /tmp && cp /tmp/ffmpeg-*-amd64-static/ffmpeg /tmp/ffmpeg-*-amd64-static/ffprobe "$HOME/tools/" && chmod +x "$HOME/tools/ffmpeg" "$HOME/tools/ffprobe"
          fi
          echo "$HOME/tools" >> "$GITHUB_PATH"
          if docker image inspect aiogram/telegram-bot-api:latest >/dev/null 2>&1; then echo IMG=present;
          elif [ -f "$HOME/docker-cache/tgbotapi.tar" ]; then docker load -i "$HOME/docker-cache/tgbotapi.tar" >/dev/null && echo IMG=cache;
          else docker pull aiogram/telegram-bot-api:latest && docker save -o "$HOME/docker-cache/tgbotapi.tar" aiogram/telegram-bot-api:latest && echo IMG=pulled; fi
      - name: Start worker Bot API server
        run: |
          docker run -d --name tgbotapi -p 8081:8081 \
            -e TELEGRAM_API_ID="${{ secrets.TELEGRAM_API_ID }}" \
            -e TELEGRAM_API_HASH="${{ secrets.TELEGRAM_API_HASH }}" \
            -e TELEGRAM_LOCAL=1 \
            aiogram/telegram-bot-api:latest --local >/dev/null
          for i in $(seq 1 30); do
            curl -s -o /dev/null "http://127.0.0.1:8081/bot${{ secrets.TG_W2_TOKEN }}/getMe" && break
            sleep 2
          done
          echo WORKER_API_READY
      - name: Run worker (up to 290 min)
        timeout-minutes: 295
        run: |
          export PATH="$HOME/tools:$PATH"
          SECS=$((${{ inputs.runtime_minutes }} * 60))
          REPO_DIR="$PWD" WORKER_NAME=w2 WORKER_TOKEN="${{ secrets.TG_W2_TOKEN }}" \
          CHANNEL_ID="${{ env.CHANNEL_ID }}" \
          timeout ${SECS}s python3 farm/worker.py | tee worker-w2.log
        env:
          GH_TOKEN: ${{ github.token }}
      - name: Cleanup
        if: always()
        run: |
          docker stop tgbotapi 2>/dev/null || true; docker rm tgbotapi 2>/dev/null || true
          rm -rf /tmp/farm_* /tmp/apiurl

  w3:
    name: Worker 03 (Runner 04)
    runs-on: ubuntu-latest
    timeout-minutes: 300
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '22'
      - name: Restore tools cache
        uses: actions/cache@v4
        with:
          path: ~/tools
          key: Linux-tools-nm3u8dlre-ffmpegstatic-v2
          restore-keys: |
            Linux-tools-
      - name: Restore Bot API image cache
        uses: actions/cache@v4
        with:
          path: ~/docker-cache
          key: Linux-docker-tgbotapi-v1
          restore-keys: |
            Linux-docker-tgbotapi-
      - name: Setup tools + image
        run: |
          mkdir -p ~/tools ~/docker-cache
          if [ ! -x "$HOME/tools/N_m3u8DL-RE" ]; then
            ASSET=$(curl -sL https://api.github.com/repos/nilaoda/N_m3u8DL-RE/releases/latest \
              | jq -r '.assets[] | select(.name | test("linux-x64.*\\.tar\\.gz$")) | .browser_download_url' | head -1)
            curl -sL -o /tmp/nm.tar.gz "$ASSET" && tar xzf /tmp/nm.tar.gz -C "$HOME/tools" && chmod +x "$HOME/tools/N_m3u8DL-RE"
          fi
          if [ ! -x "$HOME/tools/ffmpeg" ]; then
            curl -sL -o /tmp/ff.tar.xz "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz"
            tar xJf /tmp/ff.tar.xz -C /tmp && cp /tmp/ffmpeg-*-amd64-static/ffmpeg /tmp/ffmpeg-*-amd64-static/ffprobe "$HOME/tools/" && chmod +x "$HOME/tools/ffmpeg" "$HOME/tools/ffprobe"
          fi
          echo "$HOME/tools" >> "$GITHUB_PATH"
          if docker image inspect aiogram/telegram-bot-api:latest >/dev/null 2>&1; then echo IMG=present;
          elif [ -f "$HOME/docker-cache/tgbotapi.tar" ]; then docker load -i "$HOME/docker-cache/tgbotapi.tar" >/dev/null && echo IMG=cache;
          else docker pull aiogram/telegram-bot-api:latest && docker save -o "$HOME/docker-cache/tgbotapi.tar" aiogram/telegram-bot-api:latest && echo IMG=pulled; fi
      - name: Start worker Bot API server
        run: |
          docker run -d --name tgbotapi -p 8081:8081 \
            -e TELEGRAM_API_ID="${{ secrets.TELEGRAM_API_ID }}" \
            -e TELEGRAM_API_HASH="${{ secrets.TELEGRAM_API_HASH }}" \
            -e TELEGRAM_LOCAL=1 \
            aiogram/telegram-bot-api:latest --local >/dev/null
          for i in $(seq 1 30); do
            curl -s -o /dev/null "http://127.0.0.1:8081/bot${{ secrets.TG_W3_TOKEN }}/getMe" && break
            sleep 2
          done
          echo WORKER_API_READY
      - name: Run worker (up to 290 min)
        timeout-minutes: 295
        run: |
          export PATH="$HOME/tools:$PATH"
          SECS=$((${{ inputs.runtime_minutes }} * 60))
          REPO_DIR="$PWD" WORKER_NAME=w3 WORKER_TOKEN="${{ secrets.TG_W3_TOKEN }}" \
          CHANNEL_ID="${{ env.CHANNEL_ID }}" \
          timeout ${SECS}s python3 farm/worker.py | tee worker-w3.log
        env:
          GH_TOKEN: ${{ github.token }}
      - name: Cleanup
        if: always()
        run: |
          docker stop tgbotapi 2>/dev/null || true; docker rm tgbotapi 2>/dev/null || true
          rm -rf /tmp/farm_* /tmp/apiurl

  w4:
    name: Worker 04 (Runner 05)
    runs-on: ubuntu-latest
    timeout-minutes: 300
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '22'
      - name: Restore tools cache
        uses: actions/cache@v4
        with:
          path: ~/tools
          key: Linux-tools-nm3u8dlre-ffmpegstatic-v2
          restore-keys: |
            Linux-tools-
      - name: Restore Bot API image cache
        uses: actions/cache@v4
        with:
          path: ~/docker-cache
          key: Linux-docker-tgbotapi-v1
          restore-keys: |
            Linux-docker-tgbotapi-
      - name: Setup tools + image
        run: |
          mkdir -p ~/tools ~/docker-cache
          if [ ! -x "$HOME/tools/N_m3u8DL-RE" ]; then
            ASSET=$(curl -sL https://api.github.com/repos/nilaoda/N_m3u8DL-RE/releases/latest \
              | jq -r '.assets[] | select(.name | test("linux-x64.*\\.tar\\.gz$")) | .browser_download_url' | head -1)
            curl -sL -o /tmp/nm.tar.gz "$ASSET" && tar xzf /tmp/nm.tar.gz -C "$HOME/tools" && chmod +x "$HOME/tools/N_m3u8DL-RE"
          fi
          if [ ! -x "$HOME/tools/ffmpeg" ]; then
            curl -sL -o /tmp/ff.tar.xz "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz"
            tar xJf /tmp/ff.tar.xz -C /tmp && cp /tmp/ffmpeg-*-amd64-static/ffmpeg /tmp/ffmpeg-*-amd64-static/ffprobe "$HOME/tools/" && chmod +x "$HOME/tools/ffmpeg" "$HOME/tools/ffprobe"
          fi
          echo "$HOME/tools" >> "$GITHUB_PATH"
          if docker image inspect aiogram/telegram-bot-api:latest >/dev/null 2>&1; then echo IMG=present;
          elif [ -f "$HOME/docker-cache/tgbotapi.tar" ]; then docker load -i "$HOME/docker-cache/tgbotapi.tar" >/dev/null && echo IMG=cache;
          else docker pull aiogram/telegram-bot-api:latest && docker save -o "$HOME/docker-cache/tgbotapi.tar" aiogram/telegram-bot-api:latest && echo IMG=pulled; fi
      - name: Start worker Bot API server
        run: |
          docker run -d --name tgbotapi -p 8081:8081 \
            -e TELEGRAM_API_ID="${{ secrets.TELEGRAM_API_ID }}" \
            -e TELEGRAM_API_HASH="${{ secrets.TELEGRAM_API_HASH }}" \
            -e TELEGRAM_LOCAL=1 \
            aiogram/telegram-bot-api:latest --local >/dev/null
          for i in $(seq 1 30); do
            curl -s -o /dev/null "http://127.0.0.1:8081/bot${{ secrets.TG_W4_TOKEN }}/getMe" && break
            sleep 2
          done
          echo WORKER_API_READY
      - name: Run worker (up to 290 min)
        timeout-minutes: 295
        run: |
          export PATH="$HOME/tools:$PATH"
          SECS=$((${{ inputs.runtime_minutes }} * 60))
          REPO_DIR="$PWD" WORKER_NAME=w4 WORKER_TOKEN="${{ secrets.TG_W4_TOKEN }}" \
          CHANNEL_ID="${{ env.CHANNEL_ID }}" \
          timeout ${SECS}s python3 farm/worker.py | tee worker-w4.log
        env:
          GH_TOKEN: ${{ github.token }}
      - name: Cleanup
        if: always()
        run: |
          docker stop tgbotapi 2>/dev/null || true; docker rm tgbotapi 2>/dev/null || true
          rm -rf /tmp/farm_* /tmp/apiurl

  cycle:
    name: Next cycle (self-retrigger)
    runs-on: ubuntu-latest
    needs: [main, w1, w2, w3, w4]
    # لا توليد عند الإلغاء اليدوي (وإلا أصبح الإلغاء مستحيلاً)، ولا عند تعطيل auto_cycle
    if: >-
      always() && inputs.auto_cycle == true &&
      needs.main.result != 'cancelled' && needs.w1.result != 'cancelled' &&
      needs.w2.result != 'cancelled' && needs.w3.result != 'cancelled' &&
      needs.w4.result != 'cancelled'
    steps:
      - name: Trigger next cycle
        run: gh workflow run faselhd-farm.yml --ref main -R ${{ github.repository }}
        env:
          GH_TOKEN: ${{ github.token }}
```
