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
