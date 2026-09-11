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
