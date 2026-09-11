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
