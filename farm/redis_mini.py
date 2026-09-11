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
