"""Bounded HTTP diagnostics for acceptance; never persist credentials."""
from datetime import datetime
import json
import threading
import time
import urllib.error
import urllib.request
import uuid
from zoneinfo import ZoneInfo


def request(url, token, t0, body=None, submitted=None):
    started=time.monotonic(); trace=uuid.uuid4().hex
    result=dict(request_id=trace,url=url,started_seconds=started-t0,
        wall_started_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        queue_seconds=0 if submitted is None else started-submitted)
    req=urllib.request.Request(url,data=None if body is None else json.dumps(body).encode(),
        headers={'Authorization':'Bearer '+token,'Content-Type':'application/json','X-Vecdb-Probe-ID':trace})
    result['prepare_seconds']=time.monotonic()-started
    try:
        with urllib.request.urlopen(req,timeout=3) as response:
            opened=time.monotonic()
            raw=response.read(); read=time.monotonic()
            result['value']=json.loads(raw)
            result.update(open_seconds=opened-started-result['prepare_seconds'],read_seconds=read-opened,json_seconds=time.monotonic()-read)
            result['http_status']=getattr(response,'status',200)
            result['server_request_id']=response.headers.get('X-Vecdb-Probe-ID') if hasattr(response,'headers') else None
    except urllib.error.HTTPError as exc:
        try: raw=exc.read(8193)
        finally: exc.close()
        result.update(error='HTTPError',http_status=exc.code,response_body=raw[:8192].decode('utf-8',errors='replace'),body_truncated=len(raw)>8192,
            server_request_id=exc.headers.get('X-Vecdb-Probe-ID') if exc.headers else None)
        try: result['response_json']=json.loads(result['response_body'])
        except ValueError: pass
    except (OSError,ValueError) as exc:
        result.update(error=type(exc).__name__,error_message=str(exc))
    result.update(elapsed=time.monotonic()-t0,duration=time.monotonic()-started)
    return result


class RequestJournal:
    """One bounded buffered handle; the caller flushes before stopping its clock.

    Request workers never reopen the file or queue unbounded in-memory records.
    Encoding stays outside the short write lock. IO errors propagate to fail the
    acceptance run, including flush/close errors on the completion path.
    """
    def __init__(self, path):
        self.path=path
        self.lock=threading.Lock()
        self.stats=dict(records=0,encoded_bytes=0,encode_seconds=0,lock_wait_seconds=0,
            max_lock_wait_seconds=0,write_seconds=0,flush_seconds=0)

    def __enter__(self):
        started=time.monotonic()
        self.stream=self.path.open('a',buffering=64*1024)
        self.stats['open_seconds']=time.monotonic()-started
        return self

    def __exit__(self, *exc):
        self.stream.close()

    def record(self, result):
        started=time.monotonic()
        raw=json.dumps({k:v for k,v in result.items() if k!='value'})+'\n'
        encoded=time.monotonic()
        with self.lock:
            acquired=time.monotonic()
            self.stream.write(raw)
            written=time.monotonic()
            self.stats['records']+=1
            self.stats['encoded_bytes']+=len(raw)
            self.stats['encode_seconds']+=encoded-started
            self.stats['lock_wait_seconds']+=acquired-encoded
            self.stats['max_lock_wait_seconds']=max(self.stats['max_lock_wait_seconds'],acquired-encoded)
            self.stats['write_seconds']+=written-acquired
        return dict(record_seconds=time.monotonic()-started,record_lock_wait_seconds=acquired-encoded,
            record_encode_seconds=encoded-started,record_write_seconds=written-acquired)

    def flush(self):
        started=time.monotonic()
        with self.lock:
            self.stream.flush()
            self.stats['flush_seconds']+=time.monotonic()-started

    def summary(self):
        with self.lock:return dict(self.stats)


def classify_error(result, *, retired=False, gateway=False):
    if result.get('http_status') in (400,401,403,405): return 'probe_protocol_error'
    if retired and result.get('http_status')==503 and result.get('response_json',{}).get('code')=='fenced':
        return 'retired_member_fenced'
    return 'gateway_error' if gateway else 'unexplained_or_current_member_error'
