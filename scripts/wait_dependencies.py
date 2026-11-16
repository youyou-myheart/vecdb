"""Wait for the Linux CI storage stack, with a finite deadline."""
import socket
import time
import urllib.request

deadline = time.monotonic()+120
last = None
while time.monotonic() < deadline:
    try:
        for port in (12379, 12160, 18333):
            with socket.create_connection(('127.0.0.1', port), timeout=2):
                pass
        with urllib.request.urlopen('http://127.0.0.1:12379/health', timeout=3) as response:
            assert response.status == 200
        print('PD, TiKV and SeaweedFS listeners ready')
        break
    except (OSError, AssertionError) as exc:
        last = exc
        time.sleep(1)
else:
    raise TimeoutError(f'storage dependencies not ready: {last}')
