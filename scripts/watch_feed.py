#!/usr/bin/env python3
"""Read-only LAN L2 subscription and arrival-age summary; Python standard library only."""
import argparse
import json
import time
import urllib.parse
from mock_e2e import WS

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('url')
p.add_argument('--coin', default='BTC')
p.add_argument('--seconds', type=int, default=60)
a = p.parse_args()
u = urllib.parse.urlparse(a.url)
assert u.scheme == 'ws' and u.path == '/ws', 'expected ws://host:port/ws'
ws = WS(u.port or 8000, u.hostname)
ws.sock.settimeout(max(a.seconds, 10))
ws.send({'method':'subscribe','subscription':{'type':'l2Book','coin':a.coin}})
ages = []
end = time.monotonic() + a.seconds
try:
    while time.monotonic() < end:
        msg = ws.recv()
        if msg['channel'] in ['status','error','subscriptionResponse']: print(json.dumps(msg), flush=True)
        if msg['channel'] == 'l2Book': ages.append(time.time()*1000-msg['data']['time'])
finally:
    ws.close()
if ages:
    ages.sort()
    print(json.dumps({'updates':len(ages),'median_age_ms':ages[len(ages)//2],
                      'p95_age_ms':ages[min(len(ages)-1,int(len(ages)*.95))],'max_age_ms':max(ages)}))
else:
    raise SystemExit('No L2 updates received')
