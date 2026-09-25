#!/usr/bin/env python3
"""Local-only process/HTTP/file/WebSocket regression test. No third-party Python packages.
Usage: python3 scripts/mock_e2e.py [target/release/websocket_server] [--stream]
"""
import base64
import datetime
import http.server
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

class WS:
    def __init__(self, port, host="127.0.0.1"):
        self.sock = socket.create_connection((host, port), timeout=5)
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock.sendall((f'GET /ws HTTP/1.1\r\nHost: {host}:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n').encode())
        self.file = self.sock.makefile('rb')
        assert b'101' in self.file.readline(), 'WebSocket upgrade failed'
        while self.file.readline() != b'\r\n':
            pass
    def send(self, data):
        payload = json.dumps(data).encode()
        mask = os.urandom(4)
        header = bytes([0x81, 0x80 | len(payload)]) if len(payload) < 126 else bytes([0x81, 0xfe]) + struct.pack('!H', len(payload))
        self.sock.sendall(header + mask + bytes(v ^ mask[i % 4] for i, v in enumerate(payload)))
    def recv(self):
        head = self.file.read(2)
        assert len(head) == 2, 'WebSocket disconnected'
        size = head[1] & 127
        if size == 126: size = struct.unpack('!H', self.file.read(2))[0]
        if size == 127: size = struct.unpack('!Q', self.file.read(8))[0]
        payload = self.file.read(size)
        assert head[0] & 15 != 8, 'WebSocket close frame'
        return json.loads(payload)
    def until(self, channel, timeout=10, predicate=lambda m: True):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            msg = self.recv()
            if msg['channel'] == channel and predicate(msg): return msg
        raise AssertionError(f'timed out waiting for {channel}')
    def close(self):
        self.file.close()
        self.sock.close()


def main():
    binary = Path(sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith('--') else 'target/release/websocket_server').resolve()
    streamed = '--stream' in sys.argv
    lock = threading.Lock()
    state = dict(height=100, snapshots=0, pause=False, skip=False, stop=False, malformed=False, rotate=False, fail_snapshot=False, old=False)
    with tempfile.TemporaryDirectory(prefix='ws-e2e-') as temp:
        root = Path(temp)
        paths = []
        for name in ['node_order_statuses_by_block', 'node_raw_book_diffs_by_block', 'node_fills_by_block']:
            path = root / name / 'hourly' / '20260101' / '0'
            path.parent.mkdir(parents=True)
            path.touch()
            paths.append(path)
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                if body.get('type') != 'fileSnapshot':
                    # Delay one read-only query to verify L2 keeps flowing while HTTP is pending.
                    if body.get('user') == 'slow': time.sleep(.4)
                    if body.get('user') == 'timeout': time.sleep(2.5)
                    if body.get('user') == 'fail':
                        self.send_response(503); self.end_headers(); self.wfile.write(b'temporary failure'); return
                    if body.get('user') == 'oversized':
                        self.send_response(200); self.send_header('Content-Length',str(2097153)); self.end_headers(); return
                    if body.get('user') == 'badjson':
                        self.send_response(200); self.end_headers(); self.wfile.write(b'{'); return
                    self.send_response(200); self.end_headers()
                    data = [] if body.get('type') == 'openOrders' else {'time':123}
                    try: self.wfile.write(json.dumps(data).encode())
                    except BrokenPipeError: pass
                    return
                with lock:
                    state['snapshots'] += 1
                    height = state['height']
                    fail = state['fail_snapshot']
                if fail:
                    self.send_response(503); self.end_headers(); return
                Path(body['outPath']).write_text(json.dumps([height, [['BTC', [[], []]]]]))
                self.send_response(200); self.end_headers(); self.wfile.write(b'null')
            def log_message(self, *_): pass
        mock_http = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=mock_http.serve_forever, daemon=True).start()
        def producer():
            while True:
                time.sleep(0.04)
                with lock:
                    if state['stop']: return
                    if state['pause']: continue
                    if state['rotate']:
                        for i, path in enumerate(paths):
                            paths[i] = path.with_name('1'); paths[i].touch()
                        state['rotate'] = False
                    state['height'] += 2 if state['skip'] else 1
                    state['skip'] = False
                    now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat()
                    block_time = ((datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=10)).replace(tzinfo=None).isoformat() if state['old'] else now)
                    batch = dict(local_time=now, block_time=block_time, block_number=state['height'], events=[])
                    for i, path in enumerate(paths):
                        with path.open('a') as f:
                            if state['malformed'] and i == 2:
                                f.write('{bad fill json}\n')
                                state['malformed'] = False
                            else:
                                # Two fragments at every height exercise streamed accumulation.
                                for _ in range(2 if streamed else 1): f.write(json.dumps(batch) + '\n')
        threading.Thread(target=producer, daemon=True).start()
        with socket.socket() as reserve:
            reserve.bind(('127.0.0.1', 0)); port = reserve.getsockname()[1]
        log_path = root / 'server.log'
        with log_path.open('w') as log:
            cmd = [str(binary), '--address', '127.0.0.1', '--port', str(port), '--node-data-dir', str(root),
                   '--snapshot-path', str(root / 'snapshot.json'), '--info-url', f'http://127.0.0.1:{mock_http.server_port}/info',
                   '--retry-interval-secs', '1', '--stale-after-secs', '2', '--websocket-compression-level', '0']
            if streamed: cmd.append('--stream-with-block-info')
            process = subprocess.Popen(cmd, stdout=log, stderr=log, env={**os.environ, 'RUST_LOG': 'info'})
            ws = None
            try:
                def health():
                    try:
                        return json.load(urllib.request.urlopen(f'http://127.0.0.1:{port}/health', timeout=2))
                    except urllib.error.HTTPError as err: return json.load(err)
                def wait_ready():
                    end = time.monotonic() + 10
                    while time.monotonic() < end:
                        try:
                            if health()['state'] == 'Ready': return
                        except OSError: pass
                        time.sleep(.05)
                    raise AssertionError('feed did not become ready')
                wait_ready()
                ws = WS(port)
                for kind in ['l2Book', 'l4Book', 'trades']:
                    ws.send(dict(method='subscribe', subscription=dict(type=kind, coin='BTC')))
                    ws.until('subscriptionResponse')
                ws.until('l2Book')
                with lock: first_count = state['snapshots']; state['malformed'] = True
                # Longer than the old 10s loop. Continuously drain the same TCP connection.
                end = time.monotonic() + 11
                while time.monotonic() < end: ws.until('l2Book')
                with lock: assert state['snapshots'] == first_count, 'healthy periodic snapshot regression'
                # Real gap forces one snapshot. Its old but contiguous replay must
                # remain gated and catch up without discarding/re-requesting it.
                with lock: state['skip'] = True; state['old'] = True
                ws.until('status', predicate=lambda m: m['data']['state'] != 'Ready')
                ws.until('status', predicate=lambda m: m['data']['state'] == 'Stale' and 'catching up' in m['data']['reason'])
                assert health()['state'] != 'Ready'
                with lock: recovery_snapshots = state['snapshots']
                time.sleep(.3)
                assert health()['state'] != 'Ready'
                ws.send({'method':'post','id':98,'request':{'type':'info','payload':{'type':'exchangeStatus'}}})
                while True:
                    message = ws.recv()
                    assert message['channel'] not in ('l2Book', 'l4Book'), 'stale replay published a book'
                    if message['channel'] == 'post' and message['data']['id'] == 98: break
                with lock:
                    assert state['snapshots'] == recovery_snapshots
                    state['old'] = False
                ws.until('status', predicate=lambda m: m['data']['state'] == 'Ready')
                with lock: assert state['snapshots'] == recovery_snapshots
                ws.until('l4Book', predicate=lambda m: 'Snapshot' in m['data'])
                ws.until('l2Book')
                # Hour rotation must read the new file's initial data without resnapshotting.
                with lock: count_before_rotation = state['snapshots']; state['rotate'] = True
                end = time.monotonic() + 2
                while time.monotonic() < end: ws.until('l2Book')
                with lock: assert state['snapshots'] == count_before_rotation, 'rotation caused recovery'
                with lock: state['pause'] = True; state['fail_snapshot'] = True
                ws.until('status', predicate=lambda m: m['data']['state'] == 'Stale')
                assert health()['state'] != 'Ready'
                ws.send({'method':'post','id':99,'request':{'type':'info','payload':{'type':'l2Book','coin':'BTC'}}})
                stale_reply=ws.until('post')['data']
                assert stale_reply['id']==99 and stale_reply['response']['type']=='error'
                assert '503' in stale_reply['response']['payload']
                # Allow a failed snapshot request, then restore the same mock upstream.
                time.sleep(1.2)
                with lock: state['pause'] = False; state['fail_snapshot'] = False
                ws.until('status', predicate=lambda m: m['data']['state'] == 'Ready')
                ws.until('l2Book')
                assert process.poll() is None
                def endpoint(path):
                    return json.load(urllib.request.urlopen(f'http://127.0.0.1:{port}/{path}',timeout=2))
                assert endpoint('version')['implementation'] == 'hyperliquid-order-book-server/low-latency-ws'
                capabilities = endpoint('capabilities')
                assert capabilities['subscriptions'] == ['l2Book','trades','l4Book','userFills','orderUpdates','openOrders','allDexsClearinghouseState','spotState']
                assert capabilities['wallet_subscriptions'] is True
                diagnostic = endpoint('diagnostics')
                for metric in ['book_apply_us','l2_aggregate_us','ws_dispatch_queue_us','ws_serialize_us','ws_socket_send_us','orders_node_local_to_read_us']:
                    assert diagnostic['metrics'][metric]['count'] > 0, metric
                assert 'retained_input_bytes' in diagnostic['backlog']
                for kind in ['orderUpdates','userFills','openOrders','allDexsClearinghouseState','spotState']:
                    ws.send({'method':'subscribe','subscription':{'type':kind,'user':'0x'+'0'*40}})
                    assert '--wallets' in ws.until('error')['data']
                ws.send({'method':'post','id':1,'request':{'type':'info','payload':{'type':'exchangeStatus'}}})
                post = ws.until('post')['data']
                assert post == {'id':1,'response':{'type':'info','payload':{'type':'exchangeStatus','data':{'time':123}}}}
                assert capabilities['websocket_info_post'] is True
                ws.send({'method':'post','id':2,'request':{'type':'info','payload':{'type':'openOrders','user':'slow'}}})
                books_while_waiting = 0
                while True:
                    msg=ws.recv()
                    if msg['channel']=='l2Book': books_while_waiting+=1
                    if msg['channel']=='post':
                        assert msg['data']['id']==2 and msg['data']['response']['payload']['data']==[]
                        break
                assert books_while_waiting >= 2, 'HTTP Info blocked book delivery'
                for request_id,user in [(3,'fail'),(4,'timeout')]:
                    ws.send({'method':'post','id':request_id,'request':{'type':'info','payload':{'type':'openOrders','user':user}}})
                    reply=ws.until('post')['data']
                    assert reply['id']==request_id and reply['response']['type']=='error'
                    assert ('503' if user=='fail' else '504') in reply['response']['payload']
                    ws.until('l2Book')
                with lock: snapshots_before_post = state['snapshots']
                ws.send({'method':'post','id':5,'request':{'type':'info','payload':{'type':'fileSnapshot','outPath':'unused'}}})
                assert ws.until('post')['data']['response']['type']=='error'
                with lock: assert state['snapshots']==snapshots_before_post, 'Info post allowed full snapshot request'
                ws.until('l2Book')
                ws.send({'method':'post','id':6,'request':{'type':'info','payload':{'type':'l2Book','coin':'BTC','nLevels':5}}})
                reply=ws.until('post')['data']
                assert reply['id']==6 and reply['response']['payload']['data']['coin']=='BTC'
                for request_id,user in [(7,'oversized'),(8,'badjson')]:
                    ws.send({'method':'post','id':request_id,'request':{'type':'info','payload':{'type':'openOrders','user':user}}})
                    reply=ws.until('post')['data']
                    assert reply['id']==request_id and reply['response']['type']=='error'
                    ws.until('l2Book')
                for request_id in range(10,15):
                    ws.send({'method':'post','id':request_id,'request':{'type':'info','payload':{'type':'openOrders','user':'slow'}}})
                replies=[ws.until('post')['data'] for _ in range(5)]
                assert {r['id'] for r in replies}==set(range(10,15))
                assert any(r['response']['type']=='error' and '429' in r['response']['payload'] for r in replies)
                assert endpoint('diagnostics')['l2_demand'] == {'markets':1,'variants':1}
                second = WS(port)
                try:
                    second.send({'method':'subscribe','subscription':{'type':'l2Book','coin':'BTC'}})
                    second.until('subscriptionResponse'); second.until('l2Book')
                    rounded = {'type':'l2Book','coin':'BTC','nSigFigs':5,'mantissa':2,'nLevels':5}
                    ws.send({'method':'subscribe','subscription':rounded})
                    ws.until('subscriptionResponse'); ws.until('l2Book')
                    def wait_variants(n):
                        end = time.monotonic()+3
                        while time.monotonic()<end:
                            if endpoint('diagnostics')['l2_demand']['variants']==n:return
                            time.sleep(.02)
                        raise AssertionError('subscription demand did not update')
                    wait_variants(2)
                    ws.send({'method':'unsubscribe','subscription':{'type':'l2Book','coin':'BTC'}})
                    ws.until('subscriptionResponse')
                    ws.send({'method':'unsubscribe','subscription':rounded})
                    ws.until('subscriptionResponse')
                    wait_variants(1)
                    second.until('l2Book') # first client's unsubscribe must preserve second client's feed
                finally: second.close()
                wait_variants(0)
                ws.send({'method':'subscribe','subscription':{'type':'l2Book','coin':'BTC','nLevels':5}})
                ws.until('subscriptionResponse'); ws.until('l2Book')
                wait_variants(1)
                print(f'PASS ({"stream" if streamed else "batch"}): healthy >10s without snapshots; malformed fills; missed block; L4 reset; rotation; stale stream; HTTP failure; same WebSocket/process recovery')
            except Exception:
                print(log_path.read_text(), file=sys.stderr)
                raise
            finally:
                with lock: state['stop'] = True
                if ws: ws.close()
                process.terminate()
                process.wait(timeout=5)
                mock_http.shutdown()

if __name__ == '__main__': main()
