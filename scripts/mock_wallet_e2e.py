#!/usr/bin/env python3
"""Local-only wallet integration: real WS process + fake node HTTP/JSONL.
Run python3 scripts/mock_wallet_e2e.py [--stream]. No node/public API access.
"""
import datetime
import http.server
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from mock_e2e import WS

USER = '0x0000000000000000000000000000000000000001'
OTHER = '0x0000000000000000000000000000000000000002'

def main():
    streamed = '--stream' in sys.argv
    binary = Path('target/release/websocket_server').resolve()
    state = dict(height=100, stop=False, pause=False, fills=[], orders=[], open=[], fail=False, slow=False, skip=False, queries=0)
    lock = threading.Lock()
    with tempfile.TemporaryDirectory(prefix='wallet-e2e-') as temp:
        root = Path(temp)
        paths = []
        for name in ['node_order_statuses_by_block', 'node_raw_book_diffs_by_block', 'node_fills_by_block']:
            p = root / name / 'hourly' / '20260101' / '0'
            p.parent.mkdir(parents=True); p.touch(); paths.append(p)
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                with lock:
                    h, orders, fail, slow = state['height'], list(state['open']), state['fail'], state['slow']
                if body['type'] == 'fileSnapshot':
                    Path(body['outPath']).write_text(json.dumps([h, [['BTC', [[], []]]]]))
                    data = None
                else:
                    assert body['type'] == 'frontendOpenOrders', body
                    assert body['user'] == USER and body['dex'] == 'xyz', body
                    with lock: state['queries'] += 1
                    if slow: time.sleep(.6)
                    if fail:
                        self.send_response(503); self.end_headers(); return
                    data = orders
                self.send_response(200); self.end_headers()
                try: self.wfile.write(json.dumps(data).encode())
                except BrokenPipeError: pass
            def log_message(self, *_): pass
        httpd = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        def producer():
            while True:
                time.sleep(.04)
                with lock:
                    if state['stop']: return
                    if state['pause']: continue
                    state['height'] += 2 if state['skip'] else 1
                    state['skip'] = False
                    now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat()
                    for i, p in enumerate(paths):
                        events = state['orders'] if i == 0 else state['fills'] if i == 2 else []
                        batch = dict(local_time=now, block_time=now, block_number=state['height'], events=events)
                        with p.open('a') as f:
                            # Multiple same-height fragments in stream mode; identical timestamps.
                            if streamed and events:
                                for e in events: f.write(json.dumps(dict(batch, events=[e])) + '\n')
                            else: f.write(json.dumps(batch) + '\n')
                    state['orders'] = []; state['fills'] = []
        threading.Thread(target=producer, daemon=True).start()
        import socket
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
        command = [str(binary), '--address', '127.0.0.1', '--port', str(port), '--node-data-dir', str(root),
            '--info-url', f'http://127.0.0.1:{httpd.server_port}/info', '--markets', 'BTC', '--wallets', USER+','+OTHER,
            '--wallet-poll-interval-ms', '250', '--stale-after-secs', '1', '--retry-interval-secs', '1',
            '--websocket-compression-level', '0']
        if streamed: command.append('--stream-with-block-info')
        log = (root/'server.log').open('w')
        proc = None; clients = []
        def start():
            p = subprocess.Popen(command, stdout=log, stderr=log, env=dict(os.environ, RUST_LOG='warn', RAYON_NUM_THREADS='2', TOKIO_WORKER_THREADS='2'))
            for _ in range(150):
                if p.poll() is not None: raise AssertionError((root/'server.log').read_text())
                try:
                    with urllib.request.urlopen(f'http://127.0.0.1:{port}/health', timeout=.2) as r:
                        if r.status == 200: return p
                except (OSError, urllib.error.URLError): pass
                time.sleep(.04)
            raise AssertionError('startup timeout')
        def sub(ws, kind, user=USER, **kw):
            ws.send(dict(method='subscribe', subscription=dict(type=kind, user=user, **kw)))
            ws.until('subscriptionResponse')
        try:
            proc = start()
            a = WS(port); clients.append(a)
            sub(a, 'userFills')
            status = a.until('walletStatus'); assert status['data']['historyComplete'] is False
            snapshot = a.until('userFills'); assert snapshot['data']['isSnapshot'] is True
            sub(a, 'orderUpdates'); a.until('walletStatus')
            # Wallet filters must ignore --markets BTC: use xyz asset and spot status.
            fill = dict(coin='xyz:NVDA', px='100', sz='1', side='B', time=int(time.time()*1000),
                startPosition='0', dir='Open Long', closedPnl='0', hash='hash', oid=1, crossed=True,
                fee='0.1', tid=123, feeToken='USDC', builderFee='0.01', deployerFee='0.02')
            order = dict(coin='@1', side='A', limitPx='1', sz='2', origSz='5', oid=999, timestamp=123,
                triggerCondition='N/A', isTrigger=False, triggerPx='0', isPositionTpsl=False,
                reduceOnly=False, orderType='Limit', tif='Gtc', cloid=None)
            with lock:
                state['orders'] = [dict(user=USER, time=datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat(), status='canceled', order=order)]
                state['fills'] = [[USER, fill], [USER, fill], [OTHER, dict(fill, tid=124)]]
            seen = {}
            while len(seen) < 2:
                msg = a.recv()
                if msg['channel'] in ('userFills', 'orderUpdates'): seen[msg['channel']] = msg
            assert seen['userFills']['data']['isSnapshot'] is False
            assert seen['userFills']['data']['fills'] == [fill], seen
            assert seen['orderUpdates']['data'][0]['order']['origSz'] == '5'
            assert isinstance(seen['orderUpdates']['data'][0]['statusTimestamp'], int)
            b = WS(port); clients.append(b); sub(b, 'userFills', OTHER)
            assert b.until('userFills')['data']['fills'][0]['tid'] == 124
            # Reconnect gets retained snapshot, no dependence on a book market filter.
            c = WS(port); clients.append(c); sub(c, 'userFills')
            assert c.until('userFills')['data']['fills'] == [fill]
            # Authoritative openOrders: no events merged onto potentially newer snapshots.
            sub(a, 'openOrders', dex='xyz')
            assert a.until('openOrders')['data']['orders'] == []
            with lock: state['open'] = [order]; state['slow'] = True
            a.send(dict(method='subscribe', subscription=dict(type='l2Book', coin='BTC', nLevels=5)))
            a.until('subscriptionResponse')
            books = 0
            while True:
                m = a.recv()
                if m['channel'] == 'l2Book': books += 1
                if m['channel'] == 'openOrders' and m['data']['orders']: break
            assert books >= 2, 'slow wallet HTTP blocked book delivery'
            with lock: state['fail'] = True; state['slow'] = False
            a.until('walletStatus', predicate=lambda m: m['data'].get('scope') == 'openOrders' and m['data']['state'] == 'Stale')
            with lock: state['fail'] = False; state['open'] = []
            assert a.until('openOrders')['data']['orders'] == []
            # Shared query cache: a second client must not double node request rate.
            d = WS(port); clients.append(d); sub(d, 'openOrders', dex='xyz')
            d.until('openOrders')
            with lock: before = state['queries']
            time.sleep(1.1)
            with lock: delta = state['queries'] - before
            assert delta <= 5, f'wallet query cache did not share requests: {delta}'
            # A skipped block must expose a wallet gap and keep the connection usable.
            with lock: state['skip'] = True
            a.until('walletStatus', predicate=lambda m: m['data'].get('resetRequired') is True and m['data'].get('generation', 0) > 0)
            a.until('l2Book')
            # Stop output: explicit gap on same WebSocket, followed by new snapshots.
            with lock: state['pause'] = True
            a.until('walletStatus', predicate=lambda m: m['data']['state'] == 'Stale')
            time.sleep(.1)
            with lock: state['pause'] = False
            a.until('walletStatus', predicate=lambda m: m['data']['state'] == 'Ready' and m['data'].get('resetRequired') is True)
            a.until('l2Book')
            # Unsupported aggregations/allowlist must error without disconnect.
            a.send(dict(method='subscribe', subscription=dict(type='userFills', user=USER, aggregateByTime=True)))
            assert 'aggregateByTime' in a.until('error')['data']
            a.send(dict(method='subscribe', subscription=dict(type='userFills', user='0x'+'3'*40)))
            assert '--wallets' in a.until('error')['data']
            # Persistence is bounded and survives process restart with explicit unavailable downtime.
            journal = root/'ws-wallet-journal.json'
            for _ in range(40):
                if journal.exists() and any(e['channel']=='userFills' for e in json.loads(journal.read_text())['events']): break
                time.sleep(.1)
            assert journal.exists() and journal.stat().st_size < 2*1024*1024
            # Runtime journal failure must be observable without stopping books or fills.
            backup = root/'journal-backup.json'
            journal.rename(backup); journal.mkdir()
            extra_fill = dict(fill, tid=125)
            with lock: state['fills'] = [[USER, extra_fill]]
            def journal_error():
                with urllib.request.urlopen(f'http://127.0.0.1:{port}/diagnostics', timeout=2) as response:
                    return json.load(response)['wallet']['journalError']
            for _ in range(40):
                if journal_error() is not None: break
                time.sleep(.1)
            assert journal_error() is not None
            a.until('l2Book')
            journal.rmdir(); backup.rename(journal)
            for _ in range(40):
                if journal_error() is None and any(e['data'].get('tid') == 125 for e in json.loads(journal.read_text())['events']): break
                time.sleep(.1)
            assert journal_error() is None
            assert any(e['data'].get('tid') == 125 for e in json.loads(journal.read_text())['events'])
            for ws in clients: ws.close()
            clients.clear(); proc.terminate(); proc.wait(timeout=10)
            proc = start(); a = WS(port); clients.append(a); sub(a, 'userFills')
            assert a.until('walletStatus')['data']['historyComplete'] is False
            assert a.until('userFills')['data']['fills'] == [fill, extra_fill]
            print('wallet e2e passed:', 'streamed' if streamed else 'batch')
        except Exception:
            print((root/'server.log').read_text(), file=sys.stderr)
            raise
        finally:
            with lock: state['stop'] = True
            for ws in clients: ws.close()
            if proc and proc.poll() is None: proc.terminate(); proc.wait(timeout=10)
            httpd.shutdown(); log.close()

if __name__ == '__main__': main()
