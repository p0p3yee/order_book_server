#!/usr/bin/env python3
"""Read-only, equal-depth LAN/public WS comparison. Standard library; no fileSnapshot requests."""
import argparse
import base64
import concurrent.futures
import datetime
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import socket
import ssl
import statistics
import struct
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

class WebSocket:
    def __init__(self, url):
        u = urllib.parse.urlparse(url)
        if u.scheme not in ('ws','wss'): raise ValueError('expected ws or wss URL')
        self.sock = socket.create_connection((u.hostname, u.port or (443 if u.scheme == 'wss' else 80)), timeout=5)
        if u.scheme == 'wss': self.sock = ssl.create_default_context().wrap_socket(self.sock, server_hostname=u.hostname)
        self.sock.settimeout(5)
        self.buffer = bytearray()
        self.fragment = bytearray()
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock.sendall((f'GET {u.path or "/"} HTTP/1.1\r\nHost: {u.netloc}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n').encode())
        while b'\r\n\r\n' not in self.buffer:
            chunk = self.sock.recv(4096)
            if not chunk: raise EOFError('upgrade closed')
            self.buffer.extend(chunk)
        header, body = self.buffer.split(b'\r\n\r\n',1)
        self.buffer = bytearray(body)
        expected = base64.b64encode(hashlib.sha1((key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest())
        if b' 101 ' not in header or expected not in header: raise ValueError(f'upgrade failed: {header!r}')
    def exact(self,n):
        while len(self.buffer) < n:
            chunk = self.sock.recv(max(4096,n-len(self.buffer)))
            if not chunk: raise EOFError('connection closed')
            self.buffer.extend(chunk)
        result = bytes(self.buffer[:n]); del self.buffer[:n]
        return result
    def frame(self, payload, opcode=1):
        mask = os.urandom(4)
        n = len(payload)
        header = bytes([128|opcode,128|n]) if n < 126 else bytes([128|opcode,254])+struct.pack('!H',n)
        self.sock.sendall(header+mask+bytes(b^mask[i%4] for i,b in enumerate(payload)))
    def send(self,obj): self.frame(json.dumps(obj).encode())
    def recv(self):
        while True:
            first, second = self.exact(2)
            n = second & 127
            if n == 126: n = struct.unpack('!H',self.exact(2))[0]
            if n == 127: n = struct.unpack('!Q',self.exact(8))[0]
            if n > 16*1024*1024: raise ValueError('oversized frame')
            mask = self.exact(4) if second & 128 else None
            payload = self.exact(n)
            if mask: payload = bytes(b^mask[i%4] for i,b in enumerate(payload))
            opcode = first & 15
            if opcode == 8: raise EOFError('close frame')
            if opcode == 9: self.frame(payload,10); continue
            if opcode == 10: continue
            if first & 0x70: raise ValueError('unexpected compressed/reserved frame')
            self.fragment.extend(payload)
            if not first & 128: continue
            result = json.loads(self.fragment); self.fragment.clear()
            return result
    def close(self):
        try: self.sock.shutdown(socket.SHUT_RDWR)
        except OSError: pass
        self.sock.close()

def summary(values):
    if not values: return {'count':0}
    values = sorted(values)
    return {'count':len(values),'median':statistics.median(values), 'p95':values[min(len(values)-1,int(len(values)*.95))], 'max':values[-1], 'min':values[0]}

def http_json(url, payload=None):
    req = urllib.request.Request(url, data=None if payload is None else json.dumps(payload).encode(), headers={'Content-Type':'application/json'})
    start = time.monotonic_ns()
    try:
        with urllib.request.urlopen(req,timeout=5) as res:
            return {'status':res.status,'data':json.load(res),'rtt_ms':(time.monotonic_ns()-start)/1e6,'received_ms':time.time_ns()/1e6}
    except urllib.error.HTTPError as err:
        return {'status':err.code,'body':err.read(1024).decode(errors='replace'),'rtt_ms':(time.monotonic_ns()-start)/1e6}
    except Exception as err: return {'error':str(err)}

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--local-ws',default='ws://127.0.0.1:8000/ws')
    p.add_argument('--local-info',default='http://127.0.0.1:3001/info')
    p.add_argument('--public-ws',default='wss://api.hyperliquid.xyz/ws')
    p.add_argument('--public-info',default='https://api.hyperliquid.xyz/info')
    p.add_argument('--coin',default='BTC'); p.add_argument('--seconds',type=int,default=60)
    p.add_argument('--out',type=Path,required=True)
    a = p.parse_args()
    if not 1 <= a.seconds <= 600: p.error('seconds must be 1..600')
    records = {'local':{'books':{},'trades':{},'ages_ms':[],'errors':[]},'public':{'books':{},'trades':{},'ages_ms':[],'errors':[]}}
    stop = threading.Event(); sockets=[]; threads=[]
    start_utc = datetime.datetime.now(datetime.timezone.utc).isoformat()
    def read(name,ws):
        r = records[name]
        try:
            while not stop.is_set():
                msg = ws.recv(); received = time.monotonic_ns()/1e6
                if msg.get('channel') == 'l2Book':
                    b=msg['data']; key=b['time']
                    levels=tuple(tuple((str(Decimal(l['px']).normalize()),str(Decimal(l['sz']).normalize()),l['n']) for l in side[:5]) for side in b['levels'])
                    r['books'].setdefault(key,{}).setdefault(levels,received)
                    r['ages_ms'].append(time.time_ns()/1e6-key)
                elif msg.get('channel') == 'trades':
                    for t in msg['data']:
                        key=(t['time'],t['coin'],t['tid'])
                        r['trades'].setdefault(key,(received,t['px'],t['sz'],t['side']))
                elif msg.get('channel') in ('error','status'): r['errors'].append(msg)
        except Exception as err:
            if not stop.is_set(): r['errors'].append({'transport_error':str(err)})
    polls=[]
    try:
        for name,url in [('local',a.local_ws),('public',a.public_ws)]:
            ws=WebSocket(url); sockets.append(ws)
            book={'type':'l2Book','coin':a.coin}
            if name=='public': book['fast']=True
            else: book['nLevels']=5
            ws.send({'method':'subscribe','subscription':book})
            ws.send({'method':'subscribe','subscription':{'type':'trades','coin':a.coin}})
            thread=threading.Thread(target=read,args=(name,ws),daemon=True); threads.append(thread); thread.start()
        health=a.local_ws.replace('ws://','http://').replace('wss://','https://').rsplit('/ws',1)[0]+'/health'
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            deadline=time.monotonic()+a.seconds
            while time.monotonic()<deadline:
                tick=time.monotonic()
                futures=[pool.submit(http_json,url,payload) for url,payload in [(a.local_info,{'type':'exchangeStatus'}),(a.public_info,{'type':'exchangeStatus'}),(health,None)]]
                polls.append(dict(zip(['local','public','health'],[f.result() for f in futures])))
                stop.wait(max(0,min(deadline-time.monotonic(),1-(time.monotonic()-tick))))
    finally:
        stop.set()
        for ws in sockets: ws.close()
        for thread in threads: thread.join(timeout=2)
    left,right=records['local'],records['public']
    common=left['books'].keys() & right['books'].keys()
    deltas=[]; mismatch=[]; ambiguous=0
    for key in common:
        lb,pb=left['books'][key],right['books'][key]
        if len(lb)!=1 or len(pb)!=1: ambiguous+=1; continue
        if lb.keys()!=pb.keys(): mismatch.append(key)
        else: deltas.append(next(iter(lb.values()))-next(iter(pb.values())))
    trade_deltas=[]; trade_mismatch=0
    for key in left['trades'].keys() & right['trades'].keys():
        l,r=left['trades'][key],right['trades'][key]
        if (Decimal(l[1]),Decimal(l[2]),l[3]) != (Decimal(r[1]),Decimal(r[2]),r[3]): trade_mismatch+=1
        else: trade_deltas.append(l[0]-r[0])
    def times(name): return [v[name]['data']['time'] for v in polls if 'data' in v[name] and 'time' in v[name]['data']]
    raw_chain_delta=[v['public']['data']['time']-v['local']['data']['time'] for v in polls if 'data' in v['local'] and 'data' in v['public'] and 'time' in v['local']['data'] and 'time' in v['public']['data']]
    result={'start_utc':start_utc,'end_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'coin':a.coin,'seconds':a.seconds,'depth':5,
            'book_match_arrival_local_minus_public_ms':summary(deltas),'book_mismatch_timestamps':mismatch,'ambiguous_book_timestamps':ambiguous,
            'trade_match_arrival_local_minus_public_ms':summary(trade_deltas),'trade_mismatches':trade_mismatch,
            'book_age_ms':{n:summary(r['ages_ms']) for n,r in records.items()},
            'book_over_1s':{n:sum(v>1000 for v in r['ages_ms']) for n,r in records.items()},
            'http_rtt_ms':{n:summary([v[n]['rtt_ms'] for v in polls if 'rtt_ms' in v[n]]) for n in ['local','public']},
            'raw_concurrent_public_minus_local_chain_time_ms':summary(raw_chain_delta),
            'events':{n:{'book_timestamps':len(r['books']),'trade_ids':len(r['trades']),'errors_and_status':r['errors']} for n,r in records.items()},
            'http_samples':polls,
            'caveats':['HTTP RTT includes new connections/TLS; concurrent responses are not simultaneous node-state samples.',
                       'Matched WS arrival deltas use one observer monotonic clock; event age depends on observer clock synchronization.',
                       'Both sides normalized to five levels; ambiguous same-millisecond multi-state timestamps excluded.']}
    a.out.parent.mkdir(parents=True,exist_ok=True); a.out.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k!='http_samples'},indent=2))

if __name__=='__main__': main()
