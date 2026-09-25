#!/usr/bin/env python3
"""Three synthetic wallets, eleven venues, account provenance and orderStatus.
Only localhost mocks. Optional --out PATH writes a synthetic wire transcript.
"""
import argparse
import collections
import datetime
import http.server
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request
from mock_e2e import WS

USERS=['0x'+str(n)*40 for n in (1,2,3)]
DEXES=['','abcd','cash','flx','hyna','io','km','mkts','para','vntl','xyz']
def now(): return int(time.time()*1000)
def perp(stamp):
    summary=dict(accountValue='100.0',totalNtlPos='2.0',totalRawUsd='100.0',totalMarginUsed='1.0')
    return dict(marginSummary=summary,crossMarginSummary=summary,crossMaintenanceMarginUsed='0.1',withdrawable='99.0',
        assetPositions=[dict(type='oneWay',position=dict(coin='BTC',szi='0.1',positionValue='2.0',entryPx='20.0'))],time=stamp)
def full_order(oid):
    return dict(coin='xyz:TEST',side='B',limitPx='20.0',sz='1.0',origSz='1.0',oid=oid,timestamp=now(),
        triggerCondition='N/A',isTrigger=False,triggerPx='0.0',children=[],isPositionTpsl=False,reduceOnly=False,orderType='Limit',tif='Gtc',cloid='0x'+format(oid,'032x'))
def main():
    parser=argparse.ArgumentParser();parser.add_argument('--out');args=parser.parse_args()
    transcript=[];phase="baseline";lock=threading.Lock();state=dict(height=100,stop=False,pause=False,orders=[],fills=[],bad=False,stale=False,slow=False,queries=collections.Counter())
    with tempfile.TemporaryDirectory(prefix='ws-bot-compat-') as tmp:
        root=Path(tmp); paths=[]
        for name in ['node_order_statuses_by_block','node_raw_book_diffs_by_block','node_fills_by_block']:
            p=root/name/'hourly'/'20260101'/'0';p.parent.mkdir(parents=True);p.touch();paths.append(p)
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                request=json.loads(self.rfile.read(int(self.headers['Content-Length'])));kind=request['type'];user=request.get('user')
                with lock:
                    state['queries'][(kind,user,request.get('dex'))]+=1
                    height,bad,stale,slow=state['height'],state['bad'],state['stale'],state['slow']
                if kind=='fileSnapshot':
                    Path(request['outPath']).write_text(json.dumps([height,[['BTC',[[],[]]]]]));data=None
                elif kind=='perpDexs': data=[None]+[dict(name=d) for d in DEXES if d]
                elif kind=='clearinghouseState':
                    assert user in USERS and request['dex'] in DEXES
                    if bad and request['dex']=='xyz':self.send_response(503);self.end_headers();return
                    if slow:time.sleep(.4)
                    data=perp(now()-(10000 if stale else 0))
                elif kind=='spotClearinghouseState':
                    assert user in USERS and 'ignorePortfolioMargin' not in request
                    data=dict(balances=[dict(coin='USDC',token=0,total='100.0',hold='1.0',entryNtl='100.0'),dict(coin='TEST',token=734,total='2.0',hold='0.0',entryNtl='1.0')])
                elif kind=='exchangeStatus':data=dict(time=now()-(10000 if stale else 0))
                elif kind=='frontendOpenOrders':assert request['dex'] in DEXES;data=[]
                else:raise AssertionError(request)
                self.send_response(200);self.end_headers()
                try:self.wfile.write(json.dumps(data).encode())
                except BrokenPipeError:pass
            def log_message(self,*_):pass
        httpd=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
        threading.Thread(target=httpd.serve_forever,daemon=True).start()
        def produce():
            while True:
                time.sleep(.04)
                with lock:
                    if state['stop']:return
                    if state['pause']:continue
                    state['height']+=1
                    stamp=datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat()
                    for i,path in enumerate(paths):
                        events=state['orders'] if i==0 else state['fills'] if i==2 else []
                        with path.open('a') as f:f.write(json.dumps(dict(local_time=stamp,block_time=stamp,block_number=state['height'],events=events))+'\n')
                    state['orders']=[];state['fills']=[]
        threading.Thread(target=produce,daemon=True).start()
        with socket.socket() as s:s.bind(('127.0.0.1',0));port=s.getsockname()[1]
        log=(root/'server.log').open('w');clients=[];proc=None
        def receive(ws):
            msg=ws.recv();transcript.append(dict(direction='recv',received_ms=now(),generation=0,connection=getattr(ws,'wallet','unknown'),scenario=phase,message=msg));assert msg['channel']!='error',msg;return msg
        def until(ws,channel,predicate=lambda m:True):
            deadline=time.monotonic()+12
            while time.monotonic()<deadline:
                m=receive(ws)
                if m['channel']==channel and predicate(m):return m
            raise AssertionError('timeout '+channel)
        def subscribe(ws,kind,user,**kw):
            req=dict(method='subscribe',subscription=dict(type=kind,user=user,**kw));ws.send(req);transcript.append(dict(direction='send',received_ms=now(),generation=0,connection=user,scenario=phase,message=req))
        def lookup(ws,oid,ident=71,user=USERS[0]):
            req=dict(method='post',id=ident,request=dict(type='info',payload=dict(type='orderStatus',user=user,oid=oid)));ws.send(req);transcript.append(dict(direction='send',received_ms=now(),generation=0,connection=user,scenario=phase,message=req))
            return until(ws,'post',lambda m:m['data']['id']==ident)['data']['response']
        try:
            command=[str(Path('target/release/websocket_server').resolve()),'--address','127.0.0.1','--port',str(port),'--node-data-dir',tmp,'--info-url',f'http://127.0.0.1:{httpd.server_port}/info','--wallets',','.join(USERS),'--markets','BTC','--websocket-compression-level','0','--stale-after-secs','1','--retry-interval-secs','1']
            proc=subprocess.Popen(command,stdout=log,stderr=log,env=dict(os.environ,RUST_LOG='warn',RAYON_NUM_THREADS='2',TOKIO_WORKER_THREADS='2'))
            for _ in range(150):
                try:
                    with urllib.request.urlopen(f'http://127.0.0.1:{port}/health',timeout=.2) as r:
                        if r.status==200:break
                except OSError:time.sleep(.04)
            else:raise AssertionError('startup timeout')
            for user in USERS:
                ws=WS(port);ws.wallet=user;clients.append(ws)
                for kind in ['allDexsClearinghouseState','spotState','userFills','orderUpdates']:subscribe(ws,kind,user)
                for dex in DEXES:subscribe(ws,'openOrders',user,dex=dex)
                got=set();orders=set();acks=0
                while len(got)<3 or orders!=set(DEXES) or acks<15:
                    m=receive(ws);ch=m['channel'];data=m.get('data',{})
                    if ch=='subscriptionResponse':
                        acks+=1
                        if data['subscription']['type']=='spotState':assert data['subscription']['ignorePortfolioMargin'] is False
                    if ch=='allDexsClearinghouseState':
                        assert [x[0] for x in data['clearinghouseStates']]==DEXES
                        assert all(type(v['time']) is int and isinstance(v['marginSummary']['accountValue'],str) for _,v in data['clearinghouseStates']);got.add(ch)
                    if ch=='spotState':assert len(data['spotState']['balances'])==2;got.add(ch)
                    if ch=='userFills':assert data['isSnapshot'];got.add(ch)
                    if ch=='openOrders':orders.add(data['dex'])
                    if ch=='walletStatus' and 'sampleCompletedAt' in data:
                        assert 0<=data['sampleCompletedAt']-data['sampleStartedAt']<=1000
                        assert data['atomic'] is False and data['generation']==0
            ws=clients[0]
            # Distinct identity forms and observed status cases preserve payload and request correlation.
            records={status:full_order(100+i) for i,status in enumerate(['open','canceled','filled'])}
            with lock:
                stamp=datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat()
                state['orders']=[dict(user=USERS[0],time=stamp,status=status,order=order) for status,order in records.items()]
            until(ws,'orderUpdates')
            for i,(status,order) in enumerate(records.items()):
                response=lookup(ws,order['oid'],71+i)
                assert response['type']=='info' and response['payload']['type']=='orderStatus',response
                result=response['payload']['data'];assert result['status']=='order' and result['order']['status']==status
                assert result['order']['order']==order
            assert lookup(ws,records['canceled']['cloid'],81)['payload']['data']['order']['status']=='canceled'
            missing=lookup(ws,999999,82);assert missing['type']=='error' and missing['payload'].startswith('LOCAL_HISTORY_UNAVAILABLE:')
            with lock:state['fills']=[[USERS[0],dict(coin='xyz:TEST',px='20.0',sz='0.1',side='B',time=now(),startPosition='0',dir='Open Long',closedPnl='0',hash='synthetic',oid=100,crossed=True,fee='0',tid=1,feeToken='USDC')]]
            until(ws,'userFills',lambda m:not m['data']['isSnapshot'])
            assert lookup(ws,100,83)['type']=='error','open status survived later fill without authoritative size'
            # Periodic unchanged spot snapshots are newly sampled, not cached heartbeat renewals.
            one=until(ws,'walletStatus',lambda m:m['data'].get('scope')=='spotState' and 'sampleCompletedAt' in m['data'])
            two=until(ws,'walletStatus',lambda m:m['data'].get('scope')=='spotState' and m['data'].get('sampleCompletedAt',0)>one['data']['sampleCompletedAt'])
            assert two['data']['sampleStartedAt']>=one['data']['sampleCompletedAt']
            phase='recovery'
            with lock:state['bad']=True
            until(ws,'walletStatus',lambda m:m['data'].get('scope')=='allDexsClearinghouseState' and m['data']['state']=='Stale')
            # After declared failure no partial perp sample may leak; independent spot keeps working.
            spot_seen=False
            # Allow the 1s sampling period plus the 2s bounded HTTP work window;
            # 1.2s was a flaky scheduling assertion under three-wallet fanout.
            deadline=time.monotonic()+3.0
            while time.monotonic()<deadline:
                m=receive(ws);assert m['channel']!='allDexsClearinghouseState'
                spot_seen |= m['channel']=='spotState'
            assert spot_seen
            with lock:state['bad']=False
            until(ws,'allDexsClearinghouseState')
            with lock:state['stale']=True
            until(ws,'walletStatus',lambda m:m['data'].get('scope')=='spotState' and m['data']['state']=='Stale')
            with lock:state['stale']=False
            until(ws,'spotState')
            # Slow fanout cannot create a falsely atomic or fresh account bundle.
            with lock:state['slow']=True
            until(ws,'walletStatus',lambda m:m['data'].get('scope')=='allDexsClearinghouseState' and m['data']['state']=='Stale')
            with lock:state['slow']=False
            until(ws,'allDexsClearinghouseState')
            # Unconfigured wallets receive a clear subscription error without socket loss.
            denied=WS(port);clients.append(denied);subscribe(denied,'spotState','0x'+'4'*40)
            assert '--wallets' in denied.until('error')['data']
            print('PASS: 3 wallets / 11 DEX baselines, exact account envelopes, provenance, numeric/cloid status, history misses, later-fill invalidation, partial/stale/slow sample suppression')
        except Exception:
            print((root/'server.log').read_text());raise
        finally:
            with lock:state['stop']=True
            for ws in clients:ws.close()
            if proc is not None:proc.terminate();proc.wait(timeout=10)
            httpd.shutdown();log.close()
            if args.out:
                path=Path(args.out);path.write_text(''.join(json.dumps(row)+'\n' for row in transcript))
                path.with_suffix('.manifest.json').write_text(json.dumps(dict(synthetic=True,wallets=USERS,dexes=DEXES,spot_tokens=[0,734],revision=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),working_tree_modified=True),indent=2)+'\n')
if __name__=='__main__':main()
