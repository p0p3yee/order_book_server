#!/usr/bin/env python3
"""Run ON the Linux node host. Read-only bounded collection; never restarts containers or requests fileSnapshot."""
import argparse
import datetime
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import urllib.error
import urllib.request


def run(args):
    try:
        p = subprocess.run(args, capture_output=True, text=True, timeout=10)
        return {'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
    except Exception as err: return {'error':str(err)}


def container_processes(name, init_pid):
    # Docker inspect's PID can be docker-init when --init is enabled. Include
    # actual container processes, using comm rather than potentially secret args.
    result = run(['docker', 'top', name, '-eo', 'pid,comm'])
    processes = {}
    if init_pid:
        processes[init_pid] = 'container-init'
    if result.get('returncode') == 0:
        for line in result.get('stdout', '').splitlines():
            fields = line.split(None, 1)
            if len(fields) == 2 and fields[0].isdigit():
                processes[int(fields[0])] = fields[1]
    else:
        result = {'error': result.get('stderr') or result.get('error') or 'docker top failed'}
        return processes, result
    return processes, None


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds',type=int,default=300)
    parser.add_argument('--ws-container',default='hyperliquid-ws-low-latency')
    parser.add_argument('--node-container')
    parser.add_argument('--profile-node',action='store_true',
                        help='Opt-in 49 Hz CPU sampling of hl-node with perf; adds overhead, never changes node settings')
    parser.add_argument('--base-url',default='http://127.0.0.1:8000')
    parser.add_argument('--out',type=Path)
    a=parser.parse_args()
    if not 1 <= a.seconds <= 900: parser.error('seconds must be 1..900')
    if a.profile_node and not a.node_container: parser.error('--profile-node requires --node-container')
    start=datetime.datetime.now(datetime.timezone.utc)
    out=a.out or Path('/tmp')/('hl-ws-diagnostics-'+start.strftime('%Y%m%dT%H%M%SZ'))
    out.mkdir(parents=True,exist_ok=False)
    metadata={'start_utc':start.isoformat(),'duration_s':a.seconds,'containers':{}}
    pids=[]
    for name in filter(None,[a.ws_container,a.node_container]):
        result=run(['docker','inspect',name])
        try:
            c=json.loads(result['stdout'])[0]
            host=c['HostConfig']; state=c['State']
            metadata['containers'][name]={'image_id':c['Image'],'pid':state['Pid'], 'started_at':state['StartedAt'],
                'restart_count':c['RestartCount'],'network_mode':host.get('NetworkMode'), 'nano_cpus':host.get('NanoCpus'),
                'cpu_quota':host.get('CpuQuota'),'cpu_period':host.get('CpuPeriod'),'memory_limit':host.get('Memory'),
                'image_labels':{k:v for k,v in (c.get('Config',{}).get('Labels') or {}).items() if k.startswith('org.opencontainers.image.')}}
            processes, error = container_processes(name, state['Pid'])
            metadata['containers'][name]['processes_at_start'] = processes
            if error: metadata['containers'][name]['process_discovery_error'] = error
            pids.extend(processes)
        except Exception:
            metadata['containers'][name]={'error':'docker inspect unavailable', 'stderr':result.get('stderr',result.get('error'))}
    metadata['clock']=run(['timedatectl','show','-p','NTPSynchronized','-p','TimeUSec'])
    metadata['chrony']=run(['chronyc','tracking'])
    metadata['tcp_listeners']=run(['ss','-ltnp','sport = :8000'])
    metadata['kernel']=run(['uname','-r'])
    metadata['cpu_topology']=run(['lscpu'])
    node_processes=metadata['containers'].get(a.node_container,{}).get('processes_at_start',{})
    node_pids=[pid for pid,comm in node_processes.items() if comm == 'hl-node']
    if len(node_pids)==1:
        metadata['node_binary_sha256']=run(['sha256sum',f'/proc/{node_pids[0]}/exe'])
    metadata['process_sampling_note']='PIDs discovered at collection start; rerun after a container restart. Shared cgroup counters must not be summed across processes.'
    pids = sorted(set(pids))
    (out/'metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
    jobs=[]; files=[]
    commands=[('vmstat.txt',['vmstat','1',str(a.seconds+1)]),
              ('iostat.txt',['iostat','-x','1',str(a.seconds+1)]),
              ('mpstat.txt',['mpstat','-P','ALL','1',str(a.seconds)])]
    if pids:commands.append(('pidstat.txt',['pidstat','-dru','-p',','.join(map(str,pids)),'1',str(a.seconds)]))
    for filename,command in commands:
        if shutil.which(command[0]):
            f=(out/filename).open('w');files.append(f)
            jobs.append(subprocess.Popen(command,stdout=f,stderr=subprocess.STDOUT,
                                         env={**os.environ, 'LC_ALL':'C', 'TZ':'UTC'}))
        else:(out/filename).write_text(f'{command[0]} unavailable; no package installation attempted\n')
    if a.profile_node:
        if len(node_pids)!=1 or not shutil.which('perf'):
            (out/'perf.log').write_text('Profiling unavailable: need perf and exactly one discovered hl-node process. No packages installed.\n')
        else:
            # CPU cycles for the node, including its kernel work. Frame-pointer stacks may be incomplete in a
            # stripped/non-frame-pointer binary; preserve raw data and do not infer
            # absent functions from missing symbols. The target is never stopped.
            f=(out/'perf.log').open('w');files.append(f)
            jobs.append(subprocess.Popen(['perf','record','-e','cycles','-F','49',
                '--call-graph','fp','-p',str(node_pids[0]),'-o',str(out/'node.perf.data'),
                '--','sleep',str(a.seconds)],stdout=f,stderr=subprocess.STDOUT,
                env={**os.environ,'LC_ALL':'C','TZ':'UTC'}))
    def read(path):
        try:return Path(path).read_text()
        except OSError as err:return str(err)
    try:
        deadline=time.monotonic()+a.seconds
        with (out/'samples.jsonl').open('w') as f:
            while time.monotonic()<deadline:
                tick=time.monotonic()
                sample={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        'pressure':{k:read('/proc/pressure/'+k) for k in ['cpu','io','memory']}, 'processes':{}}
                for pid in pids:
                    proc={k:read(f'/proc/{pid}/{k}') for k in ['stat','status','io','cgroup']}
                    for line in proc['cgroup'].splitlines():
                        if line.startswith('0::'):
                            group=Path('/sys/fs/cgroup')/line[3:].lstrip('/')
                            proc['cgroup_v2']={k:read(group/k) for k in ['cpu.stat','cpu.max','memory.current','memory.events','io.stat','cpu.pressure','io.pressure']}
                    sample['processes'][str(pid)]=proc
                for endpoint in ['health','diagnostics']:
                    began=time.monotonic()
                    try:
                        try:response=urllib.request.urlopen(a.base_url.rstrip('/')+'/'+endpoint,timeout=2)
                        except urllib.error.HTTPError as err:response=err
                        with response:
                            body=response.read().decode()
                            sample[endpoint]={'status':response.status,'body':json.loads(body) if body else None,
                                              'rtt_ms':(time.monotonic()-began)*1000}
                    except Exception as err:sample[endpoint]={'error':str(err)}
                f.write(json.dumps(sample)+'\n');f.flush()
                time.sleep(max(0,min(deadline-time.monotonic(),1-(time.monotonic()-tick))))
    finally:
        for job in jobs:
            if job.poll() is None:job.terminate()
            job.wait(timeout=5)
        for f in files:f.close()
    logs=run(['docker','logs','--since',start.isoformat(),'--tail','1000',a.ws_container])
    (out/'ws.log').write_text(logs.get('stdout','')+logs.get('stderr','')+logs.get('error',''))
    if a.node_container:
        logs=run(['docker','logs','--since',start.isoformat(),'--tail','10000',a.node_container])
        (out/'node.log').write_text(logs.get('stdout','')+logs.get('stderr','')+logs.get('error',''))
    if a.profile_node and (out/'node.perf.data').exists():
        report=run(['perf','report','--stdio','--no-children','--percent-limit','1',
                    '--sort','comm,dso,symbol','-i',str(out/'node.perf.data')])
        (out/'perf-report.txt').write_text(report.get('stdout','')+report.get('stderr','')+report.get('error',''))
    print(f'Collected read-only diagnostics in {out}. Logs may contain wallet addresses; review before sharing.')

if __name__=='__main__':main()
