"""Regression: --init containers must include the real workload, not just PID 1."""
import unittest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import collect_host_diagnostics as collector


class ContainerProcesses(unittest.TestCase):
    def test_opt_in_profile_targets_node_child_and_preserves_normal_collection(self):
        def command(args):
            if args[:2] == ['docker', 'inspect']:
                pid = 200 if args[2] == 'node' else 100
                return {'returncode':0,'stdout':json.dumps([{'HostConfig':{},'State':{'Pid':pid,'StartedAt':'test'},
                    'Image':'test','RestartCount':0,'Config':{}}])}
            if args[:2] == ['docker', 'top']:
                text = 'PID COMMAND\n200 hl-visor\n201 hl-node\n' if args[2] == 'node' else 'PID COMMAND\n100 docker-init\n101 websocket_serve\n'
                return {'returncode':0,'stdout':text}
            return {'returncode':0,'stdout':''}
        with tempfile.TemporaryDirectory() as temp:
            out=Path(temp)/'capture'
            with patch('sys.argv',['collector','--seconds','1','--out',str(out),'--node-container','node','--profile-node']), \
                 patch.object(collector,'run',side_effect=command) as commands, \
                 patch.object(collector.shutil,'which',side_effect=lambda name:'/mock/perf' if name=='perf' else None), \
                 patch.object(collector.subprocess,'Popen') as spawn, \
                 patch.object(collector.urllib.request,'urlopen',side_effect=OSError('offline fixture')), \
                 patch.object(collector.time,'monotonic',side_effect=[0,0,0,0,0,1,1,2]), \
                 patch.object(collector.time,'sleep'), patch('builtins.print'):
                spawn.return_value.poll.return_value=0
                collector.main()
            args=spawn.call_args.args[0]
            self.assertEqual(args[args.index('-p')+1],'201')
            self.assertEqual(args[args.index('-F')+1],'49')
            self.assertEqual(args[-3:],['--','sleep','1'])
            spawn.return_value.terminate.assert_not_called()
            self.assertTrue((out/'samples.jsonl').read_text())
            self.assertTrue((out/'node.log').exists())
            self.assertIn(['sha256sum','/proc/201/exe'],[call.args[0] for call in commands.call_args_list])

    def test_init_and_server_are_both_collected(self):
        with patch.object(collector, 'run', return_value={
            'returncode': 0,
            'stdout': 'PID COMMAND\n1836829 docker-init\n1836843 websocket_serve\n',
        }) as command:
            processes, error = collector.container_processes('ws', 1836829)
        self.assertEqual(processes, {1836829: 'docker-init', 1836843: 'websocket_serve'})
        self.assertIsNone(error)
        command.assert_called_once_with(['docker', 'top', 'ws', '-eo', 'pid,comm'])

    def test_no_init_wrapper_does_not_duplicate_server(self):
        with patch.object(collector, 'run', return_value={
            'returncode': 0, 'stdout': 'PID COMMAND\n123 websocket_serve\n',
        }):
            processes, error = collector.container_processes('ws', 123)
        self.assertEqual(processes, {123: 'websocket_serve'})
        self.assertIsNone(error)

    def test_failed_discovery_keeps_cgroup_access_and_reports_failure(self):
        with patch.object(collector, 'run', return_value={'returncode': 1, 'stderr': 'unavailable'}):
            processes, error = collector.container_processes('ws', 123)
        self.assertEqual(processes, {123: 'container-init'})
        self.assertEqual(error, {'error': 'unavailable'})


if __name__ == '__main__':
    unittest.main()
