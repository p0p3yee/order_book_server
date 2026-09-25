"""Regression: --init containers must include the real workload, not just PID 1."""
import unittest
from unittest.mock import patch

import collect_host_diagnostics as collector


class ContainerProcesses(unittest.TestCase):
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
