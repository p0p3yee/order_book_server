#!/usr/bin/env python3
"""Reject deployment identifiers in tracked files; never print the matched values."""
import argparse
import ipaddress
from pathlib import Path
import re
import subprocess


def violations(text):
    for line_number, line in enumerate(text.splitlines(), 1):
        for match in re.finditer(r'0x([0-9a-fA-F]{40})(?![0-9a-fA-F])', line):
            value = match[1].lower()
            if int(value, 16) > 16 and len(set(value)) != 1:
                yield line_number, 'non-synthetic wallet address'
        for match in re.finditer(r'(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])', line):
            try:
                address = ipaddress.ip_address(match[0])
            except ValueError:
                continue
            if address.is_private and not address.is_loopback and not address.is_unspecified:
                yield line_number, 'private network address'
        if re.search(r'/(?:Users|home)/[A-Za-z0-9_.-]+|/mnt/' + r'disks/|/root/' + r'hl/', line):
            yield line_number, 'deployment-specific filesystem path'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--staged', action='store_true', help='Check Git index instead of working files')
    args = parser.parse_args()
    root = Path(subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip())
    paths = subprocess.check_output(['git', 'ls-files', '-z'], cwd=root).decode().split('\0')
    failures = []
    for name in filter(None, paths):
        if args.staged:
            raw = subprocess.check_output(['git', 'show', ':' + name], cwd=root)
        else:
            path = root / name
            if not path.is_file():
                continue
            raw = path.read_bytes()
        try:
            contents = raw.decode('utf-8')
        except UnicodeDecodeError:
            continue
        failures.extend(f'{name}:{line}: {reason}' for line, reason in violations(contents))
    if failures:
        print('\n'.join(failures))
        raise SystemExit('Use synthetic examples and private environment configuration before committing.')
    print('Public configuration check passed (tracked text files).')


if __name__ == '__main__':
    main()
