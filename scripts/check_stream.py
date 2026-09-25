#!/usr/bin/env python3
"""Read-only format/continuity preflight. Samples the final 16 MiB of each book stream."""
import argparse
import datetime
import json
from pathlib import Path

def latest(path):
    if path.is_file(): return path
    def key(p):
        return (int(p.name) if p.name.isdigit() else -1, p.name)
    for child in sorted(path.iterdir(), key=key, reverse=True):
        result = latest(child)
        if result is not None: return result
    return None

def sample(path):
    with path.open('rb') as f:
        f.seek(0, 2)
        size = f.tell()
        start = max(0, size - 16 * 1024 * 1024)
        f.seek(start)
        data = f.read()
    if start: data = data.partition(b'\n')[2]
    lines = data.split(b'\n')[:-1]
    blocks = []
    for line in lines:
        if not line: continue
        obj = json.loads(line)
        assert isinstance(obj.get('events'), list), f'{path}: expected block-info envelope, not bare events'
        blocks.append((int(obj['block_number']), obj['block_time'], len(obj['events'])))
    assert len(blocks) > 2, f'{path}: insufficient complete records'
    return blocks

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', type=Path, required=True)
    parser.add_argument('--order-status-dir', type=Path)
    parser.add_argument('--book-diff-dir', type=Path)
    parser.add_argument('--mode', choices=['batch','stream'], default='batch')
    args = parser.parse_args()
    sets = []
    for name, override in [('node_order_statuses_by_block',args.order_status_dir),('node_raw_book_diffs_by_block',args.book_diff_dir)]:
        path = latest(override or args.data_dir / name)
        assert path is not None, f'{name}: no files'
        blocks = sample(path)
        heights = [b[0] for b in blocks]
        assert all(a <= b for a,b in zip(heights,heights[1:])), f'{path}: regressing heights'
        repeats = sum(a == b for a,b in zip(heights,heights[1:]))
        if args.mode == 'batch': assert repeats == 0, 'repeated heights: configure streamed mode if these are fragments'
        unique = set(heights)
        gaps = max(unique) - min(unique) + 1 - len(unique)
        assert gaps == 0, f'{path}: {gaps} missing heights; cannot safely infer empty blocks; keep batch mode'
        stamp = datetime.datetime.fromisoformat(blocks[-1][1]).replace(tzinfo=datetime.timezone.utc)
        age = (datetime.datetime.now(datetime.timezone.utc) - stamp).total_seconds()
        print(f'{path}: records={len(blocks)} heights={min(unique)}..{max(unique)} repeated_heights={repeats} latest_age_s={age:.3f}')
        assert age < 5, 'sample is stale; check node health and active output directory'
        sets.append(unique)
    overlap = sets[0] & sets[1]
    assert len(overlap) >= 2, 'book streams do not overlap sufficiently'
    print('PASS: sampled envelopes and contiguous book heights. This sample does not prove future empty-block markers; monitor /health during streamed canary testing.')

if __name__ == '__main__': main()
