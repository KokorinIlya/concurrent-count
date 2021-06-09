#!/usr/bin/env python3
import argparse
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("--max_threads", required=True, type=int)
parser.add_argument("--runs_count", required=True, type=int)
parser.add_argument("--milliseconds", required=True, type=int)
parser.add_argument("--expected_size", required=True, type=int)
parser.add_argument("--delete_prob", required=True, type=float)
parser.add_argument("--insert_prob", required=True, type=float)
parser.add_argument("--count_prob", required=True, type=float)
parser.add_argument("--keys_from", required=True, type=int)
parser.add_argument("--keys_until", required=True, type=int)
parser.add_argument("--out_dir", required=True)
args = parser.parse_args()

assert args.delete_prob + args.insert_prob + args.count_prob <= 1.

for bench_type in ['lock-persistent', 'lock-modifiable', 'universal', 'lock-free']:
    for threads in range(1, args.max_threads + 1):
        if threads == 1:
            create_file = True
        else:
            create_file = False
        args_list = [
            f'bench_type:{bench_type}',
            f'threads:{threads}',
            f'runs_count:{args.runs_count}',
            f'keys_from:{args.keys_from}',
            f'keys_until:{args.keys_until}',
            f'expected_size:{args.expected_size}',
            f'out_dir:{args.out_dir}',
            f'insert_prob:{args.insert_prob}',
            f'delete_prob:{args.delete_prob}',
            f'count_prob:{args.count_prob}',
            f'create_file:{create_file}',
            f'milliseconds:{args.milliseconds}'
        ]
        args_str = ';'.join(args_list)
        subprocess.run(["./gradlew", "bench", f'--args="{args_str}"'])