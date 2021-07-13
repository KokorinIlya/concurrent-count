#!/usr/bin/env python3
import argparse
import subprocess
import os

parser = argparse.ArgumentParser()
parser.add_argument("--max_threads", required=True, type=int)
parser.add_argument("--runs_count", required=True, type=int)
parser.add_argument("--milliseconds", required=True, type=int)
parser.add_argument("--initial_size", required=True, type=int)
parser.add_argument("--delete_prob", required=True, type=float)
parser.add_argument("--insert_prob", required=True, type=float)
parser.add_argument("--count_prob", required=True, type=float)
parser.add_argument("--key_range", required=False)
parser.add_argument("--out_dir", required=True)
args = parser.parse_args()

assert args.delete_prob + args.insert_prob + args.count_prob <= 1.
os.makedirs(args.out_dir, exist_ok=True)

for bench_type in ['universal', 'lock-free']: # 'lock-persistent', 'lock-modifiable'
    with open(f'{args.out_dir}/{bench_type}.bench', 'w') as out_file:
        for threads in range(1, args.max_threads + 1):
            print(f'Bench name = {bench_type}, threads = {threads}')

            args_list = [
                f'bench_type:{bench_type}',
                f'threads:{threads}',
                f'initial_size:{args.initial_size}',
                f'insert_prob:{args.insert_prob}',
                f'delete_prob:{args.delete_prob}',
                f'count_prob:{args.count_prob}',
                f'milliseconds:{args.milliseconds}'
            ]
            if args.key_range is not None:
                args_list.append(f'key_range:{args.key_range}')
            args_str = ';'.join(args_list)

            bench_res = 0.
            for _ in range(args.runs_count):
                run_res = subprocess.run(['./gradlew', 'bench', f'--args="{args_str}"'], stdout=subprocess.PIPE)
                output = str(run_res.stdout).split('\\n')
                bench_res += float(output[-5])
            bench_res /= args.runs_count
            out_file.write(f'{threads} threads, {bench_res} ops / millisecond\n')
