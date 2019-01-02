#!/usr/bin/env python3.6
import os
import sys

window_size = 50
file_num = 10
local_folder = 'dgim'

os.system(f'hdfs dfs -getmerge dgim/{file_num - 1} result')
k = int(sys.argv[1]) if len(sys.argv) > 1 else window_size
assert(0 <= k <= window_size)

def read_stream():
  ts = 0
  ret = []
  ws = k
  for i in reversed(range(file_num)):
    if not ws:
      break
    with open(f'{local_folder}/data{i}.txt') as f:
      t, *res = f.read().split()
      if not ts:
        ts = int(t) + len(res) - 1
      w = min(len(res), ws)
      ret += res[-1:-w - 1:-1]
      ws -= w
  return ts, ret

def read_dgim():
  ret = []
  with open('result') as f:
    for line in f:
      p, *end = line.split()
      ret.append((2 ** int(p)) * len(end))
  return ret

ts, window = read_stream()
answer = sum([window[i] == '1' for i in range(k)])

dgim = read_dgim()
approx = sum(dgim[:-1]) + dgim[-1] // 2

error = abs(answer - approx) / answer

print('Final Streaming Time:', ts)
print(f'Query Window ({window_size}): {k}')
print('Real:', answer)
print('DGIM:', approx)
print(f'Error: {error:.2f}')
