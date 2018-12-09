#!/usr/bin/env python3.6
import os
import random
import shutil

stream_len = 100
file_num = 10
local_folder = 'dgim'
hdfs_folder = 'data'

shutil.rmtree(local_folder, ignore_errors=True)
os.mkdir(local_folder)

for i in range(file_num):
  with open(f'{local_folder}/data{i}.txt', 'w+') as f:
    f.write(f'{stream_len * i + 1}\t')
    for _ in range(stream_len):
      f.write(random.choice(['0 ', '1 ']))

os.system(f'hdfs dfs -rm -r -f {hdfs_folder}/{local_folder}')
os.system(f'hdfs dfs -copyFromLocal {local_folder} {hdfs_folder}/')
