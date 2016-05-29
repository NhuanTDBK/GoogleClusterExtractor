# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

import pandas as pd
# from pyspark.sql import SQLContext
# from MapFunction import *
import numpy as np
import os

folder_path ='dataset/original/'
schema_path = 'schema_tu.csv'
# schema_df = ["mID","moment","cpu_rate","mem_usage","disk_io_time","disk_space"]
data_path = 'dataset/part-00232-of-00500.csv.gz'

# <codecell>

schema = pd.read_csv(schema_path)
total_size = []
sum_size = []
for file_path in os.listdir(folder_path):   
    print "Reading %s"%file_path
    dat = pd.read_csv("%s/%s"%(folder_path,file_path),names=schema.columns,compression='gzip')
    grouped = dat.groupby('jID')
    group_size = grouped.size()
    for jID in group_size.nlargest(10).index:
        sample = dat[dat.jID==jID]
        task_length = (sample.etime.max()-sample.stime.min())/1E6
        total_size.append((jID,task_length,file_path))
print "Get top job ID by run-time"
df = pd.DataFrame(total_size,columns=["jID","length","file"])
s = df.groupby('jID').sum()
s.nlargest(10,'length').to_csv('top_jobID')

