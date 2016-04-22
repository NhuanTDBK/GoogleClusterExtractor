# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

from pyspark import SparkConf, SparkContext
import pandas as pd
from pyspark.sql import SQLContext
from MapFunction import *
import numpy as np
import os

sc = SparkContext()
sqlContext = SQLContext(sc)
folder_path ='/home/ubuntu/google_cluster_data/task_usage/task_usage/'
schema_path = 'schema_tu.csv'
schema_df = ["mID","moment","cpu_rate","mem_usage","disk_io_time","disk_space"]
# data_path = '/home/ubuntu/google_cluster_data/task_usage/task_usage/part-00499-of-00500.csv'
schema = pd.read_csv(schema_path)
topMID = pd.read_csv('top10MID.csv',header=None,names=["mID"])
topFile = pd.read_csv('topFile.csv',header=None,names=["file"])
for file_name in topFile.file:
    dat = pd.read_csv("%s%s"%(folder_path,file_name),names=schema.columns)
    sample_dat = dat[["stime","etime","CPU_rate","canonical_mem","disk_IO","disk_usage","mID"]]
    sample_dat = sample_dat[sample_dat.mID.isin(topMID.mID)]
    sample_rdd = sqlContext.createDataFrame(sample_dat)
    sample_rdd.registerTempTable("metrics")
    cpu_val = sqlContext.sql("SELECT * from metrics")
    mapper = cpu_val.flatMap(flatMap_moments).reduceByKey(reduce_moments).map(map_moments).collect()
    df = pd.DataFrame(mapper,columns=schema_df)
    df.to_csv('data/mapperr_%s'%file_name,index=False,header=None)
sc.stop()

