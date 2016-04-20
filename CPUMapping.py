# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

from pyspark import SparkConf, SparkContext
import pandas as pd
from pyspark.sql import SQLContext
from MapFunction import *

import numpy as np
sc = SparkContext()
sqlContext = SQLContext(sc)

schema_path = 'schema_tu.csv'
data_path = '/home/ubuntu/google_cluster_data/task_usage/task_usage/part-00499-of-00500.csv'
schema = pd.read_csv(schema_path)
dat = pd.read_csv(data_path,names=schema.columns)
sample_dat = dat[["stime","etime","CPU_rate","mID"]][:3000]
sample_rdd = sqlContext.createDataFrame(sample_dat)
sample_rdd.registerTempTable("metrics")
cpu_val = sqlContext.sql("SELECT * from metrics")
mapper = cpu_val.flatMap(flatMap_moments).reduceByKey(reduce_moments).map(map_sorted).sortByKey(True,4).map(map_moments).collect()
df = pd.DataFrame(mapper,columns=['mID','moment','CPU'])
df.to_csv('mapperr',index=False)
# <codecell>
sc.stop()

