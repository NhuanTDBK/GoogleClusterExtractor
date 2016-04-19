# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

from pyspark import SparkConf, SparkContext
import pandas as pd
from pyspark.sql import SQLContext

import numpy as np

conf = (SparkConf()
         .setMaster("local")
         .setAppName("CPU Resource")
         .set("spark.executor.memory", "4g")
	 .set("spark.driver.maxResultSize","2g"))
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

schema = pd.read_csv("schema_tu.csv")
dat = pd.read_csv('/home/ubuntu/google_cluster_data/task_usage/task_usage/part-00499-of-00500.csv',names=schema.columns)
sample_data = dat
del dat
del schema
sample_rdd = sqlContext.createDataFrame(sample_data)
del sample_data

sample_rdd.registerTempTable("metrics")
cpu_val = sqlContext.sql("SELECT stime,etime,mID,CPU_rate from metrics limit 300")

def map_moments(T):
    stime = T[0]
    etime = T[1]
    moments = np.arange(stime,etime,step=1E6)
    return [((T[2],t),T[3]) for t in moments]

reduce_moments = lambda (x,y) : x[1]+y[1]
flatMap_moments = lambda (x) :(x[0][0],x[0][1],x[1])
mapper = cpu_val.flatMap(map_moments).reduceByKey(reduce_moments).map(flatMap_moments)

# <codecell>

t = mapper.collect()
#mapper.saveAsSequenceFile('machine_usage_seq')
# <codecell>
#txtFile = pd.DataFrame(t,columns=["MID","Moment","CPU"])
#print txtFile.shape
#txtFile.to_csv('/home/ubuntu/machine_usage.csv',index=False)
# <codecell>
#mapper.saveAsTextFile('machine_usage_seq')
txtFile = pd.DataFrame(t,columns=["MID","Moment","CPU"])
txtFile.to_csv('/home/ubuntu/machine_usage.csv',index=False)

