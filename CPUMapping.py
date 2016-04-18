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
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

schema = pd.read_csv("/home/nhuanhunter/MyWorking/SparkInvestor/data/schema_tu.csv")
dat = pd.read_csv('/home/nhuanhunter/MyWorking/SparkInvestor/data/par0.csv',names=schema.columns)
sample_data = dat
del dat
del schema
sample_rdd = sqlContext.createDataFrame(sample_data)
del sample_data

sample_rdd.registerTempTable("metrics")
cpu_val = sqlContext.sql("SELECT stime,etime,mID,CPU_rate from metrics limit 10")

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

# <codecell>
txtFile = pd.DataFrame(t,columns=["MID","Moment","CPU"])
txtFile.to_csv('machine_usage')
# <codecell>


