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

schema_path = '/home/nhuanhunter/MyWorking/SparkInvestor/data/schema_tu.csv'
data_path = '/home/nhuanhunter/MyWorking/SparkInvestor/data/par0.csv'

schema = pd.read_csv(schema_path)
dat = pd.read_csv(data_path,names=schema.columns)
sample_dat = dat[["stime","etime","CPU_rate","mID"]]
sample_rdd = sqlContext.createDataFrame(sample_dat)

sample_rdd.registerTempTable("metrics")
cpu_val = sqlContext.sql("SELECT * from metrics")
mapper = cpu_val.flatMap(flatMap_moments).reduceByKey(reduce_moments).map(map_moments)
t = mapper.toPandas().to_csv('/home/ubuntu/machine_usage.csv')
# <codecell>


