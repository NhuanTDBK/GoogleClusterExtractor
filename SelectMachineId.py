from pyspark import SparkConf, SparkContext
import pandas as pd
from pyspark.sql import SQLContext
from MapFunction import *
import os
import numpy as np
sc = SparkContext()
sqlContext = SQLContext(sc)
data_path ='/home/ubuntu/google_cluster_data/task_usage/task_usage/'
schema_path = 'schema_tu.csv'
schema = pd.read_csv(schema_path)
for file in os.listdir(data_path):
	dat = pd.read_csv(data_path,names=schema.columns)
	sample_dat = dat[["mID"]]       
	sample_rdd = sqlContext.createDataFrame(sample_dat)
	sample_rdd.registerTempTable("metrics")
	cpu_val = sqlContext.sql("SELECT mID,COUNT(*) as cnt FROM metrics GROUP BY mID ORDER BY cnt DESC LIMIT 10").collect()
	df = pd.DataFrame(cpu_val,columns=['mID','counts']).to_csv('count%s'%file)
sc.stop()

	
