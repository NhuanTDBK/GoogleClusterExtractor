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
for file_name in os.listdir(data_path):
	dataMIDG = pd.DataFrame(sc.textFile('%s%s'%(data_path,file_name)).map(lambda x: x.split(',')).map(lambda x: (x[4],1))
			.groupByKey().mapValues(len)
			.sortBy(lambda x:x[1],False).take(10))
	dataMIDG.to_csv('summary/count%s'%file_name,header=None,index=False)	
files  = sc.textFile('summary/*')
dat = files.map(lambda x: x.split(',')).collect()
dat = pd.DataFrame(dat,columns=['mID','counts'])
t = dat.sort(['counts'],ascending=False)
print t.iloc[:10]
t.to_csv('top10.csv',index=False)
sc.stop()

	
