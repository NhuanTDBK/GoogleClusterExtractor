from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from MapFunction import *
import os
import numpy as np
sc = SparkContext()
sqlContext = SQLContext(sc)
import pandas as pd
files  = sc.textFile('summary/*')
dat = files.map(lambda x: x.split(',')).collect()
dat = pd.DataFrame(dat,columns=['mID','counts'])
print "Sorting"
t = dat.sort(['counts'],ascending=False)
print t.iloc[:10]
t.to_csv('top10.csv',index=False)

