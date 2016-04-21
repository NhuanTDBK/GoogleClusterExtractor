import numpy as np

def flatMap_moments(T):
    stime = T[0]
    etime = T[1]
    moments = np.arange(stime,etime,step=1E6)
    return [((t,T[3]),T[2]) for t in moments]
def reduce_moments(x,y):
	return x+y
map_sorted = lambda (x): (x[0][1],(x[0][0],x[1]))
map_moments = lambda (x) :(x[0],x[1][0],x[1][1])
def retrieveData(sqlContext,sample_dat):
    sample_rdd = sqlContext.createDataFrame(sample_dat)
    sample_rdd.registerTempTable("metrics")
    cpu_val = sqlContext.sql("SELECT * from metrics")
    mapper = cpu_val.flatMap(flatMap_moments).reduceByKey(reduce_moments).map(map_sorted).sortByKey(True, 4).map(
        map_moments).collect()