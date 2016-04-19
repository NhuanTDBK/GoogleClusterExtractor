import numpy as np

def flatMap_moments(T):
    stime = T[0]
    etime = T[1]
    moments = np.arange(stime,etime,step=1E6)
    return [((T[2],t),T[3]) for t in moments]
reduce_moments = lambda (x,y) : x[1]+y[1]
map_moments = lambda (x) :(x[0][0],x[0][1],x[1])
