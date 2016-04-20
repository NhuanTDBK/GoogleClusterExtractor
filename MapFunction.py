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
