import numpy as np

def flatMap_moments(T):
    stime = T[0]
    etime = T[1]
    moments = np.arange(stime, etime, step=1E6)
    return [((T[-1], t), np.array(T[2:-1]).flatten()) for t in moments]
def reduce_moments(x,y):
    return x+y
map_sorted = lambda (x): (x[0][1],(x[0][0],x[1]))
def map_moments(x):
    lst_moment = []
    for i in x[0]:
        lst_moment.append(i)
    for i in x[-1]:
        lst_moment.append(i)
    return lst_moment
