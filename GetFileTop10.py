import os
import pandas as pd
topMID = pd.read_csv('top10MID.csv',names=['mID'])
folder = []
for file_name in os.listdir('summary'):
        sample_dat = pd.read_csv('summary/%s'%file_name,names=['mID','counts'])
        if(sample_dat[sample_dat.mID.isin(topMID.mID)].shape[0]!=0):
		folder.append(file_name.replace('count',''))
folder = pd.Series(folder).to_csv('topFile.csv',index=False)
