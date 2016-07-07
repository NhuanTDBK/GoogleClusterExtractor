import pandas as pd
import os 

print "Loading schema"
schema = pd.read_csv("schema_te.csv",names=['col'])['col']
schema_tv = schema.values[1:]
print schema_tv
folder_path ='/home/ubuntu/data/google_cluster/task_events/'
jobID = 6176858948
results = []
for file_name in os.listdir(folder_path):
	#print "Reading %s"%file_nam
        dat = pd.read_csv('%s/%s'%(folder_path,file_name),names=schema_tv,compression='gzip')
	sample_dat = dat[dat.job_ID==jobID]
	if sample_dat.shape[0] != 0:
		print "Reading %s"%file_name
		sample_dat.to_csv('dataset/dat_%s'%file_name,index=None,header=None)
#results = pd.DataFrame(results).to_csv('result_%s'%jobID)
