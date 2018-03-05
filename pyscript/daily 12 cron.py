from pymongo import MongoClient,ASCENDING
import os
if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
conn = MongoClient()['NSE']
inPut = conn['scrip_master'].find().sort('Symbol',ASCENDING)
outPut = []
for i in inPut:
    i.update({'Done': 0})
    outPut.append(i)

conn['data_for_cron'].drop()
conn['data_for_cron'].insert_many(outPut)
