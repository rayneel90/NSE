from pymongo import MongoClient

conn = MongoClient()['NSE']
a = conn['price1min'].find({'Symbol': "YESBANK"},{'Datetime':1,"_id":0})
a=[i['Datetime'] for i in a]
