##############################################################################
#                            Library Import                                  #
##############################################################################

import pandas as pd
from pymongo import MongoClient
import os
from time import sleep

##############################################################################
#                            Working Directory                               #
##############################################################################

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


##############################################################################
#                            Instantiation                                   #
##############################################################################

db = MongoClient()['NSE']


##############################################################################
#                            Infinite Loop                                   #
##############################################################################

while 1:
    flist = os.listdir('data/finished')
    if not flist:
        sleep(60)
    for fnm in flist:
        dat = pd.read_csv('data/finished/'+fnm)
        os.remove('data/finished/'+fnm)
        prev = [i['Datetime'] for i in db['daily_price'].find({'Scrip': fnm.split('_')[0]},
                                      {'Datetime': 1, '_id': 0})]
        dat['Datetime'] = pd.to_datetime(dat.Datetime).dt.tz_localize(
            'US/Eastern').dt.tz_convert("UTC")
        dat['Scrip']=fnm.split('_')[0]
        dat = dat[~dat.Datetime.isin(prev)]
        db['daily_price'].insert_many(dat.to_dict(orient='record'))
