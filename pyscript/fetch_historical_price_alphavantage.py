"""
This Script will run everyday from 9am to 4pm. It will be scraping the daily
data in a looped manner and store it in daily_price collection. The scrips I
have ever traded will have a priority and smaller interval while the others
will be collected approximately once every 90 minutes.
"""

##############################################################################
#                            Import Files                                    #
##############################################################################

import requests
import pandas as pd
import os
import threading
import json
from time import time, sleep
import datetime
from pymongo import MongoClient, ASCENDING
##############################################################################
#                      Change Working Directory                              #
##############################################################################

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


##############################################################################
#                               Instantiation                                #
##############################################################################

apikey = 'ASRQ194BU5XL5ZU4'
db=MongoClient()['NSE']

##############################################################################
#                               The Loop                                  #
##############################################################################

print ('abc')
def collect_historical_data(ticker, key,retry):
    i = retry
    while i:
        print(i)
        try:
            url_template = 'https://www.alphavantage.co/query?function=TIME_' \
                           'SERIES_INTRADAY&symbol={0}&outputsize=full&interv' \
                           'al=1min&apikey={1}'
            response = json.loads(requests.get(url_template.format(ticker,
                                                                   key)
                                                ).text
                                  )
            dat = pd.DataFrame.from_dict(response['Time Series (1min)'],
                                         orient='index')
            dat.columns = [i.split()[1].capitalize() for i in dat.columns]
            dat['Datetime'] = dat.index
            prev = [i['Datetime'] for i in
                    db['daily_price'].find({'Scrip': ticker},
                                                {'Datetime': 1, '_id': 0})]
            last_record = [i['Datetime'] for i in
                           db['historical_price'].find({'Scrip': ticker},
                                                              {'_id': 0,
                                                            'Datetime': 1}
                                                           ).sort('Datetime',
                                                                  ASCENDING
                                                                  ).limit(1)]

            dat['Datetime'] = pd.to_datetime(dat.Datetime).dt.tz_localize(
                'US/Eastern').dt.tz_convert("UTC")
            dat['Scrip'] = ticker
            dat = dat[~dat.Datetime.isin(prev)]
            if last_record:
                dat = dat[dat.Datetime > last_record[0]]
            if dat.shape[0]:
                db['historical_price'].insert_many(dat.to_dict(orient='record'))

            return 0
        except Exception as e:
            i -= 1
            if i:
                continue
            else:
                with open('log/historical_'+ticker+'.txt', 'a+') as fil:
                    print(str(e))
                    fil.write(
                        '{0};{1};{2}\n'.format(
                            datetime.datetime.now().__str__(),
                            type(e).__name__,
                            str(e)
                        )
                    )


cursor = db['scrip_master'].find({},
                                   {'_id': 0,
                                    'NSE': 1,
                                    }).sort('NSE', ASCENDING)
scrip_list=[i['NSE'] for i in cursor]
n_iter = len(scrip_list) // 30 + 1
for i in range(n_iter):
    scrips = scrip_list[i::n_iter]
    print(len(scrips))
    threads = [
        threading.Thread(target=collect_historical_data,
                         args=(ticker, apikey,5))
        for ticker in scrips]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    sleep(120)

lst = list(db['daily_price'].find({},{'_id':0}))
db['historical_price'].insert_many(lst)
