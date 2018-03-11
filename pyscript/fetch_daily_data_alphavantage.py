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
conn=MongoClient()['NSE']

##############################################################################
#                               The Loop                                  #
##############################################################################

def collect_daily_data(ticker, key):
    i = 5
    while i:
        print (i)
        try:
            url_template = 'https://www.alphavantage.co/query?function=TIME_SERIES_' \
                           'INTRADAY&symbol={0}&interval=1min&apikey={1}'
            response=json.loads(requests.get(url_template.format(ticker, key)).text)
            dat=pd.DataFrame.from_dict(response['Time Series (1min)'],orient='index')
            dat.columns = [i.split()[1].capitalize() for i in dat.columns]
            dat['Datetime']=dat.index
            fnm=ticker+'_'+str(round(time()*100))+'.txt'
            dat.to_csv('data/raw/'+fnm,index=False)
            os.rename('data/raw/'+fnm,'data/finished/'+fnm)

            return 0
        except Exception as e:
            i -= 1
            if i:
                continue
            else:
                with open('log/'+ticker+'.txt', 'a+') as fil:
                    print(str(e))
                    fil.write('{0};{1};{2}\n'.format(datetime.datetime.now().__str__(),
                                                     type(e).__name__,
                                                     str(e)))


while 1:
    cursor = conn['scrip_master'].find({},
                                       {'_id': 0,
                                        'NSE': 1,
                                        }).sort('NSE', ASCENDING)
    scrip_list=[i['NSE'] for i in cursor]
    n_iter =len(scrip_list)//30 +1
    for i in range(n_iter):
        scrips=scrip_list[i::n_iter]
        print(len(scrips))
        threads = [threading.Thread(target=collect_daily_data, args=(ticker,apikey))
               for ticker in scrips]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        sleep(120)
    if datetime.datetime.now().time()>datetime.time(16,30,0):
        break