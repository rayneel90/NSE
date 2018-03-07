##############################################################################
#                            Import modules                                    #
##############################################################################

import pandas as pd
from pymongo import MongoClient, DESCENDING, ASCENDING
import os
import requests
import json
import sqlite3
from threading import Thread
from time import sleep
##############################################################################
#                               Constant                                     #
##############################################################################

apikey = 'ASRQ194BU5XL5ZU4'
#conn = MongoClient()['NSE']

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


##############################################################################
#             wrapper function for gathering data                            #
##############################################################################


def get_daily_data(scrip, api):
    url_temp = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&' \
               'symbol={0}&interval=1min&apikey={1}'
    proxy_dic = {
        'http': 'http://proxy.dcbbank.com:8080',
        'https': 'http://proxy.dcbbank.com:8080',
    }
    dat = requests.get(url_temp.format(scrip, api),
                       proxies=proxy_dic,
                       verify=False).text
    with open('data/daily/'+scrip+'.txt','w') as fil:
        fil.write(dat)
    dat = json.loads(dat)  # type:pd.DataFrame
    df = pd.DataFrame.from_dict(dat['Time Series (1min)'],orient='index')
    df['Symbol'] = scrip
    df['datetime'] = df.index
    df.columns = [i.split()[-1] for i in df.columns]
    with sqlite3.connect('NSE.db') as conn:
        df.to_sql('price_daily', con=conn, if_exists='append')

##############################################################################
#                              Create Threads                                #
##############################################################################


scrip_list=pd.read_csv('data/scrip_master.csv',encoding='ANSI')['NSE']
print(scrip_list)
itr_cnt=scrip_list.__len__()//20
for i in range(itr_cnt):
    scrips=scrip_list[i::itr_cnt]
    threads=[Thread(target=get_daily_data,args=(scrip,apikey)) for scrip in scrips]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    sleep(60)

