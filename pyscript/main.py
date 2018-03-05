##############################################################################
#                            Import Files                                    #
##############################################################################

from alpha_vantage.timeseries import TimeSeries
import pandas as pd
from pymongo import MongoClient, DESCENDING, ASCENDING
import os
from datetime import datetime
import sys
##############################################################################
#                               Constant                                     #
##############################################################################

apikey = 'ASRQ194BU5XL5ZU4'
conn = MongoClient()['NSE']

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    OPS = sys.argv[1]
    print(OPS)
    print(type(OPS))
else:
    OPS = 'compact'


##############################################################################
#                               scrape Date                                  #
##############################################################################
"""
Logic: data_for_cron contains list of all Symbols and a flag whether scraped 
or not. Initially the flag is 0 for all. The code starts with list of all the
symbols, run a for loop for each ticker. If the flag for a ticker is 0, it 
temporarily converts it to 1 and collect the data and process. If the collection
fails at any stage, it converts the flag to 1. Once the for loop ends, it checks
if there is any ticker in data_for_cron for which the flag is still 0. If so, it 
runs the whole process again. However, if no data is collected in 3 such runs, it 
concludes that data is not available for remaining data points. In that case. or
in case data is collected for all tickers in data_for_cron, all the flags are 
again set to 0 and the whole process starts afresh. 
"""

print(datetime.now(),"Process Started")
ts = TimeSeries(key=apikey, output_format='pandas')  # initialize crawler
counter1 = 0  # This will keep track of the number of iterations passed
# without any data being collected

while 1:  # The outer loop.
    print(datetime.now(), "Process Started")
    records = conn['scrip_master'].find({}, {'_id': 0, 'Symbol': 1}).sort('Symbol', ASCENDING)
    tickers = [i['Symbol'] for i in records]
    counter2 = 0
    insert_count = 0
    for ticker in tickers:
        try:
            temp = conn['data_for_cron'].find_one_and_update({'Symbol': ticker},
                                                             {'$set': {'Done': 1}}
                                                             )
            if not temp['Done']:
                data, meta_data = ts.get_intraday(ticker,
                                                  interval='1min',
                                                  outputsize=OPS
                                                  )
                data.columns = [i.split(' ')[-1] for i in data.columns]
                data['Symbol'] = meta_data['2. Symbol']
                data['Datetime'] = pd.to_datetime(data.index)
                data['Datetime'] = data['Datetime'].dt.tz_localize('US/Eastern')\
                    .dt.tz_convert("UTC")
                try:
                    dt = conn['price1min'].find(
                        {'Symbol': ticker}
                    ).sort('Datetime', DESCENDING).limit(1)[0]['Datetime']
                except:
                    dt = data.Datetime[0].replace(year=1999)
                data = data[data.Datetime > dt]
                insert_count += data.shape[0]
                if data.shape[0]:
                    conn['price1min'].insert_many(data.to_dict(orient='record'))
                counter2 = counter2+1
        except:
            conn['data_for_cron'].update_one({'Symbol': ticker},
                                             {'$set': {'Done': 0}}
                                             )
    print(datetime.now(), "For Loop Over. Scraped {0} stocks and inserted\
     {1} rows".format(counter2,insert_count))
    if counter2:  # Means data has been collected in the for loop
        counter1 = 0
    else:
        counter1 = counter1+1
    if counter1 >= 3:
        temp = conn['data_for_cron'].find({'Done': 0}, {'_id': 0,
                                                        'Symbol': 1})
        failed = [i['Symbol'] for i in temp]
        conn['failure'].insert_one({'timestamp': datetime.utcnow(),
                                    'failed': failed})
        conn['data_for_crom'].delete_many({"Done":0})
        conn['data_for_cron'].update_many({},
                                          {'$set': {'Done': 0}})
        counter1 = 0
    print(datetime.now(), 'Value of Counter1 is ', counter1)

conn['data_for_cron'].update_one({'Symbol': ticker},
                                             {'$set': {'Done': 0}}
                                             )

