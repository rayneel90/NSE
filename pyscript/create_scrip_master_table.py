"""
This tiny script creates the scrip master table from the back-up CSV or the
Pickle file (depending on user input).
"""
import pandas as pd
from pymongo import MongoClient
import pickle
from time import sleep
from tkinter.filedialog import askopenfilename


conn=MongoClient()['NSE']
while(1):
    filename = askopenfilename()  # type: str
    if filename.find('csv') > 0:
        df = pd.read_csv(filename,encoding='ANSI')  # type: pd.DataFrame
        dat = df.to_dict(orient='record')
        break
    elif filename.find('.pkl') > 0:
        with open(filename,'rb') as fil:
            dat = pickle.load(fil)
        break
    else:
        print('invalid file chosen')
        sleep(2)

conn['scrip_master'].drop()
conn['scrip_master'].insert_many(dat)



with open('data/pickle/scrip_master.pkl','wb') as fil:
    pickle.dump(dat,fil)