"""
Primary objective of this scrip is to find out the P/E and various other
ratios used in measuring the scope and performance of stocks. It first reads
the tables of ratios and temporarily store them in collection "ratios". It
also gets the link of each individual stock page in moneycontrol website and
store them in collection "name_ratios". Finally it scrapes the BSE, NSE and
ISIN code of each scrip, merge them with the link and ratio data and insert
them in scrip_master.


Collections
----------------------

1. ratios(Temporary)-
2. name_links (Temporary)-
3. scrip_master(Permanent)-
4. filed_links(Temporary)-

"""


import requests
import lxml
from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
import re
import winsound
import os
conn=MongoClient()['NSE']
url_template = "http://www.moneycontrol.com/stocks/marketinfo/pe/nse/homebody\
.php?indcode={0}&sortcode=0"
conn['ratios'].drop()
conn['name_links'].drop()
conn['scrip_master'].drop()
count = 0
for i in range(113):
    pg = requests.get(url_template.format(i))
    if pg.status_code != 200:
        print('url block faced')
        break
    df = pd.read_html(pg.content)[0]
    df = df[[1, 5, 6, 7, 8]]
    df.columns = ['Company', 'Consol_EPS', 'EPS', 'P2C_ratio', 'P2E_ratio']
    df = df[1:]
    if not df.shape[0]:
        continue
    beautified = BeautifulSoup(pg.content, 'lxml')
    table = beautified.find_all('table', {'class': 'tbldata14'})[0]
    links = table.find_all('a')[2:]
    name_links = []
    count += len(links)
    print(count)
    for link in links:
        name_links.append({'Company': link.string,
                           'Link': link['href']
                           })
    conn['ratios'].insert_many(df.to_dict(orient='record'))
    conn['name_links'].insert_many(name_links)
ptrn = re.compile(r'BSE: *(?P<BSE>[^( |)]*) *\| *NSE: *(?P<NSE>[^( |)]*) *\| *'
                  r'ISIN: *(?P<ISIN>[^( |)]*) *\| *SECTOR: (?P<Sector>[^( |)]+)')

while 1:
    com = conn['name_links'].find_one_and_delete({})
    if not com:
        break
    url = 'http://www.moneycontrol.com'+com['Link']
    pg = requests.get(url)
    try:
        beautified = BeautifulSoup(pg.text, 'lxml')
        txt = beautified.find('div', {'class': 'FL gry10'}).text
        rec = ptrn.match(txt).groupdict()
        rec.update(com)
        conn['scrip_master'].insert_one(rec)
    except:
        conn['failed_links'].insert_one(com)
winsound.Beep(500,1000)
while 1:
    com = conn['failed_links'].find_one_and_delete({})
    if not com:
        break
    com['Link'] = input('Enter the url for ' + com['Company'])
    url = 'http://www.moneycontrol.com'+com['Link']
    pg = requests.get(url)
    try:
        beautified = BeautifulSoup(pg.text, 'lxml')
        txt = beautified.find('div', {'class': 'FL gry10'}).text
        rec = ptrn.match(txt).groupdict()
        rec.update(com)
        conn['scrip_master'].insert_one(rec)
    except:
        conn['failed_links'].insert_one(com)

scrip_mstr = pd.DataFrame([i for i in conn['scrip_master'].find({},
                                                                {'_id': 0
                                                                 })])
scrip_mstr=scrip_mstr.drop_duplicates()
conn['scrip_master'].drop()
conn['scrip_master'].insert_many(scrip_mstr.to_dict(orient='record'))

conn['scrip_master_alt'].drop()
files=os.listdir("data/index_lists")
for i in files:
    dat = pd.read_csv('data/index_lists/'+i)
    dat.columns = [i.split(" ")[0] for i in dat.columns]
    if dat.isnull().sum().sum():   # Why is this done?
        break
    conn['index_master'].insert_one({'index': i.split('.')[0],
                                     'Symbols': dat['Symbol'].tolist()
                                     })
    conn['scrip_master_alt'].insert_many(dat.to_dict(orient='record'))
    print(i+' done')


scrip_mstr_alt = pd.DataFrame([i for i in
                               conn['scrip_master_alt'].find({},{'_id':0})])
scrip_mstr_alt = scrip_mstr_alt.drop_duplicates()
scrip_mstr_alt = scrip_mstr_alt[~scrip_mstr_alt.Symbol.isin(scrip_mstr.NSE)]
