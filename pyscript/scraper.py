import requests
import lxml
from bs4 import BeautifulSoup as bs
import pandas as pd
from pymongo import MongoClient
import re
conn=MongoClient()['NSE']
url_template = "http://www.moneycontrol.com/stocks/marketinfo/pe/nse/homebody.php?indcode={0}&sortcode=0"
for i in range(113):
    pg=requests.get(url_template.format(i))
    df = pd.read_html(pg.content)[0]
    df=df[[1,5,6,7,8]]
    df.columns=['Company','Consol_EPS','EPS','P2C_ratio','P2E_ratio']
    df=df[1:]
    beautified=bs(pg.content,'lxml')
    table = beautified.find_all('table',{'class':'tbldata14'})[0]
    links=table.find_all('a')[2:]
    name_links=[]
    for link in links:
        name_links.append({'Company': [link.string],
                           'Link': link['href']
                           })
    conn['ratios'].insert_many(df.to_dict(orient='record'))
    conn['name_links'].insert_many(name_links)






com=conn['name_links'].find_one_and_delete({})
url='http://www.moneycontrol.com'+com['Link']
pg = requests.get(url)
beautified = bs(pg.content, 'lxml')
txt=beautified.find('div',{'class':'FL gry10'}).text
ptrn=re.compile(r"BSE: *(?P<BSE>\d+) *\| *NSE: *(?P<NSE>\w+) *\| *ISIN: *(?P<ISIN>\w+) *\| *SECTOR: (?P<Sector>.+) *| *")
rec=ptrn.match(txt).groupdict()
ratio_data=conn['ratios'].find_one_and_delete({})
