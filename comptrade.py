from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qs
from dataclasses import dataclass
import requests
import pandas as pd

import requests_cache

requests_cache.install_cache('comtrade_cache')

ENDPOINT = 'http', 'comtrade.un.org', '/api/get'
API_DOCS = 'https://comtrade.un.org/Data/doc/api/'

def make_url(query_string: str, endpoint=ENDPOINT):
    scheme, netloc, path = endpoint    
    return urlunsplit((scheme, netloc, path, query_string, ''))

assert make_url('abc=1') == 'http://comtrade.un.org/api/get?abc=1'

url1 = "http://comtrade.un.org/api/get?max=50000&type=C&freq=A&px=HS&ps=2018&r=276&p=0&rg=2&cc=AG6"
qs1 = "max=500&type=C&freq=A&px=HS&ps=2018&r=all&p=0&rg=2&cc=3102"

def extract_query(url):
    return parse_qs(urlsplit(url).query)



@dataclass
class RawQuery:
    r: int # reporter
    p: int # partnter  
    rg: int # 1 (imports) and 2 (exports)
    ps: int = 2018
    freq: str = 'A'
    type: str = 'C' # C Commodities 
    px: str = 'HS'
    cc: str = 'AG6'
    max: int = 50_000 
    
    @property
    def query_string(self):
        return urlencode(self.__dict__)
    
    @property
    def url(self):
        return make_url(self.query_string)   
    
    def get_json(self):
        return requests.get(self.url).json()        
    
    def response(self):
        return Response(self.get_json())

def export_query_ag6(reporter, partner, year=2018):
    return RawQuery(reporter, partner, rg=2, ps=year)

def import_query_ag6(reporter, partner, year=2018):
    return RawQuery(reporter, partner, rg=1, ps=year)

def commodity_exporters(code, year=2018):
    return RawQuery(r='all', p=0, rg=2, cc=str(code))



    
COL_DICT = dict(yr='year',
     rgDesc = 'dir',
     rtTitle='reporter',
     ptTitle='partner',
     cmdCode='code', 
     NetWeight='kg',
     TradeValue='usd',
     cmdDescE='desc')    

@dataclass
class Response:
    dict: dict   
    
    @property
    def info(self):
        return r.dict['validation']
    
    def count(self):
        return self.info['count']['value']

    @property
    def dataset(self):
        return self.dict['dataset']
    
    def dataframe(self, col_dict = COL_DICT):
        df = pd.DataFrame(self.dataset)[col_dict.keys()] \
                 .set_index('cmdCode') \
                 .rename(columns=COL_DICT) \
                 .sort_values('usd', ascending=False)
        df['price'] = (df.usd/df.kg*1000).round(1)
        return df
    
"""
pfCode                                                 H5
yr                                                   2018
period                                               2018
periodDesc                                           2018
aggrLevel                                               6
IsLeaf                                                  1
rgCode                                                  2
rgDesc                                             Export
rtCode                                                276
rtTitle                                           Germany
rt3ISO                                                DEU
ptCode                                                  0
ptTitle                                             World
pt3ISO                                                WLD
ptCode2                                              None
ptTitle2                                                 
pt3ISO2                                                  
cstCode                                                  
cstDesc                                                  
motCode                                                  
motDesc                                                  
cmdCode                                            010121
cmdCode         Horses; live, pure-bred breeding animals
qtCode                                                  5
qtDesc                                    Number of items
qtAltCode                                            None
qtAltDesc                                                
TradeQuantity                                        1308
AltQuantity                                          None
NetWeight                                          631468
GrossWeight                                          None
TradeValue                                       55011890
CIFValue                                             None
FOBValue                                             None
estCode                                                 0
Name: 0, dtype: object
"""    
    
    

#German exports to the world in 2018
q = export_query_ag6(reporter=276, partner=0, year=2018)
r = q.response()
assert r.count() == 5137

ur = commodity_exporters(code=3102, year=2018).response().dataframe()

a = extract_query(url1)
b = extract_query(q.url)
assert a == b

