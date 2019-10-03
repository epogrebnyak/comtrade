"""Access UN Comtrade statistics from a python script.

Usage:
    df1 = country_export(reporter=276).dataframe('code') #all exports at HS 6-gigit level
    df2 = exporters_by_code(3102).dataframe() # Urea exports by country
    
Features:
   - requests_cache cache the calls, prevents overloading the API
   - 
    
Restrictions:
 - I looked with commodity trade, not services
 - year by default is 2018
 
Usage advice:
 - make a query at web interface, then check API for it    
 - exporter data may be more reliable that importer

API codumentation:
    https://comtrade.un.org/Data/doc/api/
    
"""    

from urllib.parse import urlencode, urlunsplit
from dataclasses import dataclass
import requests
import pandas as pd

import requests_cache

requests_cache.install_cache('comtrade_cache')

ENDPOINT = 'http', 'comtrade.un.org', '/api/get'


def make_url(query_string: str, endpoint=ENDPOINT):
    scheme, netloc, path = endpoint    
    return urlunsplit((scheme, netloc, path, query_string, ''))


@dataclass
class CommodityQuery:
    reporter: int
    partner: int
    year: int
    code: str
    
    def _raw(self, direction: str):
        return RawQuery(r=self.repoter,
                        p=self.partner,
                        ps=self.year,
                        rg=dict(exp=2, imp=1)[direction.lower()[:2]],
                        cc=self.code)

@dataclass
class CommodityExport:    
    def raw(self):
        return self._raw('export')


@dataclass
class CommodityImport:    
    def raw(self):
        return self._raw('import')


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


def select(*arg, **kwarg):
    return RawQuery(*arg, **kwarg).response()


def country_export(reporter, partner=0, year=2018):
    return select(reporter, partner, rg=2, ps=year)


def country_import(reporter, partner=0, year=2018):
    return select(reporter, partner, rg=1, ps=year)


def exporters_by_code(code, year=2018):
    return select(r='all', p=0, rg=2, cc=str(code))

    
COL_DICT = dict(yr='year',
     rgDesc = 'dir',
     rtTitle='reporter',
     ptTitle='partner',
     cmdCode='code', 
     NetWeight='tton',
     TradeValue='musd',
     cmdDescE='desc')    


@dataclass
class Response:
    dict: dict   
    
    @property
    def info(self):
        return self.dict['validation']
    
    def count(self):
        return self.info['count']['value']

    @property
    def dataset(self):
        return self.dict['dataset']
    
    def dataframe(self, col_dict=COL_DICT, index_key=None):
        df = pd.DataFrame(self.dataset)[col_dict.keys()] \
                 .rename(columns=COL_DICT) \
                 .sort_values('musd', ascending=False)
        if index_key:
            df = df.set_index(index_key)
        df['musd'] = df.musd.divide(10**6).round(1)
        df['tton'] = df.tton.divide(10**6).round(1)
        df['price'] = (df.musd / df.tton).multiply(1000).round(1)    
        return df
 
if __name__ == '__main__':
    
    # German exports to the world in 2018
    de = country_export(reporter=276, partner=0, year=2018)
    assert de.count() == de.dataframe().shape[0]

    # Firiliser exports by country
    amm =  exporters_by_code(code=2814).dataframe()
    nit = exporters_by_code(code=3102, year=2018).dataframe()
    pho = exporters_by_code(code=3103).dataframe()
    pot = exporters_by_code(code=3104).dataframe()
    mix = exporters_by_code(code=3105).dataframe()
    
    df = pd.concat([amm, nit, pho, pot, mix])
    df = df.query('tton > 0')
    df = df.groupby('code').sum()
    df['price'] = (df.musd / df.tton).multiply(1000).round(1)
    zf = df.price
    print (zf)
    
    for df in amm, nit, pho, pot, mix:
        code = df.code[0]
        print (code, df.desc[0])                
        del df['desc']
        del df['dir']
        del df['partner']
        del df['year']
        print(df.head(10))
        s = round(df.musd.sum() / 1000, 1)
        p = zf.loc[code,]
        t = round(s / p * 1000, 1)
        print("Totals:", t, s, p)
    
         
