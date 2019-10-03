from time import sleep
from comtrade import Endpoint, CommodityTrade, exporters, importers
import pandas as pd

import pytest

from urllib.parse import urlsplit, parse_qs

def test_endpoint():
    assert Endpoint(query_string='abc=1').url() == \
               'http://comtrade.un.org/api/get?abc=1'

def extract_query(url):
    return parse_qs(urlsplit(url).query)

def test_raw_query():
    q = CommodityTrade(reporter=276, partner=0, year=2018).raw_query_export()
    url = "http://comtrade.un.org/api/get?max=50000&type=C&freq=A&px=HS&ps=2018&r=276&p=0&rg=2&cc=AG6"
    a = extract_query(url)
    b = extract_query(q.url)
    assert a == b

def test_CommodityTrade_count():
    # German exports to the world in 2018
    de = CommodityTrade(reporter=276).get_export()
    assert de.count() == de.dataframe().shape[0]
    
def test_exporters():
    sleep(1)
    df = exporters(code=2814) # urea
    assert isinstance(df, pd.DataFrame)
 
@pytest.mark.skip # the test is symmetric to export
def test_importers():
    sleep(1)
    df = importers(code=2814) # urea
    assert isinstance(df, pd.DataFrame)
