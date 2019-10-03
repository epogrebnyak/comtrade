import pandas as pd
import pathlib


doc = """
wheat	100199
seed	100111
wheat	100119
seed	100191
	
flour	110100
groat	110311
	
byproduct	110320
split	110429
split	110430
split	110811
byproduct	230230
byproduct	230310
	
gluten	110900
	
infant	190110
	
dough	190120
dough	190590
	
flakes	190410
flakes	190420
flakes	190430
	
biscuit	190531
biscuit	190532
	
pet food	230910
feed	230990
	
pasta	190211
pasta	190219
pasta	190220
pasta	190230
	
beer	220300
ethyl	220720
vodka	220860
"""

pairs = [x.split('\t') for x in doc.split('\n') if x.strip()]
GROUPS = {q[1]:q[0] for q in pairs}

FILLED_COLUMNS = ['Classification', 'Year', 'Period', 'Period Desc.', 'Aggregate Level',
       'Is Leaf Code', 'Trade Flow Code', 'Trade Flow', 'Reporter Code',
       'Reporter', 'Reporter ISO', 'Partner Code', 'Partner', 'Partner ISO',
       'Commodity Code', 'Commodity', 'Qty Unit Code', 'Qty Unit',
       'Alt Qty Unit', 'Netweight (kg)', 'Trade Value (US$)', 'Flag']
SHORT_COLUMNS = ['Year', 'Trade Flow', 'Reporter', 'Partner', 'Commodity Code', 
'Netweight (kg)', 'Trade Value (US$)', 'Commodity']
RENAME_DICT = {'Year': 'year',
               'Trade Flow': 'flow',
               'Reporter': 'reporter',
               'Partner': 'partner',  
               'Commodity Code': 'code',
               'Netweight (kg)': 'kg',
               'Trade Value (US$)': 'usd',                
               }
VERY_SHORT_COLUMNS = 'code musd mton price comm'.split()


def read(path):
    df = pd.read_csv(path, dtype={'Commodity Code': str})
    df = df.rename(columns=RENAME_DICT)
    df['musd'] = df.usd.divide(10**6).round(1)
    df['mton'] = df.kg.divide(10**6).round(1)
    df['price'] = (df.usd / df.kg).multiply(1000).round(1)
    df['comm'] = df['Commodity'].apply(lambda x: x[:35])
    return df


GRAIN_CODES_LONG = list(GROUPS.keys())
#    10
#    11,
#    19,
#    2203,
#    2207,
#    2208,
#    2302,
#    230310,
#    190120,
#    1902,
#    1904,
#    1905,    
#    2203,
#    2207,
#    2208,
#    230910,
#    230990,

# For research purposes, the 6-digit Harmonized Tariff System code is 230990 for preparations used in animal feeding. That code excludes dog or cat food sold at the retail level.


GRAIN_CODES_EXCLUDE = [
    1105,
    110813,
    220820
    ]


def select(df, include=GRAIN_CODES_LONG, exclude=GRAIN_CODES_EXCLUDE):   
   
    def startswith(df, code):
        code = str(code)
        return df.code.apply(lambda x: x.startswith(code))        
    
    index = pd.Series([False] * len(df))

    for gc in include:
        #print(gc, '\n')       
        ix = startswith(df, gc)
        index = index | ix
        #print(df[ix][VERY_SHORT_COLUMNS])
        
    for ex in exclude:
        ix = startswith(df, ex)
        index = index & ~ix
        
    return index    
        
def make_output(df, index):
    output = df[index][VERY_SHORT_COLUMNS]
    output = output.merge(df[['code', 'Commodity']], on='code')
    del output['comm']
    group_col = output.code.apply(lambda x: GROUPS.get(x))
    output.insert(0, column='groups', value=group_col)    
    return output        

SUM_DICT = dict(flakes=[190410, 190420, 190430])

def get_index(name):
    return list(map(str, SUM_DICT[name]))

def save(df, filename):
    if not pathlib.Path(filename).exists():
        df.to_excel(filename)
    
def pipe(country):
    import os
    df = read(os.path.join("data", f"{country}.csv"))    
    index = select(df, include=GRAIN_CODES_LONG, exclude=GRAIN_CODES_EXCLUDE)
    output = make_output(df, index)
    save(df=output, filename=f"{country}.xlsx")    
    return df, output
    

if __name__ == "__main__":        
    de0, de = pipe('germany')
    ru0, ru = pipe('russia')
    country='russia'
