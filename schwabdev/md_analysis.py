
import pandas as pd
import numpy as np

root_dir = 'C:/Users/helpdesk/md/'
md_file = 'md_20241015 - Copy.log'

df = pd.read_json(root_dir + md_file,lines=True )
df =  df[~df['data'].isna()]
df1 = pd.json_normalize(df['data'])

# convert None to {}
for col in df1.columns:
    df1[col] = np.where(df1[col].isna(), {}, df1[col])
# so this will work
df1 = df1[df1[0].apply(lambda x : 'LEVELONE_EQUITIES' in x.values())]

# equity df
df2 = pd.json_normalize(df1[0], 'content', ['service','timestamp','command'])
# forward fill NaN
df2.ffill(inplace=True)

# convert timestamp
df2['time'] = pd.to_datetime(df2['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.strftime('%Y-%m-%d %H:%M:%S.%f')

# bad data, could be due to ffill
df_bad = df2[ (df2['1'] > df2['2']) |  (df2['3'] > df2['2']) | (df2['3'] < df2['1'] )]