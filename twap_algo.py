import pandas as pd
import numpy as np
import datetime 
import matplotlib

pd.options.mode.chained_assignment = None

replay_date = "2025-12-16"
symbol = "SPY"

start_epoch = int(datetime.datetime.strptime(f"{replay_date} 9:30:00", "%Y-%m-%d %H:%M:%S").timestamp()) * 1000
end_epoch = int(datetime.datetime.strptime(f"{replay_date} 16:00:00", "%Y-%m-%d %H:%M:%S").timestamp()) * 1000

md = {}

md_keys = ['ts','1','2','3','9']

df_orders = pd.DataFrame(columns=['enteredTime','parentid','orderId','symbol','side','price','quantity','status','closeTime','layer'])

# parse MD
df = pd.read_json(f'./md_{replay_date.replace("-","")}.log',lines=True)
df = pd.json_normalize(df['data'].explode())
df = df.join(df['content'].explode(), lsuffix='_left')
df1 = df[['timestamp']].reset_index(drop=True).join(pd.json_normalize(df['content']))
df1 = df1[['timestamp', 'key', '1','2','3','9']]
df1 = df1[df1['timestamp'].notnull()].reset_index(drop=True)

# ffill bid and ask
for col in ['1','2','9']:
    df1[col] = df1.groupby('key')[col].ffill()

# remove null last price
df1 = df1[df1['3'].notna()].reset_index(drop=True)

# clean up data by removing crossed quotes and last price outside of bid and ask
df2 = df1[(df1['1'] <= df1['2']) & (df1['3'] >= df1['1']) & (df1['3'] <= df1['2'])].reset_index(drop=True)
# 9:30 to 16:00 filter
df2 = df2[(df2['timestamp'] >= start_epoch) & (df2['timestamp'] <= end_epoch) & (df2['key'] == symbol)].reset_index(drop=True)
df2['datetime'] = (pd.to_datetime(df2['timestamp'],unit='ms')).dt.tz_localize('UTC').dt.tz_convert('America/New_York')
mds = df2.to_dict('records')

delta = 0.05
layer_spread = 0.10
num_layers = 10

orderid = 0
quantity = 10
current_volume = 0
max_volume_per_5_min = 100
strat = "BS"

pnl_list = []
raw_pnl_list = []

df_layers = pd.DataFrame([{'symbol': symbol, 'layer': i, 'layer_price': 0, 'allowance':1, 'parentid':"", 'buy_orderid':"", 'sell_orderid':""}  for i in range(-num_layers//2, num_layers//2 + 1)])
past_5_min = (start_epoch - 1) // 60000

for md in mds :
    current_min = md['timestamp'] // 60000
    current_volume = df_orders[(df_orders['closeTime'] >= past_5_min * 60000) & (df_orders['closeTime'] <= current_min * 60000) & (df_orders['status'] == 'FILLED')]['quantity'].sum()

    if (current_min % 5 == 0) and current_min > past_5_min:
        past_5_min = current_min

        if current_min != (start_epoch // 60000) :
            if current_volume < max_volume_per_5_min :
                orderid += 1
                df_orders.loc[len(df_orders)] = {'enteredTime': md['timestamp'],
                                            'parentid': str(parentid),
                                            'orderId': str(orderid),
                                            'symbol': symbol,
                                            'side': 'BUY',
                                            'price': md['3'],
                                            'quantity': max_volume_per_5_min - current_volume,
                                            'status': 'WORKING',
                                            'closeTime': None,
                                            'layer': closest_row['layer'].item()
                                            }

        # initialize layers
        init_price = md['3']
        df_layers['layer_price'] = [round(init_price + layer_spread * layer, 2) for layer in df_layers['layer']]

        # reset allowance
        df_layers['allowance'] = 1

        # clear orders
        df_layers['parentid'] = ""
        df_layers['buy_orderid'] = ""
        df_layers['sell_orderid'] = ""

        # cancel orders not filled during last 5-min interval
        for idx, order in df_orders[df_orders['status']=='WORKING'].iterrows():
            df_orders.at[idx, 'status'] = 'CANCELLED'
    
    if strat == "BS" and current_volume < max_volume_per_5_min:
        closest_row = df_layers.iloc[(df_layers['layer_price'] - md['3']).abs().argsort()[:1]]

        if closest_row['allowance'].item() == 1 : # no position at this layer_price, we can trade
            df_layers.loc[df_layers['layer'] == closest_row['layer'].item(), 'allowance'] = 0
            orderid += 1
            parentid = orderid
            df_layers.loc[df_layers['layer'] == closest_row['layer'].item(), 'parentid'] = str(parentid)

            orderid += 1
            df_orders.loc[len(df_orders)] = {'enteredTime': md['timestamp'],
                                        'parentid': str(parentid),
                                        'orderId': str(orderid),
                                        'symbol': symbol,
                                        'side': 'BUY',
                                        'price': md['3'] - delta,
                                        'quantity': quantity,
                                        'status': 'WORKING',
                                        'closeTime': None,
                                        'layer': closest_row['layer'].item()
                                        }
            df_layers.loc[df_layers['layer'] == closest_row['layer'].item(), 'buy_orderid'] = str(orderid)
            df_layers.loc[df_layers['layer'] == closest_row['layer'].item(), 'sell_orderid'] = ""
            
        elif closest_row['allowance'].item() == 0 and df_layers.loc[df_layers['layer'] == closest_row['layer'].item(), 'sell_orderid'].item() == "":
            # if buy order filled, then place sell order
            if df_orders[df_orders['orderId'] == closest_row['buy_orderid'].item()]['status'].item() == 'FILLED' :
                orderid += 1
                df_orders.loc[len(df_orders)] = {'enteredTime': md['timestamp'],
                                            'parentid': closest_row['parentid'].item(),
                                            'orderId': str(orderid),
                                            'symbol': symbol,
                                            'side': 'SELL',
                                            'price': md['3'] + delta,
                                            'quantity': quantity,
                                            'status': 'WORKING',
                                            'closeTime': None,
                                            'layer': closest_row['layer'].item()
                                            }
                df_layers.loc[df_layers['layer'] == closest_row['layer'].item(), 'sell_orderid'] = str(orderid)

        # check for fills                                       
        for idx, order in df_orders[df_orders['status']=='WORKING'].iterrows():
            if order['side'] == 'BUY' and md['3'] <= order['price']:
                df_orders.at[idx, 'status'] = 'FILLED'
                df_orders.at[idx, 'closeTime'] = md['timestamp']
            elif order['side'] == 'SELL' and md['3'] >= order['price']:
                df_orders.at[idx, 'status'] = 'FILLED'
                df_orders.at[idx, 'closeTime'] = md['timestamp']
        
        #print(df_layers)
        # check if both sides filled, then free up the layer
        df_layers['allowance'] = df_layers.apply(lambda row: 1 if set((row['buy_orderid'], row['sell_orderid'])).issubset(set(df_orders[df_orders['status']=='FILLED']['orderId'])) else row['allowance'],axis=1)

        # effective PnL calculation
        pnl = df_orders.apply(lambda row: row['price'] if row['side']=='SELL' and  row['status']=='FILLED' else ( -row['price'] if row['side']=='BUY' and row['status']=='FILLED' else (md['3'] if row['side']=='SELL' else -md['3'])), axis=1).sum()
        print(f"Current PnL: {round(pnl,2)}")
        pnl_list.append([md['timestamp'], round(pnl,2)])

        # PnL 
        df_fill = df_orders[df_orders['status']=='FILLED']
        df_fill.loc[:,'fill_count'] = df_fill.groupby('parentid')['parentid'].transform('count')
        raw_pnl = 2 * delta * (df_fill[df_fill['fill_count']==2].shape[0] //2)
        print(f"Realized PnL: {round(raw_pnl,2)}")
        raw_pnl_list.append([md['timestamp'], round(raw_pnl,2)])

# plot results
df_orders['datetime'] = (pd.to_datetime(df_orders['enteredTime'],unit='ms')).dt.tz_localize('UTC').dt.tz_convert('America/New_York')
df_orders['close_datetime'] = (pd.to_datetime(df_orders['closeTime'],unit='ms')).dt.tz_localize('UTC').dt.tz_convert('America/New_York')

df_pnl = pd.DataFrame(pnl_list, columns=['timestamp','pnl'])
df_pnl['datetime'] = (pd.to_datetime(df_pnl['timestamp'],unit='ms')).dt.tz_localize('UTC').dt.tz_convert('America/New_York')

df_pnl_raw = pd.DataFrame(raw_pnl_list, columns=['timestamp','pnl'])
df_pnl_raw['datetime'] = (pd.to_datetime(df_pnl_raw['timestamp'],unit='ms')).dt.tz_localize('UTC').dt.tz_convert('America/New_York')

ax = df2.plot(x='datetime', y='3', legend=False,figsize=(20, 12),color='cyan')
df_pnl.plot(x='datetime', y='pnl', secondary_y=True, ax=ax, legend=False, color='red')
df_pnl_raw.plot(x='datetime', y='pnl', secondary_y=True, ax=ax, legend=False, color='green')

df_orders[(df_orders['status'] == 'FILLED') &(df_orders['side'] == 'SELL')][['close_datetime','price']].plot(x='close_datetime',y='price', ax=ax,style='ro', markersize=5, marker='v', label='Sell' )
df_orders[(df_orders['status'] == 'FILLED') &(df_orders['side'] == 'BUY')][['close_datetime','price']].plot(x='close_datetime', y='price', ax=ax,style='go', markersize=5, marker='^',label='Buy' )
