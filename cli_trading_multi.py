import os
import sys
home_dir = 'C:/git_home/'
#home_dir = '/storage/emulated/0/'
sys.path.insert(1,home_dir + 'schwab_phone')
import credentials_sq
import order

import zmq
import json
from time import strftime, localtime
import pandas_ta as ta
import pandas as pd
import numpy as np
import requests
import base64
import json
from os import system
import platform
import time
import datetime
from datetime import datetime as dt, time as dt_time
import pytz
import importlib
import pathlib
import smtplib
import threading
from decimal import Decimal

from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

import logging

import glob

def trend_get_bounds () :
    md_path = "C:/Users/helpdesk/md/"
    files =  glob.glob(md_path + 'md_202*.log')
    df_14 = pd.DataFrame()

    for i in files[-15:-1]:
        print(i)
        df = pd.read_json(i,lines=True)
        df = pd.json_normalize(df['data'].explode())
        df = df.join(df['content'].explode(), lsuffix='_left')

        df1 = df[['timestamp']].reset_index(drop=True).join(pd.json_normalize(df['content']))
        df1 = df1[['timestamp', 'key', '1','2','3']]
        df1 = df1[df1['timestamp'].notnull()].reset_index(drop=True)

        # ffill bid and ask
        for col in ['1','2']:
            df1[col] = df1.groupby('key')[col].ffill()

        # remove null last price
        df1 = df1[df1['3'].notna()].reset_index(drop=True)

        # clean up data by removing crossed quotes and last price outside of bid and ask
        df2 = df1[(df1['1'] <= df1['2']) & (df1['3'] >= df1['1']) & (df1['3'] <= df1['2'])].reset_index(drop=True)

        df2['datetime'] = (pd.to_datetime(df2['timestamp'],unit='ms')).dt.tz_localize('UTC').dt.tz_convert('America/New_York')

        # convert to 1-minute candlestick
        df3 = df2.set_index('datetime').groupby('key').resample('1min').agg({'3':['first','last']}).reset_index()
        df3.columns = ['key', 'datetime', 'open', 'close']

        # select open and close
        time_open = "09:30:00"
        time_close = "15:59:00"
        df3 = df3.set_index('datetime')
        df3 = df3.between_time(time_open,time_close).reset_index()
        # add new column market_open
        df3['market_open'] = df3.groupby('key')['open'].transform('first')
        df_14 = pd.concat([df_14, df3],ignore_index=True)

    df_14['move'] = abs((df_14['close'] - df_14['market_open']) / df_14['market_open'])
    df_14['time'] = df_14['datetime'].dt.time
    df_14['date'] = df_14['datetime'].dt.date

    dict_prev_close = df_14.groupby('key').tail(1)[['key','close']].set_index('key')['close'].to_dict()
    df_14 = df_14.groupby(['key','time']).agg({'move':'mean'}).reset_index()

    return df_14, dict_prev_close


def calc_rsi (price, period=14, avg=[], rsi_ahead=[40,60], rsi_target=[30,70],pnl=0.1):
    limit = [0,10_000]
    price_diff = price.tail(period+1).diff().tail(period)
    gain = np.where(price_diff >= 0, price_diff, 0)
    loss = np.where(price_diff < 0, -1 * price_diff, 0)

    gain_1 =  gain[:-1]
    loss_1 =  loss[:-1]

    avg_gain_1 = gain_1.sum() / period
    avg_loss_1 = loss_1.sum() / period

    if len(avg) == 0 :
        avg_gain = gain.sum() / period
        avg_loss = loss.sum() / period
    else : 
        avg_gain = (avg[0] * (period - 1) + gain[-1]) / period
        avg_loss = (avg[1] * (period - 1) + loss[-1]) / period
    
    if avg_loss != 0 :
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
    else:
        rsi = 100
    
    if rsi >= rsi_ahead[1] and rsi < rsi_target[1] :
        limit[1] = round(price.tail(1).values[0] + (rsi_target[1] * avg_loss * period / (100 - rsi_target[1]) - avg_gain * (period-1)),2)
    elif rsi >=  rsi_target[1] :
        limit[1] = round((price.tail(1).values[0] + pnl /10) ,2)
        

    if rsi <= rsi_ahead[0] and rsi > rsi_target[0] :
        limit[0] = round(price.tail(1).values[0] - (period * avg_gain * (100 -rsi_target[0]) / rsi_target[0] - avg_loss * (period-1)), 2 )
    elif rsi <= rsi_target[0]:
        limit[0] = round((price.tail(1).values[0] - pnl/10) , 2 )

    avg = [avg_gain_1, avg_loss_1]

    return rsi, avg, limit

def creation_date(path_to_file):
    """
    Try to get the date that a file was created, falling back to when it was
    last modified if that isn't possible.
    See http://stackoverflow.com/a/39501288/1709587 for explanation.
    """
    if platform.system() == 'Windows':
        return os.path.getmtime(path_to_file)
    else:
        stat = os.stat(path_to_file)
        try:
            #return stat.st_birthtime
            return stat.st_mtime
        except AttributeError:
            # We're probably on Linux. No easy way to get creation dates here,
            # so we'll settle for when its content was last modified.
            return stat.st_mtime

def getAccessToken(grant_type, code):
    headers = {
        'Authorization': f'Basic {base64.b64encode(bytes(f"{appKey}:{appSecret}", "utf-8")).decode("utf-8")}',
        'Content-Type': 'application/x-www-form-urlencoded'}
    
    if grant_type == 'authorization_code':
        data = {'grant_type': 'authorization_code', 'code': code,'redirect_uri': callbackUrl}
        response = requests.post('https://api.schwabapi.com/v1/oauth/token', headers=headers, data=data)
        if response.ok :
            access_token_issued = datetime.datetime.now().isoformat()
            toWrite = {"access_token_issued": access_token_issued, "token_dictionary": response.json()}
            print(toWrite)
            with open(tokenFile, 'w') as f:
                json.dump(toWrite, f, ensure_ascii=False, indent=4)
                #print(response.json())
        else:
            print('retry refreshtoken')

        return response.json()

    elif grant_type == 'refresh_token':
        with open (tokenFile, 'r') as f :
            d = json.load(f)
            if (datetime.datetime.now() - datetime.datetime.fromisoformat(d.get("access_token_issued"))).seconds < (1800 - 61):
                return d['token_dictionary']
            else :
                data = {'grant_type': 'refresh_token', 'refresh_token': code}  # refreshes the access token
                for i in range(3):
                    response = requests.post('https://api.schwabapi.com/v1/oauth/token', headers=headers, data=data)
                    #print(response.json())
                    if response.ok :
                        toWrite = {"access_token_issued": datetime.datetime.now().isoformat(), "token_dictionary": response.json()}
                        with open(tokenFile, 'w') as f:
                            json.dump(toWrite, f, ensure_ascii=False, indent=4)
                        #print(response.json())
                        break
                    else:
                        print(f"Could not get new access token ({i+1} of 3).")
                        time.sleep(0.3)
                return response.json()        
    else:
        print("Invalid grant type")
        return None


def getRefreshToken() :
    authUrl = f'https://api.schwabapi.com/v1/oauth/authorize?client_id={appKey}&redirect_uri={callbackUrl}'
    print(f'Click to authenticate: {authUrl}')
    returnedLink = input('Paste the redirect URL here: ')
    code = f"{returnedLink[returnedLink.index('code=')+5:returnedLink.index('%40')]}@"

    # get refreshToken, we will use it everytime to get accessToken
    tokens = getAccessToken('authorization_code',code)
    refreshToken = tokens['refresh_token']
    print('refreshToken: ', refreshToken )
    return refreshToken

def get_account_number() :
    tokens =  getAccessToken('refresh_token', refreshToken)
    #print('tokens:',tokens)
    accessToken = tokens['access_token']
    response = requests.get(f'{baseUrl}/accounts/accountNumbers', headers={'Authorization': f'Bearer {accessToken}'})

    print('accountNumber:', response.json())
    #send_email(response.json())
    return response.json()

def get_positions() :
    tokens =  getAccessToken('refresh_token', refreshToken)
    #print('tokens:',tokens)
    accessToken = tokens['access_token']
    response = requests.get(f'{baseUrl}/accounts/{accountHash}?fields=positions', headers={'Authorization': f'Bearer {accessToken}'})
    res = response.json()
    list_pos = res['securitiesAccount']['positions']
    for p in list_pos:
        print(p['instrument']['symbol'], p['longQuantity'])
    #print('accountInfo:', response.json())
    #send_email(response.json())
    return response.json()

def _place_order(symbol,side,qty, price, session):
    order_details = order.equityOrder(ordType='LIMIT', side=side, qty=qty, symbol=symbol,price=price,session=session)
    tokens =  getAccessToken('refresh_token', refreshToken)
    #print('tokens:',tokens)
    accessToken = tokens['access_token']

    response = requests.post(f'{baseUrl}/accounts/{accountHash}/orders',
                            headers={"Accept": "application/json", 'Authorization': f'Bearer {accessToken}',"Content-Type": "application/json"},
                            json=order_details,
                            timeout=2)
    print(response.text)
    logging.info(f'{order_details}')
    logging.info(f'{response.headers}')
    
    return get_orderid(response.headers)
    
    
def place_buy_order(symbol,qty,limit_price,session):
    #limit_price = float(input("Please enter your limit price for your BUY order: "))
    return _place_order(symbol,'BUY', qty, limit_price,session)

def place_sell_order(symbol,qty,limit_price,session) :
    #limit_price = float(input("Please enter your limit price for your SELL order: "))
    return _place_order(symbol,'SELL', qty, limit_price,session)

def thread_getOrders(t,stop) :
    pub_socket = context.socket(zmq.PUB)
    #server_socket.bind('tcp://*:5559')
    pub_socket.connect('tcp://localhost:5559')
    while True:
        if stop():
            print("Exiting loop.")
            break
        try :
            order_status = getOrderStatus()
        except Exception as e:
            print(str(e))
        #print(order_status)
        pub_socket.send_json(json.dumps(order_status))
        df_tmp = pd.DataFrame(symbol_list)
        df_tmp = df_tmp.drop('avg')
        print(df_tmp)
        time.sleep(t)
    print("thread_getOrders, signing off")

def getOrderStatus_1():
    tokens =  getAccessToken('refresh_token', refreshToken)
    accessToken = tokens['access_token']
    #toEnteredTime = (datetime.datetime.now() + datetime.timedelta(days=2)).strftime('%Y-%m-%d') + 'T00:00:00.000Z'
    fromEnteredTime = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=20)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    toEnteredTime = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    print(fromEnteredTime,toEnteredTime)
    response = requests.get(f'{baseUrl}/accounts/{accountHash}/orders',
                        headers={"Accept": "application/json", 'Authorization': f'Bearer {accessToken}'},
                        params={'fromEnteredTime': fromEnteredTime,'toEnteredTime': toEnteredTime})
    orders = response.json()
    print(orders)
    if len(orders) > 0 :
        df = pd.DataFrame(orders)
        df['side'] = df['orderLegCollection'].str[0].str['instruction']
        cols = ['orderId','status','side','quantity','price']
        print(df[cols])
        orderId = str(df['orderId'][0])
        status = df['status'][0]
        orderStatus = {"type":"orderStatus","orderId": orderId,"status": status}
    else :
        orderStatus = {"type":"orderStatus","orderId": "","status":""}     
    
    return orderStatus

def getOrderStatus():
    tokens =  getAccessToken('refresh_token', refreshToken)
    accessToken = tokens['access_token']
    toEnteredTime = (datetime.datetime.now() + datetime.timedelta(days=2)).strftime('%Y-%m-%d') + 'T00:00:00.000Z'
    fromEnteredTime = datetime.datetime.now().strftime('%Y-%m-%d') + 'T00:00:00.000Z'
    #print(fromEnteredTime,toEnteredTime)
    response = requests.get(f'{baseUrl}/accounts/{accountHash}/orders',
                        headers={"Accept": "application/json", 'Authorization': f'Bearer {accessToken}'},
                        params={'fromEnteredTime': fromEnteredTime,'toEnteredTime': toEnteredTime})
    #print(response.text)                       
    orders = response.json()
    orderList=[]
    if len(orders) > 0 :
        for order in orders:
            dict1 = {}
            #print(order)
            if order['orderStrategyType'] == 'TRIGGER' :
                dict1['status'] = order['childOrderStrategies'][0]['status']
                dict1['orderId'] = order['childOrderStrategies'][0]['orderId']
                dict1['price'] = order['childOrderStrategies'][0]['price']
                dict1['side'] = order['childOrderStrategies'][0]['orderLegCollection'][0]['instruction']
                dict1['symbol'] = order['childOrderStrategies'][0]['orderLegCollection'][0]['instrument']['symbol']
                if 'orderActivityCollection' in order['childOrderStrategies'][0] : 
                    dict1['fill2'] = order['childOrderStrategies'][0]['orderActivityCollection'][0]['executionLegs'][0]['price']
                    dict1['time2'] = order['childOrderStrategies'][0]['orderActivityCollection'][0]['executionLegs'][0]['time']
            else :
                dict1['status'] = order['status']
                dict1['orderId'] = order['orderId']
                if 'price' in order:
                    dict1['price'] = order['price']
                else :
                    dict1['price'] = 'NA'
                dict1['side'] = order['orderLegCollection'][0]['instruction'] 
                dict1['symbol'] = order['orderLegCollection'][0]['instrument']['symbol']

            if 'orderActivityCollection' in order :
                dict1['fill1'] = order['orderActivityCollection'][0]['executionLegs'][0]['price']
                dict1['time1'] = order['orderActivityCollection'][0]['executionLegs'][0]['time']

            dict1['parent_orderid'] = str(order['orderId'])

            if 'statusDescription' in order :
                dict1['statusDescription'] = order['statusDescription']

            orderList.append(dict1)
        df = pd.DataFrame(orderList)  
        if 'fill2' in df.columns :
            df['pnl'] = np.where((df['fill2'] > 0) & (df['fill1'] > 0), (df['fill2'] - df['fill1']).abs(), 0 )
            df['sum'] = df['pnl'].iloc[::-1].cumsum().iloc[::-1]
        print(df)

        df1 = df.groupby('symbol').head(1).reset_index()
        orderStatus = df1[['symbol','parent_orderid','status','orderId']].to_dict('list')

        #print(orderStatus)
        #orderStatus = orderList[0]
        #orderStatus = df1[df1['status'].isin(['FILLED'])]['symbol'].to_list()
    else:
        df = pd.DataFrame(columns=['status','orderId','price','side','symbol','parent_orderid'])
        orderStatus = {"type":"orderStatus","orderId": "","status":"","parent_orderid":""}    

    return orderStatus

def getOrderStatus_for_cancel():
    tokens =  getAccessToken('refresh_token', refreshToken)
    accessToken = tokens['access_token']
    toEnteredTime = (datetime.datetime.now() + datetime.timedelta(days=2)).strftime('%Y-%m-%d') + 'T00:00:00.000Z'
    fromEnteredTime = datetime.datetime.now().strftime('%Y-%m-%d') + 'T00:00:00.000Z'
    #print(fromEnteredTime,toEnteredTime)
    response = requests.get(f'{baseUrl}/accounts/{accountHash}/orders',
                        headers={"Accept": "application/json", 'Authorization': f'Bearer {accessToken}'},
                        params={'fromEnteredTime': fromEnteredTime,'toEnteredTime': toEnteredTime})
    #print(response.text)                       
    orders = response.json()
    orderList=[]
    if len(orders) > 0 :
        for order in orders:
            dict1 = {}
            #print(order)
            if order['orderStrategyType'] == 'TRIGGER' :
                dict1['status'] = order['childOrderStrategies'][0]['status']
                dict1['orderId'] = order['childOrderStrategies'][0]['orderId']
                dict1['price'] = order['childOrderStrategies'][0]['price']
                dict1['side'] = order['childOrderStrategies'][0]['orderLegCollection'][0]['instruction']
                dict1['symbol'] = order['childOrderStrategies'][0]['orderLegCollection'][0]['instrument']['symbol']
                if 'orderActivityCollection' in order['childOrderStrategies'][0] : 
                    dict1['fill2'] = order['childOrderStrategies'][0]['orderActivityCollection'][0]['executionLegs'][0]['price']
                    dict1['time2'] = order['childOrderStrategies'][0]['orderActivityCollection'][0]['executionLegs'][0]['time']
            else :
                dict1['status'] = order['status']
                dict1['orderId'] = order['orderId']
                if 'price' in order:
                    dict1['price'] = order['price']
                else :
                    dict1['price'] = 'NA'
                dict1['side'] = order['orderLegCollection'][0]['instruction'] 
                dict1['symbol'] = order['orderLegCollection'][0]['instrument']['symbol']

            if 'orderActivityCollection' in order :
                dict1['fill1'] = order['orderActivityCollection'][0]['executionLegs'][0]['price']
                dict1['time1'] = order['orderActivityCollection'][0]['executionLegs'][0]['time']

            dict1['parent_orderid'] = str(order['orderId'])

            if 'statusDescription' in order :
                dict1['statusDescription'] = order['statusDescription']

            orderList.append(dict1)
        df = pd.DataFrame(orderList)  
        if 'fill2' in df.columns :
            df['pnl'] = np.where((df['fill2'] > 0) & (df['fill1'] > 0), (df['fill2'] - df['fill1']).abs(), 0 )
            df['sum'] = df['pnl'].iloc[::-1].cumsum().iloc[::-1]
        print(df)
    else:
        df = pd.DataFrame(columns=['status','orderId','price','side','symbol','parent_orderid'])  

    return df

def cancel_all_order(symbol='ALL'):
    tokens =  getAccessToken('refresh_token', refreshToken)
    accessToken = tokens['access_token']

    df = getOrderStatus_for_cancel()
    #print(df)
    list_close_status = ['FILLED','CANCELED','REPLACED','REJECTED'] 

    if symbol == 'ALL' : 
        list_ids = df[~df['status'].isin(list_close_status)]['parent_orderid'].to_list()
    else :
        list_ids = df[~df['status'].isin(list_close_status) & (df['symbol'] == symbol)]['parent_orderid'].to_list()

    if len(list_ids) > 0 :
        for i in list_ids:
                response = requests.delete(f'{baseUrl}/accounts/{accountHash}/orders/{i}',
                            headers={'Authorization': f'Bearer {accessToken}'},
                            timeout=2)
                print(i,response.text)
    else :
        print('No open orders to cancel')

def getOrders():
    tokens =  getAccessToken('refresh_token', refreshToken)
    accessToken = tokens['access_token']
    #toEnteredTime = (datetime.datetime.now() + datetime.timedelta(days=2)).strftime('%Y-%m-%d') + 'T00:00:00.000Z'
    toEnteredTime = (datetime.datetime.now() + datetime.timedelta(minutes=2)).strftime('%Y-%m-%d') + 'T00:00:00.000Z'
    fromEnteredTime = datetime.datetime.now().strftime('%Y-%m-%d') + 'T00:00:00.000Z'
    #print(fromEnteredTime,toEnteredTime)
    response = requests.get(f'{baseUrl}/accounts/{accountHash}/orders',
                        headers={"Accept": "application/json", 'Authorization': f'Bearer {accessToken}'},
                        params={'fromEnteredTime': fromEnteredTime,'toEnteredTime': toEnteredTime})
    #print(response.text)        
                
    df = pd.DataFrame(response.json())
    df['side'] = df['orderLegCollection'].str[0].str['instruction']
    #print(df['side'])

    if len(df) > 0:
        for col in ['enteredTime','closeTime']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col],utc=True).dt.tz_convert('America/New_York').dt.strftime('%H:%M:%S')
            else :
                df[col] = ''

        if 'statusDescription' not in df.columns:
            df['statusDescription'] = ''

        df['statusDescription'] = df['statusDescription'].str[:20]

        cols = ['status','side','price','session','duration','orderType','quantity','filledQuantity','remainingQuantity','orderId','cancelable','editable','enteredTime','closeTime','tag','accountNumber','statusDescription']
        #df.to_csv('orders.csv')
        for col in cols:
            if col not in df.columns:
                df[col] = ''
        #print(df[cols][df['status'] != 'REPLACED'][['status','side','price','enteredTime']].sort_values(['status','enteredTime'],ascending=False).reset_index(drop=True))
        df = df[cols][~df['status'].isin(['REPLACED'])][['status','side','price','enteredTime','orderId']].sort_values(['status','enteredTime'],ascending=False).reset_index(drop=True)
        #print(df[cols][df['status'] != 'REPLACED'][['status','side','price','enteredTime']].sort_values(['status','enteredTime'],ascending=False).reset_index(drop=True))
        print(df)
        return df
    else :
        print('No orders placed today.')
        return pd.DataFrame(columns=cols)

def cancel_order():
    tokens =  getAccessToken('refresh_token', refreshToken)
    accessToken = tokens['access_token']
    df = getOrders()

    oi = input("Please enter order index to CANCEL: ")
    if int(oi) in df.index:
        orderId = df.iloc[int(oi)]['orderId']

        response = requests.delete(f'{baseUrl}/accounts/{accountHash}/orders/{orderId}',
                            headers={'Authorization': f'Bearer {accessToken}'},
                            timeout=2)
        print(response.text)
    else:
        print(f'index {oi} not in df.index')

def _place_oto_order(symbol,side, qty, order_input):
    price = Decimal(order_input[0])
    delta = Decimal(order_input[1])

    order_details = order.otoOrder(ordType='LIMIT', side=side, qty=qty, symbol=symbol,price=float(price),delta=float(delta))
    tokens =  getAccessToken('refresh_token', refreshToken)
    #print('tokens:',tokens)
    accessToken = tokens['access_token']

    response = requests.post(f'{baseUrl}/accounts/{accountHash}/orders',
                            headers={"Accept": "application/json", 'Authorization': f'Bearer {accessToken}',"Content-Type": "application/json"},
                            json=order_details,
                            timeout=2)
    
    logging.info(f'{order_details}')
    logging.info(f'{response.headers}')

    return get_orderid(response.headers)
    
def place_buy_oto_order(symbol, qty, order_input):
    return _place_oto_order(symbol,'BUY', qty, order_input)

def place_sell_oto_order(symbol,qty, order_input):
    return _place_oto_order(symbol,'SELL',qty, order_input)

def _place_layered_oto_order(symbol, side, order_input):
    init_price = Decimal(order_input[0])
    increment = Decimal(order_input[1])
    qty = int(order_input[2])
    num_orders = int(order_input[3])
    spread = Decimal(order_input[4])

    for x in range (num_orders):
        if side == 'BUY':
            price = init_price - (x*spread)
        elif side == 'SELL':
            price = init_price + (x*spread)
        delta = increment + (spread * Decimal(0.5) * x)
        _place_oto_order(symbol, side, qty,[round(price,2),round(delta,2)])

def place_layered_buy_oto_order(symbol, layer_input):
    return _place_layered_oto_order(symbol,'BUY', layer_input)

def place_layered_sell_oto_order(symbol, layer_input):
    return _place_layered_oto_order(symbol,'SELL', layer_input)

def get_orderid(response_header):
    if 'Location' in response_header :
        orderId = response_header['Location'].split('/')[-1]
    else:
        orderId = 'error'
    return orderId

def _place_layered_order(side, order_input,session):
    symbol = order_input[0]
    init_price = Decimal(order_input[1])
    increment = Decimal(order_input[2])
    num_orders = int(order_input[3])
    for x in range (num_orders):
        if side == 'BUY':
            price = init_price - (x*increment)
        elif side == 'SELL':
            price = init_price + (x*increment)
        _place_order(symbol,side, symbol_list[symbol]['qty'],float(price),session)

def place_layered_buy_order(layer_input,session):
    return _place_layered_order('BUY', layer_input,session)

def place_layered_sell_order(layer_input,session):
    return _place_layered_order('SELL', layer_input,session)

def send_email(body):

    user = "sq17@hotmail.com"
    passwd = "Sesuriv001$"
    subject = "Access Token"
    
    msg = MIMEMultipart()
    msg['From'] = user
    msg['To'] = user
    msg['Subject'] = subject

    part1 = MIMEText(body,'html')
    msg.attach(part1)

    rcpt = [user]

    smtp_srv = "smtp-mail.outlook.com"
    smtp = smtplib.SMTP(smtp_srv,587)
    smtp.ehlo()
    smtp.starttls()
    smtp.ehlo()
    smtp.login(user, passwd)
    smtp.sendmail(msg['From'],rcpt,'test', msg.as_string())
    smtp.quit()

logger = logging.getLogger(__name__)
today = datetime.datetime.today().strftime('%Y%m%d')
logging.basicConfig(filename=f'C:/Users/helpdesk/md/myapp_{today}.log', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

appKey = credentials_sq.appKey
appSecret = credentials_sq.appSecret
callbackUrl = credentials_sq.callbackUrl
accountNumber = credentials_sq.accountNumber

baseUrl = 'https://api.schwabapi.com/trader/v1/'
mktUrl = 'https://api.schwabapi.com/marketdata/v1'


tokenFile = home_dir + 'schwab_phone/token.json'
print(tokenFile)

# trend: get bounds:
#df_bounds, prev_close_dict = trend_get_bounds()
#print(prev_close_dict)

# controllable vi send_zmq
symbol_list = {
    #'TQQQ' :    {'qty' : 5, 'price_pnl' : 0.35, 'rsi':['-30','200'], 'avg':[], 'allowance':1},
    #'QQQ' :     {'qty' : 1, 'price_pnl' : 0.70, 'rsi':['10','200'], 'avg':[], 'allowance':0},
    #'IWM' :     {'qty' : 2, 'price_pnl' : 0.25, 'rsi':['-30','200'], 'avg':[], 'allowance':0},
    'JEPQ' :     {'qty' : 1, 'layer_spread':0.05,'layer':10,'price_pnl' : 0.10, 'rsi':['15','200'], 'avg':[], 'allowance':0},
    'TLT' :     {'qty' : 1, 'layer_spread':0.08,'layer':10,'price_pnl' : 0.25, 'rsi':['15','200'], 'avg':[], 'allowance':0},
    'XLV' :     {'qty' : 1, 'layer_spread':0.08,'layer':8,'price_pnl' : 0.25, 'rsi':['15','200'], 'avg':[], 'allowance':0},
    'XLP' :     {'qty' : 1, 'layer_spread':0.05,'layer':10, 'price_pnl' : 0.20, 'rsi':['15','200'], 'avg':[], 'allowance':0},
    'XLE' :     {'qty' : 1, 'layer_spread':0.05,'layer':10, 'price_pnl' : 0.16, 'rsi':['15','200'], 'avg':[], 'allowance':0},
    'XLRE' :     {'qty' : 1,'layer_spread':0.05,'layer':10, 'price_pnl' : 0.20, 'rsi':['15','200'], 'avg':[], 'allowance':0},
    'SPY' :     {'qty' : 1,'layer_spread':0.10,'layer':1, 'price_pnl' : 0.85, 'rsi':['15','200'], 'avg':[], 'allowance':0}
}

#df_symbol_list = pd.DataFrame(symbol_list)

allowance = 0
#rsi_low, rsi_high = input("Please enter RSI range (low,high): ").split(",")
#rsi_low, rsi_high = ['10','200']
session = 'REGULAR'
orderid = '0000'

#send_email(str(symbol_list) + '\n' + 'allowance: ' + str(allowance) + '\n' + 'session: ' + session)

if (not os.path.isfile(tokenFile)) or ((int(time.time()) - creation_date(tokenFile)) > 6 * 24 * 3600) :
    # get initial token
    refreshToken = getRefreshToken()
else: 
    with open(tokenFile) as f:
        tokens = json.load(f)
        #tokens = tokens_tmp['token_dictionary']
    #print(tokens)
    if 'token_dictionary' not in tokens :
        file_path = pathlib.Path(tokenFile)
        file_path.unlink()
        print('Invalid token, re-run command')
        sys.exit(1)
    else :
        refreshToken = tokens['token_dictionary']['refresh_token']
        #print('refreshToken: ', refreshToken )

# get accountHash, will always use accountHash instead of accountNumber
try:
    accountHash = [x['hashValue'] for x in get_account_number() if x['accountNumber'] == accountNumber][0]
except Exception as e:
    print(e)
    print("Rerun command")
    pathlib.Path.unlink(tokenFile)
    sys.exit(1)
#print('accountHash', accountHash)
get_positions()

context = zmq.Context()
client_socket = context.socket(zmq.SUB)
client_socket.setsockopt_string(zmq.SUBSCRIBE, '')
#client_socket.connect('tcp://localhost:5559')
client_socket.bind('tcp://*:5559')

#trend
trend_upper = {}
trend_lower = {}
high = 0
low = sys.float_info.max
vol = 0
cumulative_vol = 0
vwap_num = 0
market_open = dt_time(9, 30)
market_close = dt_time(16, 0)
prev_time = market_open
position = False

close = {}
md = {}
orderid_dict = {}
for sym in symbol_list:
    close[sym] = {}
    md[sym] = {}
    orderid_dict[sym]  = ''

df_orderid = pd.DataFrame(orderid_dict.items(), columns=['symbol', 'last_orderid'])
md_keys = ['1','2','3','9']
#avg = []

stop_threads = False
workers = []
time.sleep(10)
t = threading.Thread(target=thread_getOrders,args=(10,lambda: stop_threads))
workers.append(t)
t.start()

while True:
    try:
        #print('allowance: ', allowance)
        #print('orderid: ', orderid)
        msg = json.loads(client_socket.recv_json())
        #print(msg)
        if all ( x in msg for x in ['status','orderId']) :
            #print(msg)
            if 'parent_orderid' in msg :
                print(msg)
                df_msg = pd.DataFrame(msg)
                df_allowance = df_orderid.join(df_msg.set_index(['symbol','parent_orderid']),on=['symbol','last_orderid'])
                print(df_allowance)
                list_symbol_allowance = df_allowance[df_allowance['status'].isin(['FILLED'])]['symbol'].to_list()
                if len(list_symbol_allowance) > 0 :
                    for x in  list_symbol_allowance :
                        symbol_list[x]['allowance'] = 1 
                        #pass
                #symbol_filled = df_msg[df_msg['status'].isin(['FILLED'])]
                print('allowance: ', [symbol_list[x]['allowance'] for x in symbol_list])
                print(md_sym, rsi, symbol_list[symbol]['avg'], limit)
                # print('type orderid: ', type(orderid))
                # print('orderid: ', orderid)
                # print('parent_orderid: ', type(msg['parent_orderid']) )
                # print('parent_orderid: ', msg['parent_orderid'] )
                # print('status: ', msg['status'] )
                #if msg['parent_orderid'] == orderid and msg['status'] in ['FILLED'] :
                    #print(msg)
                    #symbol_list['SPY']['allowance'] = 1
                    #pass
            elif msg['status'] == 'key' :
                access_token = msg['content']['access_token']
                print(access_token)
            elif msg['status'] == 'ALLOWANCE' :
                allowance = int(msg['value'])
                print('allowance:', allowance)
            elif msg['status'] == 'RSI' :
                rsi_low_msg, rsi_high_msg = msg['value'].split(',')
                print('rsi:', [rsi_low_msg,rsi_high_msg])
            elif msg['status'] == 'PNL' :
                price_pnl_msg = float(msg['value'])
                print('pnl:', price_pnl_msg)
            elif msg['status'] == 'SYMBOL' :
                #pass
                msg_symbol = msg['value']
                print('symbol:', msg_symbol)
                # symbol change, resetting:
                #md = {}
                #close = {}
            elif msg['status'] == 'SESSION' :
                session = msg['value']
                print('session:', session)
            elif msg['status'] == 'OFF' :
                stop_threads = True
                for worker in workers:
                    worker.join()
                sys.exit('OFF received, main thread off')
            elif msg['status'] == 'BUY' :
                ord_symbol,ord_price = msg['value'].split(',')
                place_buy_order(ord_symbol,symbol_list[ord_symbol]['qty'], float(ord_price),'SEAMLESS')
            elif msg['status'] == 'SELL' :
                ord_symbol,ord_price = msg['value'].split(',')
                place_sell_order(ord_symbol, symbol_list[ord_symbol]['qty'],float(ord_price),'SEAMLESS')           
            elif msg['status'] == 'LAYER_BUY' :
                layer_input = msg['value'].split(',')
                print(layer_input)
                place_layered_buy_order(layer_input,'SEAMLESS')
            elif msg['status'] == 'LAYER_SELL' :
                layer_input = msg['value'].split(',')
                place_layered_sell_order(layer_input,'SEAMLESS')
            elif msg['status'] == 'BUY_EXTO' :
                ord_symbol,ord_price = msg['value'].split(',')
                place_buy_order(ord_symbol,symbol_list[ord_symbol]['qty'], float(ord_price),'EXTO')
            elif msg['status'] == 'SELL_EXTO' :
                ord_symbol,ord_price = msg['value'].split(',')
                place_sell_order(ord_symbol, symbol_list[ord_symbol]['qty'],float(ord_price),'EXTO')           
            elif msg['status'] == 'LAYER_BUY_EXTO' :
                layer_input = msg['value'].split(',')
                print(layer_input)
                place_layered_buy_order(layer_input,'EXTO')
            elif msg['status'] == 'LAYER_SELL_EXTO' :
                layer_input = msg['value'].split(',')
                place_layered_sell_order(layer_input,'EXTO')                 
            elif msg['status'] == 'QTY' :
                qty = int(msg['value'])
                print('QTY:', qty)                
            elif msg['status'] == 'RSI_multi' :
                list_rsi_tmp = msg['value'].split(',')
                if list_rsi_tmp[0] == 'ALL':
                    for symbol in symbol_list :
                        symbol_list[symbol]['rsi'] = [list_rsi_tmp[1],list_rsi_tmp[2]]
                else :
                    symbol_list[list_rsi_tmp[0]]['rsi'] = [list_rsi_tmp[1],list_rsi_tmp[2]]
                print('symbol_list:', symbol_list)
            elif msg['status'] == 'ALLOWANCE_multi' :
                list_rsi_tmp = msg['value'].split(',')
                if list_rsi_tmp[0] == 'ALL':
                    for symbol in symbol_list :
                        symbol_list[symbol]['allowance'] = int(list_rsi_tmp[1])
                else :
                    symbol_list[list_rsi_tmp[0]]['allowance'] = int(list_rsi_tmp[1])
                print('symbol_list:', symbol_list)
            elif msg['status'] == 'QTY_multi' :
                list_qty_tmp = msg['value'].split(',')
                symbol_list[list_qty_tmp[0]]['qty'] = int(list_qty_tmp[1])
                print('symbol_list:', symbol_list)
            elif msg['status'] == 'PNL_multi' :
                list_rsi_tmp = msg['value'].split(',')
                symbol_list[list_rsi_tmp[0]]['price_pnl'] = float(list_rsi_tmp[1])
                print('symbol_list:', symbol_list)
            elif msg['status'] == 'CANCEL_ALL' :
                list_cxl_tmp = msg['value']
                cancel_all_order(list_cxl_tmp) 
            elif msg['status'] == 'ADD_SYMBOL_multi' :
                symbol_list[msg['value']] = {'qty' : 1, 'price_pnl' : 0.30, 'rsi':['10','200'], 'avg':[], 'allowance':0}
                print('symbol_list:', symbol_list)

            logging.info(f'SETTING,{symbol_list}')
        # cal RSI, decide b/s/h
        # update md[sym] for every market data msg received
        if 'data' in msg:
            for level in msg['data'] :
                if level['service'] == "LEVELONE_EQUITIES" : 
                    #print(msg['data'][0]['content'][0])
                    for d in level['content']:
                        for sym in symbol_list :
                            if d['key'] == sym :
                                #print(d)
                                md[sym].update(d)

            # we set SYMBOL here
            for symbol in symbol_list :
                if all(name in md[symbol] for name in md_keys):
                    md_sym = md[symbol]
                    # remove bad quote
                    if (md_sym['1'] <= md_sym['2']) and (md_sym['3'] <= md_sym['2']) and (md_sym['3'] >= md_sym['1']) :
                        time_local = strftime('%Y-%m-%d %H:%M', localtime(msg['data'][0]['timestamp']/1000))
                        #print(time_local)
                        close[symbol][time_local] = md_sym['3']
                        close_s = pd.Series(close[symbol])
                        #print(close_s)

                        ## strategy: trend
                        # 1. get open price at 9:30, field 17
                        # 2. get vwap every minute, accumulative sum(lastpx * lastsize)/sum(lastsize)
                        last = md_sym['3']
                        # get current local time in hours and minutes
                        curr_time = dt.now(pytz.timezone('America/New_York')).time().replace(second=0, microsecond=0)
                        if (symbol == 'SPY') and (curr_time >= market_open) and (curr_time < market_close):
                            td_open = 647.49
                            if prev_time != curr_time :
                                # at the start of a new minute, calculate vwap of the prev minute
                                #print(prev_time)
                                #print(f'High: {high}')
                                #print(f'Low: {low}')
                                #print(f'Close: {last}')
                                #print(f'Volume: {vol}')
                                #print(f'Cumulative Volume: {cumulative_vol}')
                                vwap_num += ((high + low + last) / 3) * vol
                                if (cumulative_vol != 0) :
                                    vwap = vwap_num / cumulative_vol
                                    print(f'VWAP: {vwap}')

                                high = last
                                low = last
                                vol = md_sym['9']
                                cumulative_vol += vol
                            else :
                                # get close, high, low, and vol of each minute
                                high = max(high, last)
                                low = min(low, last)
                                vol += md_sym['9']
                                cumulative_vol += md_sym['9']

                            if (curr_time.minute == 0) or (curr_time.minute == 30) :
                                if prev_time != curr_time :
                                    avg_move = df_bounds.loc[(df_bounds['key'] == 'SPY') & (df_bounds['time'] == curr_time), 'move'].iloc[0]
                                    upper = max(td_open, prev_close_dict['SPY']) * (1 + avg_move)
                                    lower = max(td_open, prev_close_dict['SPY']) * (1 - avg_move)
                                    print(f'Upper bound: {upper}')
                                    print(f'Lower bound: {lower}')

                                    if last >= upper :
                                        if position == False :
                                            print(f'BUY @ {last}')
                                            position = True
                                        else :
                                            if last == max(upper, vwap) :
                                                print(f'SELL @ {last}')
                                                position = False
                                    elif last <= lower :
                                        if position == True :
                                            print(f'SELL @ {last}')
                                            position = False
                                        else :
                                            if last == min(lower, vwap) :
                                                print(f'BUY @ {last}')
                                                position = True
                            prev_time = curr_time
                       
                        ## strategy : mean reversion by RSI
                        if len(close_s) > 14  :
                            #rsi = ta.rsi(close_s)
                            rsi_low, rsi_high = symbol_list[symbol]['rsi']
                            rsi, symbol_list[symbol]['avg'], limit = calc_rsi(close_s,avg=symbol_list[symbol]['avg'], rsi_target=[int(rsi_low), int(rsi_high)], rsi_ahead=[int(rsi_low)+5,int(rsi_high)-5],pnl=symbol_list[symbol]['price_pnl'])
                            if rsi > int(rsi_high) or rsi < int(rsi_low) or limit[0] != 0 or limit[1] != 10_000 :
                                #print(close_s.tail(3))
                                #print(rsi, avg, limit)
                                if symbol_list[symbol]['allowance'] > 0 :
                                    if limit[0] > 0 :
                                        #place_buy_order(float(limit[0]))
                                        if session == 'REGULAR' :
                                            if symbol_list[symbol]['layer'] > 1 :
                                                place_layered_buy_oto_order(symbol,[round(float(limit[0]),2),symbol_list[symbol]['price_pnl'],symbol_list[symbol]['qty'],symbol_list[symbol]['layer'],symbol_list[symbol]['layer_spread']])
                                            else:
                                                orderid_dict[symbol] = place_buy_oto_order(symbol,symbol_list[symbol]['qty'],[round(float(limit[0]),2),symbol_list[symbol]['price_pnl']])
                                        else :
                                            orderid_dict[symbol]  = place_buy_order(symbol,symbol_list[symbol]['qty'],round(float(limit[0]),2))
                                        df_orderid = pd.DataFrame(orderid_dict.items(), columns=['symbol', 'last_orderid'])
                                        print(md_sym, rsi, symbol_list[symbol]['avg'], limit)
                                        symbol_list[symbol]['allowance'] = 0

                                        logging.info(f'BUY,{md_sym},{rsi},{limit}')

                                    elif limit[1] < 10_000 :
                                        #place_sell_order(float(limit[1]))
                                        if session == 'REGULAR' :
                                            if symbol_list[symbol]['layer'] > 1 :
                                                place_layered_sell_oto_order(symbol,[round(float(limit[1]),2),symbol_list[symbol]['price_pnl'],symbol_list[symbol]['qty'],symbol_list[symbol]['layer'],symbol_list[symbol]['layer_spread']])
                                            else :
                                                orderid_dict[symbol]  = place_sell_oto_order(symbol,symbol_list[symbol]['qty'],[round(float(limit[1]),2),symbol_list[symbol]['price_pnl']])
                                        else :
                                            orderid_dict[symbol]  = place_sell_order(symbol,symbol_list[symbol]['qty'],round(float(limit[1]),2))
                                        df_orderid = pd.DataFrame(orderid_dict.items(), columns=['symbol', 'last_orderid'])
                                        print(md_sym,rsi, symbol_list[symbol]['avg'], limit)
                                        symbol_list[symbol]['allowance'] = 0

                                        logging.info(f'SELL,{md_sym},{rsi},{limit}')

    except KeyboardInterrupt:
        stop_threads = True
        for worker in workers:
            worker.join()
        print("out of the loop!")
        time.sleep(12)
        sys.exit('main thread done')
    except Exception as e:
        print(str(e))

    time.sleep(0.01)




