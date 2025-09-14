import zmq
import pandas as pd
import time
import io
import json

import warnings
warnings.filterwarnings("ignore")

context = zmq.Context()

client_socket = context.socket(zmq.SUB)
client_socket.setsockopt_string(zmq.SUBSCRIBE, '')
client_socket.connect('tcp://localhost:5559')
data = ''
current_time = int(time.time()) * 1000
equity = [{"service":"LEVELONE_EQUITIES", "timestamp":current_time,"content":[]}]
options = [{"service":"LEVELONE_OPTIONS", "timestamp":current_time,"content":[]}]
dict_df = {}
cols_print = ['timestamp','40','4','1','X','2','5','39','-','3','9','41','8']

while True:
    # print(client_socket.recv_json())
    msg = json.loads(client_socket.recv_json())

    if 'data' in msg: 
        for i in msg['data'] :
            if i['service'] == 'LEVELONE_EQUITIES':
                equity.append(i)
            elif i['service'] == 'LEVELONE_OPTIONS':
                options.append(i)
        
        df_equity = pd.json_normalize(equity,"content",["timestamp","service"])
        df_options = pd.json_normalize(options,"content",["timestamp","service"]) 

        df_equity.ffill(inplace=True)
        df_equity['X'] = 'x'
        df_equity['-'] = '-'
        try : 
            print('\r',df_equity[cols_print].tail(1).to_string(index=False, header=False), end='', flush=True) 
        except Exception as e:
            print(e)
