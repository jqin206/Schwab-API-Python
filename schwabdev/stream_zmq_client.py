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
cols_print = ['timestamp','7','4','1','2','5','6','3','16','8',]

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
        try : 
            print(df_equity[['timestamp','1','2','3']].tail(1), end='', flush=True) 
        except Exception as e:
            print(e)




        data += '\n' +  msg 
        df = pd.read_json(io.StringIO(data),lines=True)
        df1 = pd.json_normalize(df['data'])
        try :
            for col in df1.columns:
                print(col)
                df2[col] = pd.json_normalize(df1[col],"content",["timestamp","service"])
                df2[col] = df2[col].set_index(['key']).groupby('key').ffill().reset_index()
        except Exception as e:
            print(e)
            print(msg)
            #print(data)
            df1.to_csv('df1.csv')
            break
    print(time.time() - start)
