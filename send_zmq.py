#/Users/juneqin/anaconda3/bin/python
import json
from os import system
import sys
import platform
import time
import platform
import zmq
import inspect

dev_platform = platform.platform()

if 'Linux' in dev_platform:
    home_dir = '/storage/emulated/0/'
elif 'Windows' in dev_platform:
    home_dir = 'C:/git_home/'
elif'iPhone' in dev_platform:
    pass
else :
    pass
        
def display_menu(menu):
    """
    Display a menu where the key identifies the name of a function.
    :param menu: dictionary, key identifies a value which is a function name
    :return:
    """
    for k, function in menu.items():
        print(k, function.__name__)

def set_symbol ():
    symbol_msg = input("Please enter symbol to trade: (SPY,QQQ) ")
    send_zmq({"status":"SYMBOL","orderId":"","value": symbol_msg} )

def set_allowance_multi():
    allowance_msg = input("Please enter your allowance: 0 to pause, 1 to trade (SYMBOL,ALLOWANCE) ")
    send_zmq({"status":"ALLOWANCE_multi","orderId":"","value": allowance_msg} )

def send_rsi ():
    rsi_msg = input("Please enter your RSI (rsi_low,rsi_high): ")
    send_zmq({"status":"RSI","orderId":"","value":rsi_msg})

def send_pnl ():
    pnl_msg = input("Please enter your PNL (0.5): ")
    send_zmq({"status":"PNL","orderId":"","value":pnl_msg})

def set_session ():
    session_msg = input("Please enter session to trade: (REGULAR, EXT) ")
    send_zmq({"status":"SESSION","orderId":"","value": session_msg} )

def set_qty ():
    qty = input("Please enter qty to trade: ")
    send_zmq({"status":"QTY","orderId":"","value": qty} )

def place_buy_order():
    order_input = input("Please enter your limit price for your BUY order: (SYMBOL,PRICE) ")
    send_zmq({"status":"BUY","orderId":"","value": order_input} )

def place_sell_order():
    order_input = input("Please enter your limit price for your SELL order: (SYMBOL,PRICE)  ")
    send_zmq({"status":"SELL","orderId":"","value": order_input} )

def place_layer_buy_order():
    order_input = input("Please place your layered BUY order (SYMBOL,INIT_PRICE,INCREMENT,NUM_ORDERS): ")
    send_zmq({"status":"LAYER_BUY","orderId":"","value": order_input} )

def place_layer_sell_order():
    order_input = input("Please place your layered SELL order (SYMBOL,NIT_PRICE,INCREMENT,NUM_ORDERS): ")
    send_zmq({"status":"LAYER_SELL","orderId":"","value": order_input} )

def place_buy_order_exto():
    order_input = input("Please enter your limit price for your BUY order: (SYMBOL,PRICE) ")
    send_zmq({"status":"BUY_EXTO","orderId":"","value": order_input} )

def place_sell_order_exto():
    order_input = input("Please enter your limit price for your SELL order: (SYMBOL,PRICE)  ")
    send_zmq({"status":"SELL_EXTO","orderId":"","value": order_input} )

def place_layer_buy_order_exto():
    order_input = input("Please place your layered BUY order (SYMBOL,INIT_PRICE,INCREMENT,NUM_ORDERS): ")
    send_zmq({"status":"LAYER_BUY_EXTO","orderId":"","value": order_input} )

def place_layer_sell_order_exto():
    order_input = input("Please place your layered SELL order (SYMBOL,NIT_PRICE,INCREMENT,NUM_ORDERS): ")
    send_zmq({"status":"LAYER_SELL_EXTO","orderId":"","value": order_input} )

def send_off ():
    send_zmq({"status":"OFF","orderId":""})

## multi
def send_rsi_multi ():
    rsi_multi_msg = input("Please enter your RSI (symbol,rsi_low,rsi_high): ")
    send_zmq({"status":"RSI_multi","orderId":"","value":rsi_multi_msg})

def set_qty_multi ():
    qty_multi_msg = input("Please enter qty to trade (symbol,qty): ")
    send_zmq({"status":"QTY_multi","orderId":"","value": qty_multi_msg} )

def send_pnl_multi ():
    pnl_multi_msg = input("Please enter your PNL (symbol,0.5): ")
    send_zmq({"status":"PNL_multi","orderId":"","value":pnl_multi_msg})   

def add_symbol_multi ():
    add_symbol_multi_msg = input("Please add symbol (SPXL): ")
    send_zmq({"status":"ADD_SYMBOL_multi","orderId":"","value":add_symbol_multi_msg})  

def cancel_all ():
    cancel_symbol = input("Please enter symbol to cxl (ALL,SPY): ")
    send_zmq({"status":"CANCEL_ALL","orderId":"","value":cancel_symbol})  

def send_zmq (data_dict) :
    context = zmq.Context()
    pub_socket = context.socket(zmq.PUB)
    #server_socket.bind('tcp://*:5559')
    pub_socket.connect('tcp://32.217.55.9:5559')
    #pub_socket.connect('tcp://10.0.4.38:5559')
    #pub_socket.connect('tcp://localhost:5559')
    time.sleep(2)
    pub_socket.send_json(json.dumps(data_dict))

def done():
    system('cls')  # clears stdout
    print("Goodbye")
    sys.exit()

def main():
    # Create a menu dictionary where the key is an integer number and the
    # value is a function name.
    functions_names = [set_symbol,set_allowance_multi,send_rsi,send_pnl,set_session,set_qty,place_buy_order, place_sell_order,place_layer_buy_order,place_layer_sell_order,send_off,done,send_rsi_multi,set_qty_multi,send_pnl_multi,add_symbol_multi,cancel_all,place_buy_order_exto, place_sell_order_exto,place_layer_buy_order_exto,place_layer_sell_order_exto]
    menu_items = dict(enumerate(functions_names, start=1))

    while True:
        display_menu(menu_items)
        selection = int(
            input("Please enter your selection number: "))  # Get function key
        selected_value = menu_items[selection]  # Gets the function name
        selected_value()  # add parentheses to call the function


if __name__ == "__main__":
    main()


