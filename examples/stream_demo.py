import os
import sys

home_dir = os.path.expanduser('~/')
sys.path.insert(0, 'C:\git_home\Schwab-API-Python')

import schwabdev
from dotenv import load_dotenv
import os
import datetime

def main():
    # place your app key and app secret in the .env file
    load_dotenv()  # load environment variables from .env file

    client = schwabdev.Client(os.getenv('app_key'), os.getenv('app_secret'), os.getenv('callback_url'))
    client.update_tokens_auto()  # update tokens automatically (except refresh token)
    expiration = datetime.datetime.today().strftime('%y%m%d')
    #spy_atm = input('Pleas enter SPY ATM:')
    spy_opt_temp_call = 'SPY   {d}C00{s}000'
    #spy_options_call = [ spy_opt_temp_call.format(d=expiration,s=str(int(spy_atm) +i)) for i in range(5,-6,-1)]
    spy_opt_temp_put = 'SPY   {d}P00{s}000'
    #spy_options_put = [ spy_opt_temp_put.format(d=expiration,s=str(int(spy_atm) +i)) for i in range(5,-6,-1)]
    #spy_options = ','.join(spy_options_call + spy_options_put)

    """ 
    # example of using your own response handler, prints to main terminal.
    def my_handler(message):
        print(message)
    client.stream.start(my_handler)
    """

    # example of using the default response handler
    try :
        client.stream.start()

        """
        # you can also define a variable for the steamer:
        streamer = client.stream
        streamer.start()
        """

        """
        by default all shortcut requests (below) will be "ADD" commands meaning the list of symbols will be added/appended 
        to current subscriptions for a particular service, however if you want to overwrite subscription (in a particular 
        service) you can use the "SUBS" command. Unsubscribing uses the "UNSUBS" command. To view current subscriptions use
        the "VIEW" command.
        """

        # these three do the same thing
        # client.stream.send(client.stream.basic_request("LEVELONE_EQUITIES", "ADD", parameters={"keys": "AMD,INTC", "fields": "0,1,2,3,4,5,6,7,8"}))
        # client.stream.send(client.stream.level_one_equities("AMD,INTC", "0,1,2,3,4,5,6,7,8"), command="ADD")
        client.stream.send(client.stream.level_one_equities("SPY,TLT,XLP,XLV,XLRE,JEPQ,XLE", "0,1,2,3,4,5,6,7,8,9,10,11,12,16,17,25,27,28,29,30,32,34,35,37,38,39,40,41"))
        #client.stream.send(client.stream.level_one_equities("SPY", "0,1,2,3,4,5,6,7,8,9,10,11,16,34,35,37,38,39,40,41"))

        #client.stream.send(client.stream.level_one_options("SPY   240723C00552000,SPY   240723C00553000,SPY   240723C00554000,SPY   240723C00555000,SPY   240723C00552600", "0,1,2,3,4,5,6,7,8,9,10,11,15,16,17,18"))
        #client.stream.send(client.stream.level_one_options(spy_options, "0,1,2,3,4,5,6,7,8,9,10,11,15,16,17,18"))

        client.stream.send(client.stream.level_one_futures("/MES,/MNQ,/MYM", "0,1,2,3,4,5,6"))

        # client.stream.send(client.stream.level_one_futures_options("keys", "0,1,2,3,4,5"))

        # client.stream.send(client.stream.level_one_forex("EUR/USD", "0,1,2,3,4,5,6,7,8"))

        #client.stream.send(client.stream.nyse_book("SPY", "0,1,2,3,4,5,6,7,8"))

        #client.stream.send(client.stream.nasdaq_book("SPY", "0,1,2,3,4,5,6,7,8"))

        # client.stream.send(client.stream.options_book("keys", "0,1,2,3,4,5,6,7,8"))

        # client.stream.send(client.stream.chart_equity("keys", "0,1,2,3,4,5,6,7,8"))

        # client.stream.send(client.stream.chart_futures("/ES", "0,1,2,3,4,5,6,7,8"))

        # client.stream.send(client.stream.screener_equity("keysC", "0,1,2,3,4,5,6,7,8"))

        # client.stream.send(client.stream.screener_options("keys", "0,1,2,3,4,5,6,7,8"))

        # client.stream.send(client.stream.account_activity("Account Activity", "0,1,2,3"))

        # stop the stream after 60 seconds (since this is a demo
        import time
        time.sleep(50400)
        client.stream.stop()
    except KeyboardInterrupt :
        print('stopping ...')
        client.stream.stop()


if __name__ == '__main__':
    print("Welcome to the unofficial Schwab interface!\nGithub: https://github.com/tylerebowers/Schwab-API-Python")
    main()  # call the user code above
