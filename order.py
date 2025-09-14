import json

def equityOrder(ordType='LIMIT', side='BUY', qty=1, symbol='SPY',price=0.01,session='SEAMLESS'):
    if side == 'SELL' :
        taxLotMethod = 'LOW_COST'
    else :
        taxLotMethod = 'FIFO'
        
    order = {
  "orderType": "LIMIT", 
  "session": session, 
  "price": price,
  "duration": "DAY", 
  "orderStrategyType": "SINGLE",
  "taxLotMethod": taxLotMethod,
  "orderLegCollection": [ 
   { 
    "instruction": side, 
    "quantity": qty, 
    "instrument": { 
     "symbol": symbol, 
     "assetType": "EQUITY" 
    } 
   } 
  ] 
}
    return order


def otoOrder(ordType='LIMIT', side='BUY', qty=1, symbol='SPY',price=0.01,delta=0.05):
    if side == 'BUY' :
        side2 = 'SELL'
        price2 = price + delta
    else :
        side2 = 'BUY'
        price2 = price - delta
    price2 = round(price2,2)
    order = { 
        "orderType": "LIMIT", 
        "session": "NORMAL", 
        "price": price, 
        "duration": "DAY", 
        "orderStrategyType": "TRIGGER", 
        "orderLegCollection": [ 
            { 
                "instruction": side, 
                "quantity": qty, 
                "instrument": { 
                    "symbol": symbol, 
                    "assetType": "EQUITY" 
                } 
            } 
        ], 
         "childOrderStrategies": [ 
            { 
                "orderType": "LIMIT", 
                "session": "NORMAL", 
                "price": price2, 
                "duration": "DAY", 
                "orderStrategyType": "SINGLE", 
                "orderLegCollection": [ 
                    { 
                    "instruction": side2, 
                    "quantity": qty, 
                    "instrument": { 
                        "symbol": symbol, 
                        "assetType": "EQUITY" 
                    } 
                    } 
                ] 
            } 
        ] 
    }
    print(order)
    return order