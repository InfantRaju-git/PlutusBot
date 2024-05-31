from datetime import datetime as dt
import datetime
from dateutil.rrule import rrule, WEEKLY
import requests
import pandas as pd
import time
import Global
import DhanMethods
from ta.trend import PSARIndicator

SHORT = "PUT"
LONG = "CALL"
BUY = "BUY"
SELL = "SELL"
LOGS_FOLDER = "SecurityInfo"

def BotException(exceptionMessage):
    send_telegram_message(exceptionMessage)
    raise Exception(exceptionMessage)

def unix_to_local_time(unix_timestamp):
    return str(dt.fromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S'))

def write_to_log(message):
    current_date = dt.now().strftime("%Y-%m-%d")
    log_file_name = f"{LOGS_FOLDER}/{current_date}.log"
    with open(log_file_name, "a") as log_file:
        log_file.write(str(dt.now())+": "+message)

def send_telegram_message(message):
    if Global.SEND_TELEGRAM_MESSAGE:
        url = f"https://api.telegram.org/bot{Global.TELEGRAM_TOKEN}/sendMessage?chat_id={Global.TELEGRAM_CHATID}&text={message}"
        requests.get(url)

def get_atm_strike(symbol):
    if symbol == "NIFTY":
        increment = 50
    elif symbol == "BANKNIFTY":
        increment = 100
    else:
        raise BotException("Invalid symbol at get_atm_strike")

    if Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] is None:
        raise BotException("Current price is none in get_atm_strike")

    if Global.SYMBOL_SETTINGS[symbol]["POSITION_TYPE"] == SHORT:
        rounded_strike = ((Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] - increment -1) // increment) * increment
    elif Global.SYMBOL_SETTINGS[symbol]["POSITION_TYPE"] == LONG:
        rounded_strike = ((Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] + increment - 1) // increment) * increment
    else:
        raise BotException("Invalid current_trend value in get_atm_strike")

    return int(rounded_strike)

def trade_symbol(symbol):
    try:
        end_time_in_millis = int(time.time() * 1000)
        end_time = datetime.datetime.fromtimestamp(end_time_in_millis / 1000)

        start_time = end_time - datetime.timedelta(days=0)
        start_time = start_time.replace(hour=9, minute=0, second=0, microsecond=0)
        start_time_in_millis = int(start_time.timestamp() * 1000)
        end_time_in_millis = int(end_time.timestamp() * 1000)
        
        url = 'https://groww.in/v1/api/charting_service/v4/chart/exchange/NSE/segment/CASH/'+symbol
        params = {
            'endTimeInMillis': str(end_time_in_millis),
            'intervalInMinutes': Global.SYMBOL_SETTINGS[symbol]["TRADE_TF"],
            'startTimeInMillis': str(start_time_in_millis),
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            json_data = response.json()
            df = pd.DataFrame(json_data)
        
        df.drop(['changeValue', 'changePerc', 'closingPrice', 'startTimeEpochInMillis'], axis=1, inplace=True)
        ohlc = df['candles'].apply(pd.Series)
        ohlc.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        ohlc.drop(['volume'], axis=1, inplace=True)
        psar = PSARIndicator(ohlc['high'], ohlc['low'], ohlc['close'], step=0.2, max_step=0.2) # Calculate PSAR values
        ohlc['psar'] = psar.psar()
        
        del response, json_data, df

        last_index = len(ohlc)-1
        ohlc_open = ohlc.iloc[last_index]['open']
        prev_close = ohlc.iloc[last_index-1]['close']
        prev_open = ohlc.iloc[last_index-1]['open']
        prev_psar = ohlc.iloc[last_index-1]['psar']
        min_price_moment = 5

        del ohlc

        if prev_close > prev_open and prev_open > prev_psar: #CE Entry

            if Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == False:
                Global.SYMBOL_SETTINGS[symbol]["POSITION_TYPE"] = LONG
                Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = True
                Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = ohlc_open
                Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"] = DhanMethods.find_matching_security_ids(get_atm_strike(symbol), LONG, symbol)
                DhanMethods.place_order(symbol , LONG, BUY)
                print("Long: "+str(Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"]))
                #send_telegram_message(symbol+" Long Entry:"+ str(Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"]))
                write_to_log(symbol+" Long Entry:"+ str(Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"]))

        if prev_open > prev_close and prev_psar > prev_open: #PE Entry

            if Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == False:
                Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = True
                Global.SYMBOL_SETTINGS[symbol]["POSITION_TYPE"] = SHORT
                Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = ohlc_open
                Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"] = DhanMethods.find_matching_security_ids(get_atm_strike(symbol), SHORT, symbol)
                DhanMethods.place_order(symbol, SHORT, BUY)
                print("Short: "+str(Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"]))
                #send_telegram_message(symbol+" Sell Entry:"+ str(Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"]))
                write_to_log(symbol+" Sell Entry:"+ str(Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"]))

        #Exit Position
        if Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == True:
            
            if Global.SYMBOL_SETTINGS[symbol]["POSITION_TYPE"] == LONG:
                if prev_open > prev_close and prev_psar > prev_open:
                    Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = False
                    Global.SYMBOL_SETTINGS[symbol]["POSITION_TYPE"] = None
                    DhanMethods.place_order(symbol, LONG, SELL)
                    profit_loss = ohlc_open - Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"]
                    #send_telegram_message(symbol+" Exit Long: "+str(ohlc_open)+", P/L: "+str(int(profit_loss)))
                    write_to_log(symbol+" Exit Long: "+str(ohlc_open)+", P/L: "+str(int(profit_loss)))
                    Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = None
                    Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"] = None

            if Global.SYMBOL_SETTINGS[symbol]["POSITION_TYPE"] == SHORT:
                if prev_close > prev_open and prev_open > prev_psar:
                    Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = False
                    Global.SYMBOL_SETTINGS[symbol]["POSITION_TYPE"] = None
                    DhanMethods.place_order(symbol, SHORT, SELL)
                    profit_loss = Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] - ohlc_open
                    #send_telegram_message(symbol+" Exit Sell: "+ str(ohlc_open) +", P/L: "+str(int(profit_loss)))
                    write_to_log(symbol+" Exit Sell: "+ str(ohlc_open) +", P/L: "+str(int(profit_loss)))
                    Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = None
                    Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"] = None

    except Exception as e:
        raise BotException("Error in trade symbol: "+str(e))

def exit_open_trade(symbol):
    if(Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == True):
        DhanMethods.place_order(symbol, Global.SYMBOL_SETTINGS[symbol]["POSITION_TYPE"], SELL)
        Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = False
        Global.SYMBOL_SETTINGS[symbol]["POSITION_TYPE"] = None
        send_telegram_message("Exit EOD")
        Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = None
        Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"] = None
