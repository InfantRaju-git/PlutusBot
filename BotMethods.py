from datetime import datetime as dt
import datetime
from dateutil.rrule import rrule, WEEKLY
import requests
import pandas as pd
import time
import Global
import DhanMethods

SHORT = "PE"
LONG = "CE"

def BotException(exceptionMessage):
    send_telegram_message(exceptionMessage)
    raise Exception(exceptionMessage)

def unix_to_local_time(unix_timestamp):
    return str(dt.fromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S'))

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
        
        del response, json_data, df

        ohlc['HA_Close'] = (ohlc['open'] + ohlc['high'] + ohlc['low'] + ohlc['close']) / 4
        ha_open = ohlc['HA_Close'].shift(1)
        ohlc['HA_Open'] = ha_open.values[0]
        ohlc.loc[1:, 'HA_Open'] = ha_open.values[1:]
        ohlc['HA_High'] = ohlc[['HA_Open', 'HA_Close', 'high']].max(axis=1)
        ohlc['HA_Low'] = ohlc[['HA_Open', 'HA_Close', 'low']].min(axis=1)

        last_index = len(ohlc)-1
        curr_close = ohlc.iloc[last_index]['HA_Close']
        curr_open = ohlc.iloc[last_index]['HA_Open']
        ohlc_open = ohlc.iloc[last_index]['open']
        prev_close = ohlc.iloc[last_index-1]['HA_Close']
        prev_open = ohlc.iloc[last_index-1]['HA_Open']
        prev_low = ohlc.iloc[last_index-1]['HA_Low']
        prev_high = ohlc.iloc[last_index-1]['HA_High']

        del ohlc

        if curr_close >= curr_open and (prev_close >= prev_open or prev_open == prev_low): #CE Entry
            if Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == True and Global.SYMBOL_SETTINGS[symbol]["POSITION_TYPE"] == SHORT:  #Exit PE
                Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = False
                Global.SYMBOL_SETTINGS[symbol]["POSITION_TYPE"] = None
                DhanMethods.place_order(symbol)
                profit_loss = Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] - ohlc_open
                send_telegram_message(symbol+" Exit Sell: "+ str(ohlc_open) +", P/L: "+str(profit_loss))
                Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = None
                Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"] = None

            if Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == False: #Enter CE
                Global.SYMBOL_SETTINGS[symbol]["POSITION_TYPE"] = LONG
                Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = True
                Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = ohlc_open
                Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"] = DhanMethods.find_matching_security_ids(get_atm_strike(symbol), LONG, symbol)
                DhanMethods.place_order(symbol)
                print("Long: "+str(Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"]))
                send_telegram_message(symbol+" Long Entry:"+ str(Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"]))

        if curr_close < curr_open and (prev_close < prev_open or prev_open == prev_high): #PE Entry
            if Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == True and Global.SYMBOL_SETTINGS[symbol]["POSITION_TYPE"] == LONG: #Exit CE
                Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = False
                Global.SYMBOL_SETTINGS[symbol]["POSITION_TYPE"] = None
                DhanMethods.place_order(symbol)
                profit_loss = ohlc_open - Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"]
                send_telegram_message(symbol+" Exit Long: "+str(ohlc_open)+", P/L: "+str(profit_loss))
                Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = None
                Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"] = None

            if Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == False: #Enter PE
                Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = True
                Global.SYMBOL_SETTINGS[symbol]["POSITION_TYPE"] = SHORT
                Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = ohlc_open
                Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"] = DhanMethods.find_matching_security_ids(get_atm_strike(symbol), SHORT, symbol)
                DhanMethods.place_order(symbol)
                print("Short: "+str(Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"]))
                send_telegram_message(symbol+" Sell Entry:"+ str(Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"]))

    except Exception as e:
            raise BotException("Error in trade symbol: "+str(e))

def exit_open_trade(symbol):
    if(Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == True):
        Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = False
        Global.SYMBOL_SETTINGS[symbol]["POSITION_TYPE"] = None
        send_telegram_message("Exit EOD")
        Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = None
        Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"] = None
