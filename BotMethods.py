from datetime import datetime as dt
import datetime
from dateutil.rrule import rrule, WEEKLY
import requests
import pandas as pd
import time
import Global
import DhanMethods
from ta.trend import PSARIndicator

BUY = "BUY"
SELL = "SELL"
LOGS_FOLDER = "SecurityInfo"

def BotException(exceptionMessage):
    send_telegram_message(exceptionMessage)
    raise Exception(exceptionMessage)

def unix_to_local_time(unix_timestamp):
    return str(dt.fromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S'))

def send_telegram_message(message):
    if Global.SEND_TELEGRAM_MESSAGE:
        url = f"https://api.telegram.org/bot{Global.TELEGRAM_TOKEN}/sendMessage?chat_id={Global.TELEGRAM_CHATID}&text={message}"
        requests.get(url)
    
def set_config(symbol):
    end_time_in_millis = int(time.time() * 1000)
    end_time = datetime.datetime.fromtimestamp(end_time_in_millis / 1000)
    start_time = end_time - datetime.timedelta(days=40)
    start_time = start_time.replace(hour=9, minute=0, second=0, microsecond=0)
    start_time_in_millis = int(start_time.timestamp() * 1000)
    end_time_in_millis = int(end_time.timestamp() * 1000)
    close_price = None

    url = 'https://groww.in/v1/api/charting_service/v4/chart/exchange/NSE/segment/CASH/'+symbol
    params = {
        'endTimeInMillis': str(end_time_in_millis),
        'intervalInMinutes': 60,
        'startTimeInMillis': str(start_time_in_millis),
    }
    
    response = requests.get(url, params=params)
    if response.status_code == 200:
        json_data = response.json()
        df = pd.DataFrame(json_data)

    if response.status_code == 200:
        json_data = response.json()
        df = pd.DataFrame(json_data)
        df.drop(['changeValue', 'changePerc', 'closingPrice', 'startTimeEpochInMillis'], axis=1, inplace=True)
        df = df['candles'].apply(pd.Series)
        df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        df['EMA_7'] = df['close'].ewm(span=7, adjust=False).mean()

    close_price = df.iloc[len(df)-1]['close']
    ema_price = df.iloc[len(df)-1]['EMA_7']
    if(close_price >= ema_price):
        Global.SYMBOL_SETTINGS[symbol]["TREND"] = "CE"
        trend = "CE"
    if(close_price < ema_price):
        Global.SYMBOL_SETTINGS[symbol]["TREND"] = "PE"
        trend = "PE"

    del response, json_data, df

    if symbol == "BANKNIFTY":
        increment = 100
    else:
        increment = 50

    if close_price is None:
        raise BotException("Current close_price is none in get_atm_strike")

    if trend == "PE":
        rounded_strike = int(((close_price + increment -1) // increment) * increment)
    elif trend == "CE":
        rounded_strike = int((close_price // increment) * increment)

    option_info = DhanMethods.find_matching_security_ids(rounded_strike, trend, symbol)
    Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"] = option_info[0]
    
    if(Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"] == None):
        send_telegram_message("Security ID is Not Set")
        raise BotException("Security ID is Not Set")

    expiry_timestamp = option_info[1]
    expiry_date = expiry_timestamp.date()

    year = expiry_date.year % 100  # Last two digits of the year
    month = expiry_date.month  # Month as an integer
    day = expiry_date.day  # Day as an integer

    expiry_str = f"{year:02d}{month}{day:02d}"
    strike_price_str = f"{rounded_strike:05d}"
    Global.SYMBOL_SETTINGS[symbol]["OPTION_ID"] = f"{symbol}{expiry_str}{strike_price_str}{trend}"

    if(Global.SYMBOL_SETTINGS[symbol]["OPTION_ID"] is None):
        send_telegram_message("Security ID is Not Set")
        raise BotException("Option ID is not set")
    
    send_telegram_message("Option ID: "+Global.SYMBOL_SETTINGS[symbol]["OPTION_ID"])

def trade_symbol(symbol):
    if(Global.SYMBOL_SETTINGS[symbol]["OPTION_ID"] is None):
        set_config(symbol)
    try:
        end_time_in_millis = int(time.time() * 1000)
        end_time = datetime.datetime.fromtimestamp(end_time_in_millis / 1000)

        start_time = end_time - datetime.timedelta(days=6)
        start_time = start_time.replace(hour=9, minute=0, second=0, microsecond=0)
        start_time_in_millis = int(start_time.timestamp() * 1000)
        end_time_in_millis = int(end_time.timestamp() * 1000)
        
        url = 'https://groww.in/v1/api/stocks_fo_data/v4/charting_service/chart/exchange/NSE/segment/FNO/'+Global.SYMBOL_SETTINGS[symbol]["OPTION_ID"]
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
        ohlc['EMA_6'] = ohlc['close'].ewm(span=6, adjust=False).mean()

        del response, json_data, df

        last_index = len(ohlc)-1
        ohlc_open = ohlc.iloc[last_index]['open']
        prev_close = ohlc.iloc[last_index-1]['close']
        prev_open = ohlc.iloc[last_index-1]['open']
        ohlc_ema = ohlc.iloc[last_index-1]['EMA_6']
        if Global.SYMBOL_SETTINGS[symbol]["TREND"] == "CE":
            take_position = "CALL" 
        else:
            take_position = "PUT"

        del ohlc
        print(symbol+": "+str(dt.now())+": "+str(prev_open)+", "+str(prev_close)+", "+str(ohlc_ema)) #to debug

        if prev_close > prev_open and prev_close > ohlc_ema and Global.SYMBOL_SETTINGS[symbol]["DAILY_PL"] < Global.SYMBOL_SETTINGS[symbol]["DAILY_PL_LIMIT"] and Global.SYMBOL_SETTINGS[symbol]["DAILY_PL"] > -10: #CE Entry

            if Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == False:
                Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = True
                Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = ohlc_open
                DhanMethods.place_order(symbol, take_position, BUY)
                send_telegram_message(symbol+": Enter: "+str(ohlc_open))

        #Exit Position
        if Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == True:
            
            if prev_close < ohlc_ema or ohlc_open <= (Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"]-Global.SYMBOL_SETTINGS[symbol]["STOP_LOSS"]) or ohlc_open >= (Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"]+Global.SYMBOL_SETTINGS[symbol]["TAKE_PROFIT"]):
                Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = False
                DhanMethods.place_order(symbol, take_position, SELL)
                profit_loss = ohlc_open - Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"]
                Global.SYMBOL_SETTINGS[symbol]["DAILY_PL"] += profit_loss
                send_telegram_message(symbol+": Exit: "+str(ohlc_open)+"PL: "+str(profit_loss))
                Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = None

    except Exception as e:
        send_telegram_message("Error in trade symbol: "+str(e))
        raise BotException("Error in trade symbol: "+str(e))

def exit_open_trade(symbol):
    if Global.SYMBOL_SETTINGS[symbol]["TREND"] == "CE":
        take_position = "CALL" 
    else:
        take_position = "PUT"
    Global.SYMBOL_SETTINGS[symbol]["OPTION_ID"] = None
    Global.SYMBOL_SETTINGS[symbol]["DAILY_PL"] = None

    if(Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == True):
        DhanMethods.place_order(symbol, take_position, SELL)
        Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = False
        send_telegram_message("Exit EOD")
        Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = None
        Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"] = None
