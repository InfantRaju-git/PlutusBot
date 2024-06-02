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

def write_to_log(symbol,message):
    current_date = dt.now().strftime("%Y-%m-%d")
    log_file_name = f"{LOGS_FOLDER}/{symbol}-{current_date}.log"
    with open(log_file_name, "a") as log_file:
        log_file.write(str(dt.now())+"\n"+message)

def send_telegram_message(message):
    if Global.SEND_TELEGRAM_MESSAGE:
        url = f"https://api.telegram.org/bot{Global.TELEGRAM_TOKEN}/sendMessage?chat_id={Global.TELEGRAM_CHATID}&text={message}"
        requests.get(url)
    
def set_config(symbol, trend):
    end_time_in_millis = int(time.time() * 1000)
    end_time = datetime.datetime.fromtimestamp(end_time_in_millis / 1000)
    start_time = end_time - datetime.timedelta(days=2) #to change have 0
    start_time = start_time.replace(hour=9, minute=0, second=0, microsecond=0)
    start_time_in_millis = int(start_time.timestamp() * 1000)
    end_time_in_millis = int(end_time.timestamp() * 1000)
    close_price = None

    url = 'https://groww.in/v1/api/charting_service/v4/chart/exchange/NSE/segment/CASH/'+symbol
    params = {
        'endTimeInMillis': str(end_time_in_millis),
        'intervalInMinutes': 1,
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

    close_price = df.iloc[len(df)-1]['open']

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

def calculate_heikin_ashi(df):
    ha_df = pd.DataFrame(index=df.index, columns=['HA-Open', 'HA-High', 'HA-Low', 'HA-Close'])
    ha_df['HA-Close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
    ha_df.at[0, 'HA-Open'] = (df['open'].iloc[0] + df['close'].iloc[0]) / 2
    for i in range(1, len(df)):
        ha_df.at[i, 'HA-Open'] = (ha_df.at[i - 1, 'HA-Open'] + ha_df.at[i - 1, 'HA-Close']) / 2
        ha_df.at[i, 'HA-High'] = max(df['high'].iloc[i], ha_df.at[i, 'HA-Open'], ha_df.at[i, 'HA-Close'])
        ha_df.at[i, 'HA-Low'] = min(df['low'].iloc[i], ha_df.at[i, 'HA-Open'], ha_df.at[i, 'HA-Close'])
    result_df = pd.concat([df, ha_df], axis=1)
    return result_df

def trade_symbol(symbol, trend):
    try:
        end_time_in_millis = int(time.time() * 1000)
        end_time = datetime.datetime.fromtimestamp(end_time_in_millis / 1000)

        start_time = end_time - datetime.timedelta(days=3)
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
        ohlc.drop(['volume'], axis=1, inplace=True)
        ohlc = calculate_heikin_ashi(ohlc)

        del response, json_data, df

        last_index = len(ohlc)-1
        ohlc_open = ohlc.iloc[last_index]['open']
        prev_close = ohlc.iloc[last_index-1]['HA-Close']
        prev_open = ohlc.iloc[last_index-1]['HA-Open']
        if trend == "CE":
            take_position = "CALL" 
        else:
            take_position = "PUT"

        del ohlc

        if prev_close > prev_open and Global.SYMBOL_SETTINGS[symbol]["DAILY_PL"] < Global.SYMBOL_SETTINGS[symbol]["DAILY_PL_LIMIT"] and Global.SYMBOL_SETTINGS[symbol]["DAILY_PL"] > -Global.SYMBOL_SETTINGS[symbol]["DAILY_PL_LIMIT"]: #CE Entry

            if Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == False:
                Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = True
                Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = ohlc_open
                DhanMethods.place_order(symbol, take_position, BUY)
                print("Buy: "+str(Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"]))
                write_to_log(symbol,symbol+" Entry: "+ str(Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"]))

        #Exit Position
        if Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == True:
            
            if prev_close < prev_open or ohlc_open <= (Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"]-Global.SYMBOL_SETTINGS[symbol]["STOP_LOSS"]) or ohlc_open >= (Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"]+Global.SYMBOL_SETTINGS[symbol]["TAKE_PROFIT"]):
                Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = False
                DhanMethods.place_order(symbol, take_position, SELL)
                profit_loss = ohlc_open - Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"]
                Global.SYMBOL_SETTINGS[symbol]["DAILY_PL"] += profit_loss
                write_to_log(symbol,symbol+" Exit: "+str(ohlc_open)+", P/L: "+str(int(profit_loss)))
                Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = None

    except Exception as e:
        send_telegram_message("Error in trade symbol: "+str(e))
        raise BotException("Error in trade symbol: "+str(e))

def exit_open_trade(symbol, trend):
    if trend == "CE":
        take_position = "CALL" 
    else:
        take_position = "PUT"
    Global.SYMBOL_SETTINGS[symbol]["OPTION_ID"] = None
    send_telegram_message(symbol+" "+trend+" Daily P/L: " +str(Global.SYMBOL_SETTINGS[symbol]["DAILY_PL"]))
    write_to_log(symbol, symbol+" Daily P/L: "+str(Global.SYMBOL_SETTINGS[symbol]["DAILY_PL"]))
    Global.SYMBOL_SETTINGS[symbol]["DAILY_PL"] = None

    if(Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == True):
        DhanMethods.place_order(symbol, take_position, SELL)
        Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = False
        send_telegram_message("Exit EOD")
        Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = None
        Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"] = None
