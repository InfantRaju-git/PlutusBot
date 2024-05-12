from datetime import datetime as dt
import datetime
from dateutil.rrule import rrule, WEEKLY
import requests
import pandas as pd
import time
import Global
import DhanMethods
from ta.trend import PSARIndicator

def BotException(exceptionMessage):
    send_telegram_message(exceptionMessage)
    raise Exception(exceptionMessage)

def unix_to_local_time(unix_timestamp):
    return str(dt.fromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S'))

def send_telegram_message(message):
    if Global.SEND_TELEGRAM_MESSAGE:
        url = f"https://api.telegram.org/bot{Global.TELEGRAM_TOKEN}/sendMessage?chat_id={Global.TELEGRAM_CHATID}&text={message}"
        requests.get(url)

def get_expiry_day_of_week(day_of_week):
    """Return the date for a specified day of the week within the current week."""
    today = datetime.date.today()
    rule = rrule(WEEKLY, dtstart=today, byweekday=day_of_week, count=1)
    return rule[0].date()

def create_option_symbol(index_name, option_type, strike_price):
    """Generate an option symbol for NIFTY, BANKNIFTY, or MIDCPNIFTY with the current week's expiration date."""
    if option_type not in ["PE", "CE"]:
        raise ValueError("Invalid option type. Use 'PE' for Put or 'CE' for Call.")

    if index_name == Global.NIFTY:
        expiry_date = get_expiry_day_of_week(3)  # Thursday (2: Monday, 3: Thursday)
    elif index_name == Global.BANKNIFTY:
        expiry_date = get_expiry_day_of_week(2)  # Wednesday
    elif index_name == Global.MIDNIFTY:
        expiry_date = get_expiry_day_of_week(0)  # Monday
    else:
        raise ValueError("Invalid index name. Use 'NIFTY', 'BANKNIFTY', or 'MIDCPNIFTY'.")

    year = expiry_date.year % 100  # Last two digits of the year
    month = expiry_date.month  # Month as an integer
    day = expiry_date.day  # Day as an integer

    expiry_str = f"{year:02d}{month}{day:02d}"
    strike_price_str = f"{strike_price:05d}"
    option_symbol = f"{index_name}{expiry_str}{strike_price_str}{option_type}"
    return option_symbol

def get_atm_strike(symbol, current_price, current_trend):
    if symbol == "NIFTY":
        increment = 50
    elif symbol == "BANKNIFTY":
        increment = 100
    else:
        raise BotException("Invalid symbol at get_atm_strike")

    if current_price is None:
        raise BotException("Current price is none in get_atm_strike")

    if current_trend == "CE":
        rounded_strike = (current_price // increment) * increment
    elif current_trend == "PE":
        rounded_strike = ((current_price + increment - 1) // increment) * increment
    elif current_trend is None:
        rounded_strike = 0
    else:
        raise BotException("Invalid current_trend value in get_atm_strike")

    return rounded_strike

def get_ohlc_data(symbol,isOptionChart):
    try:
        if isOptionChart:
            daysToOHLC = 7
        else:
            daysToOHLC = 40
        end_time_in_millis = int(time.time() * 1000)
        end_time = datetime.datetime.fromtimestamp(end_time_in_millis / 1000)
        start_time = end_time - datetime.timedelta(days= daysToOHLC)
        start_time = start_time.replace(hour=9, minute=0, second=0, microsecond=0)
        start_time_in_millis = int(start_time.timestamp() * 1000)
        
        if isOptionChart:
            optionType = Global.SYMBOL_SETTINGS[symbol]["CURR_TREND"]
            strikePrice = Global.SYMBOL_SETTINGS[symbol]["STRIKE"]
            optionName = create_option_symbol(symbol, optionType, strikePrice)
            url = 'https://groww.in/v1/api/stocks_fo_data/v3/charting_service/chart/exchange/NSE/segment/FNO/'+optionName
            timeframe = int(Global.SYMBOL_SETTINGS[symbol]["TRADE_TF"])
        else:
            url = 'https://groww.in/v1/api/charting_service/v4/chart/exchange/NSE/segment/CASH/'+symbol
            timeframe = 240
            
        params = {
            'endTimeInMillis': str(end_time_in_millis),
            'intervalInMinutes': str(timeframe),
            'startTimeInMillis': str(start_time_in_millis),
        }
        
        if isOptionChart:
            if(Global.SYMBOL_SETTINGS[symbol]["CURR_TREND"] == None):
                return
            else:
                response = requests.get(url, params=params)
        elif (isOptionChart == False):
            response = requests.get(url, params=params)
        
        if response.status_code == 200:
            json_data = response.json()
            df = pd.DataFrame(json_data)
        
        # Drop unwanted columns and Rename the columns to the desired format
        df.drop(['changeValue', 'changePerc', 'closingPrice', 'startTimeEpochInMillis'], axis=1, inplace=True)
        ohlc = df['candles'].apply(pd.Series)        
        ohlc.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        ohlc.drop(['volume'], axis=1, inplace=True)

        del response, json_data, df
        
        if isOptionChart:
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

            del ohlc

            if curr_close >= curr_open and prev_close >= prev_open and Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == False:
                Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = True
                Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = ohlc_open
                send_telegram_message("Entry: "+optionName+ " : " +str(ohlc_open))
    
            elif curr_close < curr_open and prev_close < prev_open and Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == True:
                Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = False
                send_telegram_message("Exit: "+optionName+ " : "  +str(ohlc_open)+" PL: "+ str(ohlc_open-Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"]))
                Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = None
        else:
            return ohlc
        
    except Exception as e:
        raise BotException("Error in get_ohlc_data: "+str(e))


def get_trend(symbol):
    try:
        gap_ema_close = 30
        ohlc_data = get_ohlc_data(symbol, False)
        ohlc_data['EMA_9'] = ohlc_data['close'].ewm(span=9, adjust=False).mean()
        
        last_index = len(ohlc_data)-1
        current_ema_9 = ohlc_data.iloc[last_index]['EMA_9']
        current_close = ohlc_data.iloc[last_index]['close']
        
        del ohlc_data
        
        if current_close > current_ema_9 and abs(current_close - current_ema_9) >= gap_ema_close: #CE Trend
            
            if(Global.SYMBOL_SETTINGS[symbol]["CURR_TREND"] == "PE" and Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == True): #Open trade in opposite dir
                Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = False
                send_telegram_message("Exit Trend Change: "  +str(current_close)+" PL: "+ str(current_close-Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"]))
                Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = None
            
            if(Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == False):
                Global.SYMBOL_SETTINGS[symbol]["CURR_CLOSE"] = int(current_close)
                Global.SYMBOL_SETTINGS[symbol]["STRIKE"] = get_atm_strike(symbol, int(current_close), "CE")
                Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"] = DhanMethods.find_matching_security_ids(Global.SYMBOL_SETTINGS[symbol]["STRIKE"], "CE", symbol)

            Global.SYMBOL_SETTINGS[symbol]["CURR_TREND"] = "CE"
            send_telegram_message(symbol +" Trend: "+ Global.SYMBOL_SETTINGS[symbol]["CURR_TREND"])


        elif current_close < current_ema_9 and abs(current_close - current_ema_9) >= gap_ema_close: #PE Trend
            
            if(Global.SYMBOL_SETTINGS[symbol]["CURR_TREND"] == "CE" and Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == True): #Open trade in opposite dir
                Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = False
                send_telegram_message("Exit Trend Change: "  +str(current_close)+" PL: "+ str(current_close-Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"]))
                Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = None
            
            if(Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == False):
                Global.SYMBOL_SETTINGS[symbol]["CURR_CLOSE"] = int(current_close)
                Global.SYMBOL_SETTINGS[symbol]["STRIKE"] = get_atm_strike(symbol, int(current_close), "PE")
                Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"] = DhanMethods.find_matching_security_ids(Global.SYMBOL_SETTINGS[symbol]["STRIKE"], "CE", symbol)
            
            Global.SYMBOL_SETTINGS[symbol]["CURR_TREND"] = "PE"
            send_telegram_message(symbol +" Trend: "+Global.SYMBOL_SETTINGS[symbol]["CURR_TREND"])
        
        else:
            Global.SYMBOL_SETTINGS[symbol]["CURR_TREND"] = None
            send_telegram_message(""+symbol+" Trend: "+Global.SYMBOL_SETTINGS[symbol]["CURR_TREND"]+" No Trade")

    except Exception as e:
            raise BotException("Error in get_trend: "+str(e))
    
def exit_open_trade(symbol):
    if(Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] == True):
        Global.SYMBOL_SETTINGS[symbol]["OPEN_POSITION"] = False
        send_telegram_message("Exit EOD")
        Global.SYMBOL_SETTINGS[symbol]["ENTRY_PRICE"] = None

