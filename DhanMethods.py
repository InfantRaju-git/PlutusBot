import pandas as pd
import gc
from datetime import datetime, timedelta
import pandas as pd
import requests
import uuid
import Global

CSV_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
OUTPUT_FILE = "SecurityInfo/SecurityID.csv"
DHAN_API_URL = "https://api.dhan.co/orders"
LOGS_FOLDER = "SecurityInfo"
SHORT = "PUT"
LONG = "CALL"
BUY = "BUY"
SELL = "SELL"


def get_next_month_year():
    next_month = datetime.now().replace(day=28) + timedelta(days=4)  # this will never fail
    next_month = next_month.replace(day=1)  # set to first day of the next month
    return next_month.strftime("%b%Y")

def find_matching_security_ids(strike_price, option_type, symbol, chunk_size=1000):
    matching_records = []
    cur_month_year = datetime.now().strftime("%b%Y")
    next_month_year = get_next_month_year()

    if option_type == LONG:
        option_type = "CE" 
    elif option_type == SHORT:
        option_type = "PE"

    trading_symbol_current = f"{symbol}-{cur_month_year}-{strike_price}-{option_type}"
    trading_symbol_next = f"{symbol}-{next_month_year}-{strike_price}-{option_type}"

    def check_trading_symbol(trading_symbol):
        local_matching_records = []
        for chunk in pd.read_csv(OUTPUT_FILE, chunksize=chunk_size, usecols=[
            "SEM_TRADING_SYMBOL",
            "SEM_EXPIRY_DATE",
            "SEM_SMST_SECURITY_ID",
        ]):
            df_filtered = chunk[chunk["SEM_TRADING_SYMBOL"] == trading_symbol]
            if not df_filtered.empty:
                local_matching_records.extend(df_filtered[["SEM_SMST_SECURITY_ID", "SEM_EXPIRY_DATE"]].to_dict("records"))
            del chunk, df_filtered
            gc.collect()
        return local_matching_records

    # First check with the current month's value
    matching_records = check_trading_symbol(trading_symbol_current)
    findNextMatch = False

    if matching_records:
        df = pd.DataFrame(matching_records)
        df["SEM_EXPIRY_DATE"] = pd.to_datetime(df["SEM_EXPIRY_DATE"])
        current_date = datetime.now().date()
        df_future = df[df["SEM_EXPIRY_DATE"].dt.date >= current_date]
        if not df_future.empty:
            closest_expiry = df_future.loc[df_future["SEM_EXPIRY_DATE"].idxmin()]
            del df, df_future
            return [closest_expiry["SEM_SMST_SECURITY_ID"], closest_expiry["SEM_EXPIRY_DATE"]]
        else:
            findNextMatch = True

    # If no matching records, check with the next month's value
    if not matching_records or findNextMatch:
        matching_records = check_trading_symbol(trading_symbol_next)
        if matching_records:
            df = pd.DataFrame(matching_records)
            df["SEM_EXPIRY_DATE"] = pd.to_datetime(df["SEM_EXPIRY_DATE"])
            current_date = datetime.now().date()
            df_future = df[df["SEM_EXPIRY_DATE"].dt.date >= current_date]
            if not df_future.empty:
                closest_expiry = df_future.loc[df_future["SEM_EXPIRY_DATE"].idxmin()]
                del df, df_future
                return [closest_expiry["SEM_SMST_SECURITY_ID"], closest_expiry["SEM_EXPIRY_DATE"]]
    
    return None

def filter_and_save_csv():
    try:
        df = pd.read_csv(CSV_URL)
    except Exception as e:
        print("Error reading CSV:", e)
        return
    
    df_filtered = df[df['SEM_INSTRUMENT_NAME'] == 'OPTIDX']
    
    try:
        df_filtered.to_csv(OUTPUT_FILE, index=False)
        print("Filtered CSV saved successfully.")
    except Exception as e:
        print("Error saving filtered CSV:", e)

#Required security id and position type
def place_order(symbol, optionType, transaction_type):
    headers = {
        "Content-Type": "application/json",
        "access-token": Global.DHAN_TOKEN
    }
    
    correlation_id = str(uuid.uuid4()).replace("-","")[0:25]
    if symbol == Global.NIFTY:
        quantity = 25
    elif symbol == Global.BANKNIFTY:
        quantity = 15
    elif symbol == Global.FINNIFTY:
        quantity = 40
    else:
        raise ValueError("Unsupported symbol. Only 'NIFTY' and 'BANKNIFTY' are supported.")
    
    data = {
        "dhanClientId": Global.DHAN_CLIENT_ID,
        "correlationId": correlation_id,
        "transactionType": transaction_type,
        "exchangeSegment": "NSE_FNO",  # Set to NSE F&O as required
        "productType": "INTRADAY",  # As per the requirement
        "orderType": "MARKET",  # Always MARKET as per requirement
        "validity": "IOC",  # Immediate or Cancel as per requirement
        "tradingSymbol": symbol,
        "securityId": str(Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"]),
        "quantity": quantity,  # Quantity based on symbol, as an integer
        "disclosedQuantity": "",
        "price": "",
        "triggerPrice": "",
        "afterMarketOrder": False,
        "amoTime": "OPEN",
        "boProfitValue": "",
        "boStopLossValue": "",
        "drvExpiryDate": "",
        "drvOptionType": optionType,
        "drvStrikePrice": ""
    }

    current_date = datetime.now().strftime("%Y-%m-%d")
    log_file_name = f"{LOGS_FOLDER}/{current_date}.log"
    log_entry = str(datetime.now())+"\nReq: "+symbol+" "+transaction_type+" "+str(Global.SYMBOL_SETTINGS[symbol]['CURR_SECURITYID'])+"\n"  
    
    try:
        if Global.DHAN_TOKEN != "":
            response = requests.post(DHAN_API_URL, headers=headers, json=data)
            log_entry += "Res: "+str(response)+"\n"
            del response
    except Exception as e:
        log_entry += "Res_error: "+str(e)+"\n"
        print("Error in Place order: "+str(e))

    with open(log_file_name, "a") as log_file:
        log_file.write(log_entry)

    del data

#response = place_order("NIFTY")
#filter_and_save_csv()
#print(find_matching_security_ids(22250, "CE", "NIFTY"))