import pandas as pd
import gc
from datetime import datetime
import pandas as pd
import requests
import uuid
import Global
import datetime

CSV_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
OUTPUT_FILE = "SecurityInfo/SecurityID.csv"
DHAN_API_URL = "https://api.dhan.co/orders"
LOGS_FOLDER = "SecurityInfo"
SHORT = "PE"
LONG = "CE"
BUY = "BUY"
SELL = "SELL"


def find_matching_security_ids(strike_price, option_type, symbol, chunk_size=1000):
    matching_records = []
    cur_month_year = datetime.now().strftime("%B%Y")
    trading_symbol = f"{symbol}-{cur_month_year}-{strike_price}-{option_type}"

    for chunk in pd.read_csv(OUTPUT_FILE, chunksize=chunk_size, usecols=[
        "SEM_TRADING_SYMBOL",
        "SEM_EXPIRY_DATE",
        "SEM_SMST_SECURITY_ID",
    ]):
        df_filtered = chunk[ chunk["SEM_TRADING_SYMBOL"] == trading_symbol ]
        if not df_filtered.empty:
            matching_records.extend(df_filtered[["SEM_SMST_SECURITY_ID", "SEM_EXPIRY_DATE"]].to_dict("records"))

        del chunk, df_filtered
        gc.collect()

    df = pd.DataFrame(matching_records)    
    df["SEM_EXPIRY_DATE"] = pd.to_datetime(df["SEM_EXPIRY_DATE"])
    current_date = datetime.now().date()    
    df_future = df[df["SEM_EXPIRY_DATE"].dt.date > current_date]    
    closest_expiry = df_future.loc[df_future["SEM_EXPIRY_DATE"].idxmin()]    
    
    del df, df_future
    
    return closest_expiry["SEM_SMST_SECURITY_ID"]


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
def place_order(symbol):
    headers = {
        "Content-Type": "application/json",
        "access-token": Global.DHAN_TOKEN
    }
    
    correlation_id = str(uuid.uuid4())
    if symbol == Global.NIFTY:
        quantity = 25
    elif symbol == Global.BANKNIFTY:
        quantity = 15
    else:
        raise ValueError("Unsupported symbol. Only 'NIFTY' and 'BANKNIFTY' are supported.")
    
    transaction_type = BUY if Global.SYMBOL_SETTINGS[symbol]["POSITION_TYPE"] in (LONG,SHORT) else SELL

    data = {
        "dhanClientId": Global.DHAN_CLIENT_ID,
        "correlationId": correlation_id,
        "transactionType": transaction_type,
        "exchangeSegment": "NSE_FNO",  # Set to NSE F&O as required
        "productType": "INTRADAY",  # As per the requirement
        "orderType": "MARKET",  # Always MARKET as per requirement
        "validity": "IOC",  # Immediate or Cancel as per requirement
        "tradingSymbol": symbol,
        "securityId": Global.SYMBOL_SETTINGS[symbol]["CURR_SECURITYID"],
        "quantity": quantity,  # Quantity based on symbol, as an integer
        "disclosedQuantity": "",
        "price": "",
        "triggerPrice": "",
        "afterMarketOrder": False,
        "amoTime": "",
        "boProfitValue": "",
        "boStopLossValue": "",
        "drvExpiryDate": "",
        "drvOptionType": "",
        "drvStrikePrice": ""
    }

    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    log_file_name = f"{LOGS_FOLDER}/{current_date}.log"
    log_entry = "Req: "+symbol+" "+transaction_type+" "+Global.SYMBOL_SETTINGS[symbol]['CURR_SECURITYID']+"\n"  
    
    try:
        if Global.DHAN_TOKEN != "":
            response = requests.post(DHAN_API_URL, headers=headers, json=data)
            log_entry += "Res: "+str(response)+"\n"
        else:
            print("Skipping place order")
    except Exception as e:
        log_entry += "Res_error: "+str(e)+"\n"
        print("Error in Place order: "+str(e))

    with open(log_file_name, "a") as log_file:
        log_file.write(str(datetime.datetime.now())+"\n"+log_entry)

    del data,response 

#response = place_order("NIFTY")
#filter_and_save_csv()
#print(find_matching_security_ids(22250, "CE", "NIFTY"))