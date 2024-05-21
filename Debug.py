import pandas as pd
import gc
from datetime import datetime
import pandas as pd
import requests
import uuid

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

#Required security id and position type
def place_order(symbol,transaction_type, security_id):
    headers = {
        "Content-Type": "application/json",
        "access-token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzE2ODY4NTk0LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMjk2MTg4NCJ9.4knrPvnSsERK-r6FevWQ7B56ggrjfvneKGUt2qd3cuY7gvkLgTk0MF-6K9dbGGVZ8C3HmsQZUO3ov0DSXYYpWA"
    }
    
    correlation_id = str(uuid.uuid4())
    if symbol == "NIFTY":
        quantity = 25
    elif symbol == "BANKNIFTY":
        quantity = 15
    else:
        raise ValueError("Unsupported symbol. Only 'NIFTY' and 'BANKNIFTY' are supported.")
    
    data = {
        "dhanClientId": "1102961884",
        "correlationId": correlation_id,
        "transactionType": transaction_type,
        "exchangeSegment": "NSE_FNO",  # Set to NSE F&O as required
        "productType": "INTRADAY",  # As per the requirement
        "orderType": "MARKET",  # Always MARKET as per requirement
        "validity": "IOC",  # Immediate or Cancel as per requirement
        "tradingSymbol": symbol,
        "securityId": security_id,
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

    try:
        response = requests.post(DHAN_API_URL, headers=headers, json=data)
        print(response)
    except Exception as e:
        print("Error in Place order: "+str(e))

    del data,response 

#place_order("NIFTY")
print(find_matching_security_ids(22500, "PE", "NIFTY"))