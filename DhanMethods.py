import pandas as pd
import gc
from datetime import datetime
import pandas as pd

CSV_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
OUTPUT_FILE = "SecurityInfo/SecurityID.csv"


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


#filter_and_save_csv(CSV_URL, OUTPUT_FILE)
#print(find_matching_security_ids(22250, "CE", "NIFTY"))