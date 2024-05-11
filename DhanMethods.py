import pandas as pd
import gc
from datetime import datetime

def find_matching_security_ids(strike_price, option_type, symbol, chunk_size=1000):
    matching_records = []
    csv_file_path = "SecurityInfo/api-scrip-master.csv"
    cur_month_year = datetime.now().strftime("%B%Y")
    trading_symbol = f"{symbol}-{cur_month_year}-{strike_price}-{option_type}"

    for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size, usecols=[
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
    
    del df
    
    return closest_expiry["SEM_SMST_SECURITY_ID"]


# print(find_matching_security_ids(22000, "CE", "NIFTY"))