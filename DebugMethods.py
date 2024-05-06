def get_trend_debug(symbol):
    gap_ema_close = 30
    ohlc_data = [] #get_ohlc_data(symbol, False)
    ohlc_data['EMA_9'] = ohlc_data['close'].ewm(span=9, adjust=False).mean()
    
    trend = ['NO TREND']
    for i in range(1, len(ohlc_data)):
        previous_close = ohlc_data.iloc[i-1]['close']
        previous_ema_9 = ohlc_data.iloc[i-1]['EMA_9']
        current_close = ohlc_data.iloc[i]['close']
        current_ema_9 = ohlc_data.iloc[i]['EMA_9']
        
        if current_close > current_ema_9 and abs(current_close - current_ema_9) >= gap_ema_close:
            trend.append('CE')
        elif current_close < current_ema_9 and abs(current_close - current_ema_9) >= gap_ema_close:
            trend.append('PE')
        else:
            trend.append('NAN')
    del ohlc_data
    return trend[-1]

