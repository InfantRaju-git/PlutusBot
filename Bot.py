import datetime
import time
import schedule
import BotMethods
import Global

def run_trade_bot(symbol):
    print("Running trade bot...")

    # Define the trading hours
    start_time = datetime.time(9, 15)
    end_time = datetime.time(15, 30)

    # Define the 4-hour sequence for get_trend
    trend_times = [datetime.time(9, 15, 30), datetime.time(13, 15, 30)]

    # Schedule get_trend method to run on the 4-hour sequence
    for trend_time in trend_times:
        schedule.every().day.at(trend_time.strftime("%H:%M:%S")).do(BotMethods.get_trend, symbol=symbol).tag('trend')

    Global.TRADE_TF = 15 #NIFTY
    if symbol == Global.BANKNIFTY:
        Global.TRADE_TF = 30

    # Schedule get_ohlc_data method to run every time_frame minutes between 9:15:30 AM and 3:30 PM
    for hour in range(start_time.hour, end_time.hour + 1):
        for minute in range(start_time.minute, 60, Global.TRADE_TF):
            schedule_time = datetime.time(hour, minute, 10)
            if start_time <= schedule_time < end_time:
                schedule.every().day.at(schedule_time.strftime("%H:%M:%S")).do(BotMethods.get_ohlc_data, symbol=symbol, isOptionChart=True).tag('ohlc')

    while True:
        schedule.run_pending()
        time.sleep(1)  # Delay to avoid consuming too many resources

def main():
    symbols = ["NIFTY"]
    for symbol in symbols:
        run_trade_bot(symbol)

if __name__ == "__main__":
    main()