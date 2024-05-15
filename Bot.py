import datetime
import time
import schedule
import BotMethods
import DhanMethods
import Global
import sys

def run_trade_bot(symbol):
    print("Running trade bot on: "+ symbol)

    # Define the trading hours
    start_time = datetime.time(9, 15)
    end_time = datetime.time(15, 30)

    print("Security Extraction Job: 9:10")
    schedule.every().day.at(datetime.time(9, 10, 0).strftime("%H:%M:%S")).do(DhanMethods.filter_and_save_csv).tag('securityid')

    # Schedule get_ohlc_data method to run every time_frame minutes between 9:15:30 AM and 3:30 PM
    for hour in range(start_time.hour, end_time.hour + 1):
        for minute in range(start_time.minute, 60, Global.SYMBOL_SETTINGS[symbol]["TRADE_TF"]):
            schedule_time = datetime.time(hour, minute, 7)
            if start_time <= schedule_time < end_time:
                print("Scheduling Trade Job", schedule_time)
                schedule.every().day.at(schedule_time.strftime("%H:%M:%S")).do(BotMethods.trade_symbol, symbol=symbol).tag('trade')

    print("Exit Trade Job: 3:28")
    schedule.every().day.at(datetime.time(15, 28, 30).strftime("%H:%M:%S")).do(BotMethods.exit_open_trade, symbol=symbol).tag('exittrade')
    
    while True:
        schedule.run_pending()
        time.sleep(1)

def main(symbol):
    run_trade_bot(symbol)

if __name__ == "__main__":
    main(str(sys.argv[1]))