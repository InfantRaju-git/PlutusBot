import datetime
import time
import schedule
import BotMethods
import DhanMethods
import Global
import sys

def run_trade_bot(symbol, trend):
    if(trend not in ["CE","PE"]):
        raise Exception("Invalid Trend CE/PE")
    print("Running on: "+ symbol+", trend :"+trend)

    # Define the trading hours
    start_time = datetime.time(9, 15)
    end_time = datetime.time(15, 30)

    print("Security Extraction Job: 9:12:30")
    schedule.every().day.at(datetime.time(9, 12, 30).strftime("%H:%M:%S")).do(DhanMethods.filter_and_save_csv).tag('securityid')
    
    print("Security Extraction Job: 9:15:00")
    schedule.every().day.at(datetime.time(9, 15, 1).strftime("%H:%M:%S")).do(BotMethods.set_config, symbol=symbol, trend=trend).tag('config')

    for hour in range(start_time.hour, end_time.hour + 1):
        for minute in range(start_time.minute, 60, Global.SYMBOL_SETTINGS[symbol]["TRADE_TF"]):
            schedule_time = datetime.time(hour, minute, 5)
            if start_time <= schedule_time < end_time:
                print("Scheduling Trade Job", schedule_time)
                schedule.every().day.at(schedule_time.strftime("%H:%M:%S")).do(BotMethods.trade_symbol, symbol=symbol, trend=trend).tag('trade')

    print("Exit Trade Job: 3:29:00")
    schedule.every().day.at(datetime.time(15, 29, 0).strftime("%H:%M:%S")).do(BotMethods.exit_open_trade, symbol=symbol, trend=trend).tag('exittrade')
    
    while True:
        schedule.run_pending()
        time.sleep(1)

def main(symbol, trend):
    run_trade_bot(symbol, trend)

if __name__ == "__main__":
    main(str(sys.argv[1]),str(sys.argv[2]))