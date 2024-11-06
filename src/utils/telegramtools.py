import requests
import pandas as pd
#from datetime import datetime, timezone, timedelta


def send2telegram(message, apiToken, chatID):
    """ send a message to the group chat"""
    apiURL = f"https://api.telegram.org/bot{apiToken}/sendMessage"
    params = {"chat_id": chatID, "text":message}

    try:
        response = requests.post(apiURL, params=params)
    except Exception as e:
        print(e)


def aggregate_message(message, ticker, timeframe, df):
    """cointains the logic to generate the telegram message"""
    last_candle = df.iloc[-2]
    penult_candle = df.iloc[-3]

    collist = df.columns.tolist()
    pivot_cols = ['Pivot_D','S1_D','S2_D','R1_D','R2_D',
                  'Pivot_W','S1_W','S2_W','R1_W','R2_W',
                  'Pivot_M','S1_M','S2_M','R1_M','R2_M']
        
    signals_count = 0
    warnings_count = 0
    #msg = msg + "\n" + ticker + " | " + timeframe
    msg = "\n" + ticker + " | " + timeframe


    if last_candle.VolumeSpikeSignal > 0:
        msg = msg + "\n- Signal: "+"Volume Spike "+"(Ratio: "+ str(round(last_candle.spikeRatio, 2))+")"
        signals_count+=1
    elif last_candle.spikeRatioWarning > 0:
        msg = msg + "\n- Warning: "+"Volume "+"(SpikeRatio: "+ str(round(last_candle.spikeRatio, 2))+")"
        warnings_count+=1


    signalname = "Cross"
    for col in collist:
        if signalname in col:
            if last_candle[col] == True:
                #trend_col = col.replace(signalname, "Trend")
                msg = msg + "\n- " + col #+ " Trend: "+ last_candle[trend_col]
                signals_count += 1


    if last_candle.CDLInsideBar > 0:
        msg = msg + "\n- InsideBar"
        signals_count += 1
    if last_candle.CDLHammer > 0:
        msg = msg + "\n- Hammer"
        signals_count += 1
    if last_candle.CDLShootingStar > 0:
        msg = msg + "\n- ShootingStar"
        signals_count += 1
    

    for col in collist:
        if col in pivot_cols:
            if (penult_candle['Close'] < penult_candle[col] and last_candle['Close'] > last_candle[col]):
                msg = msg + "\n- crossed above " + col 
                signals_count += 1
            if (penult_candle['Close'] > penult_candle[col] and last_candle['Close'] < last_candle[col]):
                msg = msg + "\n- crossed below " + col 
                signals_count += 1
    

    if signals_count == 0 and warnings_count == 0:
        return message, signals_count, warnings_count
    else:
        message = message + msg

    return message, signals_count, warnings_count