import pandas as pd
import numpy as np



def populate_features(df: pd.DataFrame, timeframe: str):
    df = add_volume_features(df, timeframe)
    df = add_emas(df, [20,50,100,200])
    df = add_crossovers(df, 'EMA')
    df = add_candlepatterns(df)
    df = add_fractals(df)
    df = add_pivot_data(df)
    return df


def add_volume_features(df, timeframe):
    """
    returns a df with spikes/no spikes (1/0) and spike ratios
    todo: avgVolume parametrize window, rvol by time of day
    """

    spike_multiple_dict = {
        '1m': 10,
        '5m': 2,
        '15m': 2,
        '1h': 2,
        '4h': 2,
        '1d': 2
    }
    spikeMultiple = spike_multiple_dict.get(timeframe)
    df["avgVolume"] = df["Volume"].rolling(window=20).mean()
    df["VolumeSpikeSignal"] = np.where(df["Volume"] > spikeMultiple*df["avgVolume"], 1, 0)
    df["spikeRatio"] = df["Volume"]/df["avgVolume"]
    df["spikeRatioWarning"] = np.where(df['spikeRatio'] > 1, 1, 0)
    # rvol by time of day: https://stackoverflow.com/questions/74920694/how-to-calculate-relative-volume-using-pandas-with-faster-way
    return df


def add_emas(df, emas):
    for ema in emas:
        col_name = "EMA"+str(ema)
        df[col_name] = df['Close'].rolling(window=ema).mean()
    return df


#aufruf' add_crossovers(df, "EMA")
def add_crossovers(df, indicator):
    """
    returns a df with trend and crossover columns
    """

    collist = df.columns.tolist()
    indicators = []
    for col in collist:
        if col.startswith(indicator):
            indicators.append(col)

    for i1 in indicators:
        i1_input = int(i1.replace(indicator, ''))
        for i2 in indicators:
            i2_input = int(i2.replace(indicator, ''))
            if (i1_input < i2_input):
                trend_col = i1+"Trend"+i2
                cross_col = i1+"Cross"+i2
                trendmask = df[i1] > df[i2]
                df[trend_col] = np.where(trendmask, 1,-1)
                df[cross_col] = df[trend_col] != df[trend_col].shift(1)
            else:
                pass
            
    return df







"""
untestested from here
"""

def add_candlepatterns(df):
    #1candle patterns
    add_hammers(df)
    add_shooting_stars(df)
    #2candle patterns
    add_inside_bars(df)
    #3candle patterns
    ## add_crows(df)
    ## add_soldies(df)
    return df


def add_inside_bars(df):    
    df["CDLInsideBar"] = np.where(((df["High"]<df["High"].shift(1)) & (df["Low"]>df["Low"].shift(1))), 1, 0)
    return df


def add_hammers(df):
    # df["HammerCDL"] = np.where(df["Open"]<df["Close"])
    #effiency...
    isHammer=[]
    for ix, row in df.iterrows():
        if (row['High']>=row['Close']) & (row['Close']>row['Open']) & (row['Open']>row['Low']):
            if (abs(row['High']-row['Open'])<abs(row['Open']-row['Low'])) & (abs(row['High']-row['Close'])<abs(row['Close']-row['Open'])):
                isHammer.append(1)
            else:
                isHammer.append(0)
        else:
                isHammer.append(0)
    #pd.toSeries mglciherweiser
    df['CDLHammer'] = isHammer
    return df


def add_shooting_stars(df):
    #efficiency...
    isShootingStar =[]
    for ix, row in df.iterrows():
        if (row['Low']<=row['Close']) & (row['Close']<row['Open']) & (row['Open']<row['High']):
            if (abs(row['High']-row['Open'])>abs(row['Open']-row['Low'])) & (abs(row['Close']-row['Low'])<abs(row['Close']-row['Open'])):
                isShootingStar.append(1)
            else:
                isShootingStar.append(0)
        else:
                isShootingStar.append(0)
    #pd.toSeries mglciherweiser
    df['CDLShootingStar'] = isShootingStar
    return df

def bullish_engulfing(df):
    
    return df

def bearish_engulfing(df):

    return df

def add_crows(df):
    #df["3Crows"] = np.where()
    return df

def add_soldiers(df):

    return df


def add_fractals(df: pd.DataFrame, period: int = 2) -> pd.DataFrame:
    #def add_fractals(df: pd.DataFrame, period: int = 2) -> Tuple[pd.Series, pd.Series]:
    """returns df with williams fractals based on input x (x=bars to consider)"""
    #https://codereview.stackexchange.com/questions/259703/william-fractal-technical-indicator-implementation
    """Indicate bearish and bullish fractal patterns using shifted Series.

    :param df: OHLC data
    :param period: number of lower (or higher) points on each side of a high (or low)
    :return: tuple of boolean Series (bearish, bullish) where True marks a fractal pattern
    """

    periods = [p for p in range(-period, period + 1) if p != 0] # default [-2, -1, 1, 2]

    highs = [df['High'] > df['High'].shift(p) for p in periods]
    #bears = pd.Series(np.logical_and.reduce(highs), index=df.index)
    df["BearFractal"] = pd.Series(np.logical_and.reduce(highs), index=df.index)

    lows = [df['Low'] < df['Low'].shift(p) for p in periods]
    #bulls = pd.Series(np.logical_and.reduce(lows), index=df.index)
    df["BullFractal"] = pd.Series(np.logical_and.reduce(lows), index=df.index)
    
    #return bears, bulls
    return df 


 
def add_pivot_data(df):

    def calculate_pivots(df):
        """returns pivot points for daily and weekly timeframes only"""
        #https://quantnomad.com/implementing-pivot-reversal-strategy-in-python-with-vectorbt/

        df['Pivot'] = (df['High'] + df['Low'] + df['Close'])/3
        df['R1'] = (2*df['Pivot']) - df['Low']
        df['S1'] = (2*df['Pivot']) - df['High']
        df['R2'] = (df['Pivot']) + (df['High'] - df['Low'])
        df['S2'] = (df['Pivot']) - (df['High'] - df['Low'])
        df['R3'] = (df['R1']) + (df['High'] - df['Low'])
        df['S3'] = (df['S1']) - (df['High'] - df['Low'])
        df['R4'] = (df['R3']) + (df['R2'] - df['R1'])
        df['S4'] = (df['S3']) - (df['S1'] - df['S2'])
        return df

    def shift_pivots(data):
        data["Pivot"] = data["Pivot"].shift(1)
        data["R1"] = data["R1"].shift(1)
        data["R2"] = data["R2"].shift(1)
        data["S1"] = data["S1"].shift(1)
        data["S2"] = data["S2"].shift(1)
        return data

    def drop_all_but_pivotcols(data):
        data.dropna(inplace=True)
        data = data[["Pivot", "S1", "S2", "R1", "R2"]]
        return data

    def get_pivots(data, timeframe):
        agg_dict = {'Open': 'first', 
                    'High': 'max',
                    'Low': 'min',
                    'Close': 'last',
                    'Volume': 'sum' #mean
                }
        data = data.resample(timeframe).agg(agg_dict)
        data = calculate_pivots(data)
        data = shift_pivots(data)
        data = drop_all_but_pivotcols(data)
        collist = data.columns.tolist()
        for col in collist:
            data = data.rename(columns={col: col+"_"+timeframe})
        return data

    def merge(df1, df2, resample_flag):
        df1["time"] = df1.index
        df2["time"] = df2.index
        merged_df = pd.merge(df1.assign(grouper=df1['time'].dt.to_period(resample_flag)),
                            df2.assign(grouper=df2['time'].dt.to_period(resample_flag)),
                            how='left', on='grouper')
        merged_df["Time"] = merged_df["datetimes"]
        merged_df.set_index("Time", inplace=True)
        merged_df = merged_df.drop(columns=['grouper','time_x','time_y'])    
        return merged_df


    daily_pivots = get_pivots(df, "D")
    df_with_daily_pivots = merge(df, daily_pivots, "D")

    weekly_pivots = get_pivots(df, "W")
    df_with_daily_weekly_pivots = merge(df_with_daily_pivots, weekly_pivots, "W")
    
    monthly_pivots = get_pivots(df, "M")
    df_with_daily_weekly_monthly_pivots = merge(df_with_daily_weekly_pivots, monthly_pivots, "M")
    
    return df_with_daily_weekly_monthly_pivots