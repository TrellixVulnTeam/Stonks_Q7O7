#!/usr/local/bin/python3
import pandas_datareader as pdr
import datetime
import pandas as pd
import logging
import owned
logging.basicConfig(filename='logging.log', filemode='a',level=logging.INFO)


to_d = datetime.datetime.today()
from_d = to_d - datetime.timedelta(days=10)
to_d = to_d.strftime('%d-%m-%y')

stocks = owned.owned

get_data = lambda data :  data[['Close']]
get_current = lambda data :  int(data[-1:][['Close']].Close)
get_forecast = lambda data :  int(data['Close'].mean())
get_pct = lambda num,den : round(num/den - 1,2)
get_abs  = lambda first , second :  int(first - second)
fix_date = lambda date :  pd.to_datetime(date,format='%d-%m-%y')


def rund(stocks,day):
    colu = ['to_d',
            'stock',
            'current',
            'forecast',
            'buying',
            'quant',
            'g_act_abs',
            'g_act_pct',
            'g_exp_abs',
            'g_exp_pct'] # Columns definition
    lst = []

    for stock in stocks:
        data = pdr.get_data_yahoo(stock,start=from_d,end=day)
        #History
        actual,quant = stocks[stock]
        #Current & Forecast
        current = get_current(data)
        forecast = get_forecast(data)
        #gain actual
        g_act_abs  = quant * get_abs(current ,actual)
        g_act_pct = get_pct(current,actual)
        #gain_exp
        g_exp_pct = get_pct(forecast,current)
        g_exp_abs = quant * get_abs(forecast , current)

        lst.append([day,
                    stock,
                    current,
                    forecast,
                    actual,quant,
                    g_act_abs,
                    g_act_pct,
                    g_exp_abs,
                    g_exp_pct]) # Appending data

    return pd.DataFrame(lst, columns=colu)

def update_f(filename,to_d):
    try:
        df = pd.read_csv(filename)
    except (pd.errors.EmptyDataError,IOError):
        df = rund(stocks, to_d)
        df.to_csv(filename,
                  index=False,
                  date_format='%d-%m-%y')

    if df.empty: df = rund(stocks, to_d)

    else:
        df['to_d'] = fix_date(df['to_d'])
        to_d = fix_date(to_d)
        df = df[df['to_d'] != to_d]
        maxdate = df.to_d.max()
        #multiple days missed
        delta = to_d - maxdate
        #loop over the missing days
        for i in range(delta.days + 1):
            day = maxdate + datetime.timedelta(days=i)
            df = df[df['to_d'] != day]
            new_stocks = rund(stocks, day)
            df = df.append(new_stocks)

    df.to_csv(filename, index=False, date_format='%d-%m-%y')
    logging.info('Finished last update {},{} rows added '.format(datetime.datetime.today(),new_stocks.shape[0]))


if __name__ == '__main__':
    update_f('~/PycharmProjects/Stonks/stonks.csv',to_d)
