import csv
import requests
import time
from bs4 import BeautifulSoup
import pandas as pd
import yfinance as yf
from statistics import mean, stdev, median
import math
import pandas_ta as pta
import json

# Get list of stock tickers
## Get only large tickers in a dataframe and export to csv
## Get only mega tickers in a dataframe and export to csv
## Combine them in a dataframe and export to csv

# Get monthly prices for the tickers in a dataframe and export to csv
# Calculate daily, weekly, fortnightly, monthly change
# Calculate weekly,fortnightly, monthly stdev, monthly HV stdev, rsi
# Get historical implied volatility (HIMV)
# Volatility check (if HV is higher than HIMV good signal)
# Check for options availability



class Tickers:

    def __init__(self):
        self.large = "https://finviz.com/screener.ashx?v=111&f=cap_large"
        self.mega = "https://finviz.com/screener.ashx?v=111&f=cap_mega"

    def large_tickers(self):
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/96.0.4664.110 Safari/537.36',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/'
                      'apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}
        large_tickers = []

        while True:
            r = requests.get(self.large, headers=headers)
            html = BeautifulSoup(r.text, "html.parser")

            for a in html.select('table[id="screener-views-table"] a.screener-link-primary'):
                large_tickers.append(a.text)

            if html.select_one('.tab-link:-soup-contains("next")'):
                self.large = "https://finviz.com/" + html.select_one('.tab-link:-soup-contains("next")')['href']
            else:
                break
            time.sleep(1)
            df_large_tickers = pd.DataFrame(large_tickers, columns=['Tickers'])
            df_large_tickers.to_csv('large_tickers.csv', index=False)

    def mega_tickers(self):
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/96.0.4664.110 Safari/537.36',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/'
                      'apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}
        mega_tickers = []

        while True:
            r = requests.get(self.mega, headers=headers)
            html = BeautifulSoup(r.text, "html.parser")

            for a in html.select('table[id="screener-views-table"] a.screener-link-primary'):
                mega_tickers.append(a.text)

            if html.select_one('.tab-link:-soup-contains("next")'):
                self.mega = "https://finviz.com/" + html.select_one('.tab-link:-soup-contains("next")')['href']
            else:
                break
            time.sleep(1)
        df_mega_tickers = pd.DataFrame(mega_tickers, columns=['Tickers'])
        df_mega_tickers.to_csv('mega_tickers.csv', index=False)

    def comb_tickers(self):
        self.large_tickers()
        df_large = pd.read_csv('large_tickers.csv')
        self.mega_tickers()
        df_mega = pd.read_csv('mega_tickers.csv')
        comb = [df_large, df_mega]
        comb_tickers = pd.concat(comb)
        comb_tickers.to_csv('tickers_list.csv', index=False)


class Sharesprices:

    def __init__(self):
        self.weekly = '7d'
        self.fortnight = '15d'
        self.monthly_adj = '35d'
        self.average = 'average'
        self.median = 'median'
        self.stdev = 'stdev'
        self.rsi = 'rsi'

    def monthly_prices(self):
        tickers = pd.read_csv('tickers_list.csv')
        tick = tickers['Tickers']
        m_prices = []
        for i in tick:
            m_prices.append(yf.Ticker(i).history(period=self.monthly_adj)['Close'])
        monthly_prices_dict = {tick[i]: m_prices[i] for i in range(len(tick))}
        df_prices = pd.DataFrame.from_dict(monthly_prices_dict)
        df_prices.reset_index(inplace=True)
        df_prices['Date'] = df_prices['Date'].dt.strftime('%d/%m/%Y')
        df_prices.to_csv('shares_prices.csv', index=False, header=True)
        return monthly_prices_dict


    def stats(self):
        daily_change = []
        weekly_change = []
        fortnightly_change = []
        monthly_change = []
        weekly_std = []
        fortnightly_std = []
        monthly_std = []
        monthly_HV = []
        rsi = []

        df_tickers = pd.read_csv('tickers_list.csv')
        df_prices = pd.read_csv('shares_prices.csv')

        for i in df_tickers['Tickers']:
            try:
                daily_change.append(df_prices[i].pct_change().iloc[-1])
            except Exception as e:
                continue

        for i in df_tickers['Tickers']:
            try:
                weekly_change.append(mean(df_prices[i].pct_change(7)[7:]))
            except Exception as e:
                continue

        for i in df_tickers['Tickers']:
            try:
                fortnightly_change.append(mean(df_prices[i].pct_change(14)[14:]))
            except Exception as e:
                continue

        for i in df_tickers['Tickers']:
            try:
                monthly_change.append(mean(df_prices[i].pct_change(30)[30:]))
            except Exception as e:
                continue

        for i in df_tickers['Tickers']:
            try:
                weekly_std.append(stdev(df_prices[i].pct_change(7)[7:]))
            except Exception as e:
                continue

        for i in df_tickers['Tickers']:
            try:
                fortnightly_std.append(stdev(df_prices[i].pct_change(14)[14:]))
            except Exception as e:
                continue

        for i in df_tickers['Tickers']:
            try:
                monthly_std.append(stdev(df_prices[i].pct_change(30)[30:]))
            except Exception as e:
                continue

        for i in monthly_std:
            try:
                monthly_HV.append(i*math.sqrt(252))
            except Exception as e:
                continue

        for i in df_tickers['Tickers']:
            try:
                rsi.append(median(pta.rsi(df_prices[i], length=14)))
            except Exception as e:
                continue

        return daily_change,weekly_change,fortnightly_change,monthly_change,weekly_std,fortnightly_std,monthly_std,\
               monthly_HV,rsi


    def hist_imp_vol(self):
        df_tickers = pd.read_csv('tickers_list.csv')
        df_tickers = df_tickers['Tickers'].values.tolist()

        hist_vol_monthly = []
        test = []
        for i in df_tickers:
            url = 'https://www.alphaquery.com/data/option-statistic-chart?ticker=' + i + '&perType=30-Day&identifier=iv-mean'
            if requests.get(url).ok:
                t = json.loads(requests.get(url).text)[-21:]
                for j in range(0, 21):
                    if t[j]['value'] == None:
                        test.append(0)
                    else:
                        test.append(t[j]['value'])
                hist_vol_monthly.append(mean(test))
            else:
                hist_vol_monthly.append(0)

        return hist_vol_monthly

    def vol_check(self):
        HV_IV_indicator =[]
        HV = self.stats()[7]
        IV = self.hist_imp_vol()
        for i in range(0,len(HV)):
            try:
                if HV[i] > IV[i]:
                    HV_IV_indicator.append('Good')
                else:
                    HV_IV_indicator.append('No')
            except Exception as e:
                continue
        return HV_IV_indicator

    def options(self):
        opt_dates_1 = []
        opt_dates_2 = []
        df_tickers = pd.read_csv('tickers_list.csv')
        for i in df_tickers['Tickers']:
            try:
                opt_dates_1.append(yf.Ticker(i).options[0])
            except Exception as e:
                opt_dates_1.append('Not available')
                continue
            try:
                opt_dates_2.append(yf.Ticker(i).options[1])
            except Exception as e:
                opt_dates_2.append('Not available')
                continue
        return opt_dates_1, opt_dates_2

    def final(self):
        df_tickers = pd.read_csv('tickers_list.csv')
        df_final = pd.DataFrame(list(zip(df_tickers['Tickers'],self.stats()[0],self.stats()[1],self.stats()[2],
                                         self.stats()[3],self.stats()[4],self.stats()[5],self.stats()[6],self.stats()[7],
                                         self.stats()[8],self.hist_imp_vol(),self.vol_check(),self.options()[0],self.options()[1])),
                                columns=['ticker','daily_change','weekly_change','fortnightly_change','monthly_change',
                                         'weekly_std','fortnightly_std','monthly_std','monthly_HV','monthly_rsi',
                                         'hist_imp_vol_monthly','HV_IV indicator','opt_dates_1','opt_dates_2'])

        print(df_final)
        df_final.to_excel('raw_output.xlsx', index=False, header=True)


test = Sharesprices()
test.final()

