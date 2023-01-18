import requests
import time
from bs4 import BeautifulSoup
import pandas as pd
import yahoo_finance_async as yf

from statistics import mean, stdev, median
import math
import pandas_ta as pta
import json, datetime

import asyncio, aiohttp



class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'to_json'):
            return obj.to_json(orient='records')
        return json.JSONEncoder.default(self, obj)

class Tickers:
    def __init__(self):
        self.base = "https://finviz.com/"
        self.headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/96.0.4664.110 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/'
              'apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}


    async def fetch_tickers(self, url: str) -> str:
        """Fetches Ticker - Small While Loop as Cloudflare tends to limit requests per second from Ip/device"""
        async with aiohttp.ClientSession(headers=self.headers) as session:
            ## assuming it'll be a 429 response becaue cloudflare is silly....
            status = 429
            while status == 429:
                async with session.get(self.base + url) as response:
                    status = response.status
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, "html.parser")
                        return list([x.text for x in soup.select('table[id="screener-views-table"] a.screener-link-primary')])
                    else:
                        await asyncio.sleep(0.1)

    async def bulk_func_asyncer(self, func, data, flatten=True):
        """Asyncio Decorator - used to map function against the data"""
        response = await asyncio.gather(*map(func, data))
        if flatten == True:
            return [item for sublist in response for item in sublist]
        else:
            return response

    async def bulk_fetch(self, url_list):
        return await self.bulk_func_asyncer(self.fetch_tickers, url_list)

    def make_ticker_lists(self, category:str):

        ### getting base page
        r = requests.get(self.base + "screener.ashx?v=111&f=cap_{}".format(category), headers=self.headers)
        html = BeautifulSoup(r.text, "html.parser")

        ### Finding how many additional pages of table there is
        maxPageNumber = max([int(x.text) for x in html.select('.screener-pages')])

        ## Generating list of links to async
        links = ["screener.ashx?v=111&f=cap_{}".format(category), ]
        list([links.append("screener.ashx?v=111&f=cap_{}&r={}".format(category, page * 20 + 1)) for page in range(1, maxPageNumber)])
        return links

    def get_large_tickers(self, save:bool = False):
        """Get List of links to parrellelise"""
        links = self.make_ticker_lists("large")

        """Asyncing the list of links"""
        # self.large_tickers = pd.DataFrame(asyncio.run(self.bulk_fetch(links)), columns=['Tickers'])
        self.large_tickers = pd.DataFrame(asyncio.get_event_loop().run_until_complete(self.bulk_fetch(links)), columns=['Tickers'])


        """Saving to csv.."""
        if save == True:
            self.large_tickers.to_csv('large_tickers.csv', index=False)

    def get_mega_tickers(self, save:bool = False):
        """Get List of links to parrellelise"""
        links = self.make_ticker_lists("mega")

        """Asyncing the list of links to parrellelise"""
        # self.mega_tickers = pd.DataFrame(asyncio.run(self.bulk_fetch(links)), columns=['Tickers'])
        self.mega_tickers = pd.DataFrame(asyncio.get_event_loop().run_until_complete(self.bulk_fetch(links)), columns=['Tickers'])

        """Saving to csv.."""
        if save == True:
            self.mega_tickers.to_csv('mega_tickers.csv', index=False)

    def combine_tickers(self, load:bool = False):
        if load == False:
            self.get_large_tickers()
            self.get_mega_tickers()
        else:
            self.large_tickers = pd.read_csv('large_tickers.csv')
            self.mega_tickers = pd.read_csv('mega_tickers.csv')

        self.comb_tickers = pd.concat([self.large_tickers, self.mega_tickers])
        self.comb_tickers.to_csv('tickers_list.csv', index=False)


class Sharesprices(Tickers):

    def __init__(self):
        super().__init__()
        self.weekly = '7d'
        self.fortnight = '15d'
        self.monthly_adj = '35d'
        self.average = 'average'
        self.median = 'median'
        self.stdev = 'stdev'
        self.rsi = 'rsi'

    async def yf_Ticker(self, tick:str):
        """Asyncio Version of yf Ticker Grabber"""
        monthly_prices = await yf.OHLC.fetch(tick, interval=yf.Interval.DAY, history=yf.History.monthly_adj)

        if len(monthly_prices['candles']) != 35:
            """Incase Data is Missing - There are better ways to handle this using Pandas, but I didnt
            know how you wanted it handled."""
            blanky = list([(datetime.datetime.today() - datetime.timedelta(days=35-day)).date() for day in range(35)])
            datekeys = {d['datetime'].date(): [d['datetime'], d['close']] for d in monthly_prices['candles']}
            res = []
            for day in blanky:
                if day in datekeys:
                    res.append(datekeys[day])
                else:
                    res.append([datetime.datetime.fromisoformat(day.isoformat()), 0 ])
            return (tick, res)
        else:
            return (tick, list([[day['datetime'].strftime('%d/%m/%Y'), day['close']] for day in monthly_prices['candles']]))

    async def bulk_yf_Ticker(self, ticks):
        return await self.bulk_func_asyncer(self.yf_Ticker, ticks, flatten=False)

    def monthly_prices(self, load:bool = False, save:bool=True, jdump:bool=True):
        if load == True:
            self.large_tickers = pd.read_csv('large_tickers.csv')
            self.mega_tickers = pd.read_csv('mega_tickers.csv')
            self.comb_tickers = pd.read_csv('tickers_list.csv')
        else:
            self.combine_tickers()

        tick = self.comb_tickers['Tickers']

        # self.monthly_prices_dict = {TICK[0]: pd.DataFrame(TICK[1], columns=["Date", "Price"]) for TICK in asyncio.run(self.bulk_yf_Ticker(tick))}
        self.monthly_prices_dict = {TICK[0]: pd.DataFrame(TICK[1], columns=["Date", "Price"]) for TICK in asyncio.get_event_loop().run_until_complete(self.bulk_yf_Ticker(tick))}

        if save == True:
            if jdump == True:
                with open('shares_prices.json', 'w') as opened:
                    json.dump(self.monthly_prices_dict, opened, cls=JSONEncoder)
            else:
                [tickerData.to_csv('{}_shares_prices.csv'.format(ticker), index=False, header=True) for ticker, tickerData in self.monthly_prices_dict.items()]
        return self.monthly_prices_dict

    def load_monthly_prices_dict(self, jsoned:bool=True):
        if jsoned == True:
            self.monthly_prices_dict = {key: pd.read_json(value, convert_dates=False) for key, value in json.load(open(
                "shares_prices.json")).items()}
        else:
            self.monthly_prices_dict = pd.read_csv('shares_prices.csv')

    def stats(self, load:bool = False):
        import traceback

        self.daily_change = []
        self.daily_change2 = []
        self.daily_change3 = []


        self.weekly_change = []
        self.fortnightly_change = []
        self.monthly_change = []
        self.weekly_std = []
        self.fortnightly_std = []
        self.monthly_std = []
        self.monthly_HV = []
        self.rsi = []
        if load == True:
            df_tickers = pd.read_csv('tickers_list.csv')
            df_prices = pd.read_csv('shares_prices.csv')
            self.load_monthly_prices_dict()
        else:
            df_prices = self.monthly_prices()
            df_tickers = self.comb_tickers['Tickers']
            self.zz = df_tickers
            self.yy = df_prices

        for ticker, ticker_data in df_prices.items():
            try:
                # print("")
                self.daily_change.append(ticker_data.iloc[::1,1].pct_change())
                self.daily_change2.append(ticker_data.iloc[::1,1].pct_change().iloc[1])
                self.daily_change3.append(ticker_data.iloc[::1,1].pct_change().iloc[1:,])

                self.weekly_change.append(ticker_data.iloc[::7,1].pct_change().iloc[1:,])

                self.fortnightly_change.append(ticker_data.iloc[::14,1].pct_change().iloc[1:,])

                self.monthly_change.append(ticker_data.iloc[::30,1].pct_change().iloc[1:,])

                self.weekly_std.append(ticker_data.iloc[::7,1].std())

                self.fortnightly_std.append(ticker_data.iloc[::14,1].std())

                self.monthly_std.append(ticker_data.iloc[::30,1].std())

                # rsi.append(median(pta.rsi(df_prices[i], length=14)))

            except Exception as e:
                print(e)
                print(traceback.format_exc(10))
                continue

        for i in self.monthly_std:
            try:
                self.monthly_HV.append(i*math.sqrt(252))
            except Exception as e:
                print(e)
                continue

        return self.daily_change, self.weekly_change, self.fortnightly_change, self.monthly_change, self.weekly_std, self.fortnightly_std, self.monthly_std,\
               self.monthly_HV
        # return self.daily_change, self.weekly_change, self.fortnightly_change, self.monthly_change, self.weekly_std, self.fortnightly_std, self.monthly_std,\
        #        self.monthly_HV,rsi

    async def fetch_hist_imp_vol(self, url: str) -> str:
        async with aiohttp.ClientSession(headers=self.headers) as session:
            ## assuming it'll be a 429 response becaue cloudflare is silly....
            status = 429
            attempts = 30
            while status != 200:
                async with session.get(url) as response:
                    status = response.status
                    if response.status == 200:
                        res =  await response.text()
                        res = json.loads(res)[-21:]
                        Mean = mean(list([j['value'] if j['value'] != None else 0 for j in res ]))
                        return Mean
                    else:
                        if attempts < 30:
                            attempts += 1
                            await asyncio.sleep(0.1)
                        else:
                            status = 200
                            return 0

    async def bulk_vol_fetch(self, url_list):
        return await self.bulk_func_asyncer(self.fetch_hist_imp_vol, url_list, flatten=False)

    def hist_imp_vol(self, load:bool=False):
        if load == True:
            self.load_monthly_prices_dict()
        else:
            self.monthly_prices()


        urls = list(['https://www.alphaquery.com/data/option-statistic-chart?ticker=' + i + '&perType=30-Day&identifier=iv-mean' for i in self.monthly_prices_dict])
        self.hist_vol_monthly = asyncio.get_event_loop().run_until_complete(self.bulk_vol_fetch(urls))

        return self.hist_vol_monthly

    def vol_check(self):
        self.HV_IV_indicator =[]
        self.HV = self.stats()[7]
        self.IV = self.hist_imp_vol()
        for i in range(0,len(self.HV)):
            try:
                if self.HV[i] > self.IV[i]:
                    self.HV_IV_indicator.append('Good')
                else:
                    self.HV_IV_indicator.append('No')
            except Exception as e:
                print(e)
                continue
        return self.HV_IV_indicator

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




