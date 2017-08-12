import luigi, abc
import pandas as pd
import numpy as np

# roll up and calculate monthly returns
class CalcReturns(luigi.Task):

    @abc.abstractproperty
    def requires(self):
        pass

    @abc.abstractproperty
    def output(self):
        pass

    def run(self):
        prices = pd.read_csv(self.input().path)

        # clean up dates for merge
        prices['Date'] = pd.to_datetime(prices.Date)
        prices['Month'] = prices.Date.apply(lambda x: int(x.strftime("%Y%m")))

        # get price for previous period
        prices.sort_values(by = ['Ticker', 'Date'], ascending = True, inplace = True)
        prices['Adj Close Lag1'] = prices.groupby(['Ticker'])['Adj Close'].transform(lambda x:x.shift(1))

        # filter out any NaN / infinite returns; export
        prices = prices.loc[(prices['Adj Close Lag1'] != 0) & ~prices['Adj Close Lag1'].isnull() & ~prices['Adj Close'].isnull(),:]
        prices['Return'] = (prices['Adj Close'] / prices['Adj Close Lag1'] - 1)*100
        prices.to_csv(self.output().path, index = False)
