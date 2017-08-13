import luigi, abc, math
import pandas as pd
import numpy as np
from sklearn import linear_model

# calculate factor tilts for each asset
class CalcTilts(luigi.Task):

    @abc.abstractproperty
    def requires(self):
        pass

    @abc.abstractproperty
    def output(self):
        pass

    @abc.abstractproperty
    def meta(self):
        pass

    def make_model(self, df, predx):
        # set up x and y
        xvar, yvar = self.meta['XVAR'], self.meta['YVAR']
        xdata, ydata = df[xvar], df[yvar]

        # run model
        reg = linear_model.Lasso(alpha = self.meta['ALPHA'])
        reg.fit(df[xvar], df[yvar])

        # generate predictions
        predx_avg = (predx[xvar].mean() + predx[xvar].median())/2
        predx_covar = np.cov(predx[xvar], rowvar = False)
        pred1 = reg.predict(df[xvar])
        pred2 = reg.predict([ predx_avg ])[0]

        # prepare output
        out = dict(zip(xvar, reg.coef_))
        out['Alpha'] = reg.intercept_
        out['Residual_Variance'] = np.var(df[yvar].values - pred1)
        out['Expected_Return'] = 100*((1 + pred2/100)**12 - 1)
        out['Expected_Var'] = 12 * (np.dot(np.dot(reg.coef_.reshape(1,-1), predx_covar), reg.coef_.reshape(-1,1))[0][0] + out['Residual_Variance'])
        out['Expected_SD'] = math.sqrt(out['Expected_Var'])
        out['Expected_Sharpe'] = out['Expected_Return'] / out['Expected_SD']
        return out

    def run(self):
        # bring in inputs and join
        returns = pd.read_csv(self.input()['returns'].path)
        factors = pd.read_csv(self.input()['factors'].path)
        returns2 = returns.merge(factors, how="inner", on=["Month"])

        # calculate tilts and expected return / variance / sharpe
        returns3 = [{'Ticker':i, **self.make_model(j, factors)} for i, j in returns2.groupby('Ticker')]
        returns3 = pd.DataFrame.from_dict(returns3)

        # calculate actuals
        returns4 = returns2.sort_values(by = ['Ticker', 'Month'], ascending = True)
        returns4['Cumulative_Return'] = returns4.groupby('Ticker')['Return'].transform(lambda x: (1 + x/100).cumprod())
        returns4['Cumulative_Return_Max'] = returns4.groupby('Ticker')['Cumulative_Return'].cummax()
        returns4['Draw_Down'] = 100*(1 - returns4.Cumulative_Return / returns4.Cumulative_Return_Max)

        returns4 = returns4.groupby('Ticker', as_index = False)
        aggegations = {
            'Return': {
                'Actual_Return': lambda x: 100*(np.prod((1 + x/100))**(12/x.size) - 1),
                'Actual_SD': lambda x: math.sqrt(12) * np.std(x)
            },
            'Draw_Down': {
                'Max_Draw_Down': np.max
            }
        }
        returns4 = returns4.agg(aggegations)
        returns4.columns = ['Ticker'] + [x[1] for x in returns4.columns.ravel()[1:]]

        returns4['Actual_Var'] = returns4.Actual_SD**2
        returns4['Actual_Sharpe'] = returns4.Actual_Return / returns4.Actual_SD

        #export
        returns5 = returns3.merge(returns4, how="inner", on=["Ticker"])
        returns5.to_csv(self.output().path, index = False)
