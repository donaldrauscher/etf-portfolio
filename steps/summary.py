import luigi, abc, math
import pandas as pd
import numpy as np

# calculate summary statistics for portfolios
class CalcSummary(luigi.Task):

    @abc.abstractproperty
    def requires(self):
        pass

    @abc.abstractproperty
    def output(self):
        pass

    def run(self):
        # bring in inputs
        returns = pd.read_csv(self.input()['etf-returns'].path)
        portfolios = pd.read_csv(self.input()['portfolios'].path)

        # aggregate returns for each portfolio-month
        returns2 = portfolios.merge(returns, how="left", on=["Ticker"])
        returns2['Return'] = returns2.Weight * returns2.Return
        returns2 = returns2.groupby(['Portfolio','Month'], as_index = False).agg({'Return':np.sum})
        returns2.column  =[x[0] for x in returns2.columns.ravel()]
        returns2.rename(columns = {'Portfolio':'Ticker'}, inplace = True)

        # create summary
        keep = ['Portfolio', 'Portfolio_Expected_Return', 'Portfolio_Expected_Var', 'Portfolio_Expected_SD', 'Portfolio_Expected_Sharpe']
        portfolios2 = portfolios[keep].groupby(keep, as_index = False).first()
        portfolios2.rename(columns = {'Portfolio_Expected_Return':'Expected_Return', 'Portfolio_Expected_Var':'Expected_Var', 'Portfolio_Expected_SD':'Expected_SD', 'Portfolio_Expected_Sharpe':'Expected_Sharpe'}, inplace = True)

        portfolios3 = returns2.sort_values(by = ['Ticker', 'Month'], ascending = True)
        portfolios3['Cumulative_Return'] = portfolios3.groupby('Ticker')['Return'].transform(lambda x: (1 + x/100).cumprod())
        portfolios3['Cumulative_Return_Max'] = portfolios3.groupby('Ticker')['Cumulative_Return'].cummax()
        portfolios3['Draw_Down'] = 100*(1 - portfolios3.Cumulative_Return / portfolios3.Cumulative_Return_Max)

        portfolios3 = portfolios3.groupby('Ticker', as_index = False)
        aggegations = {
            'Return': {
                'Actual_Return': lambda x: 100*(np.prod((1 + x/100))**(12/x.size) - 1),
                'Actual_SD': lambda x: math.sqrt(12) * np.std(x)
            },
            'Draw_Down': {
                'Max_Draw_Down': np.max
            }
        }
        portfolios3 = portfolios3.agg(aggegations)
        portfolios3.columns = ['Portfolio'] + [x[1] for x in portfolios3.columns.ravel()[1:]]

        summary = portfolios2.merge(portfolios3, how="inner", on=["Portfolio"])

        # export
        returns2.to_csv(self.output()['returns-output'].path, index = False)
        summary.to_csv(self.output()['summary-output'].path, index = False)
