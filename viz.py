import luigi, datetime, yaml, math
from pipeline import *

# pull in meta data
with open('meta.yaml', 'rb') as f:
    META = yaml.load(f)


class RiskVsReturnScatter(luigi.Task):

    dt = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return [GetETFDbCSV(dt=self.dt), CalcETFTilts(dt=self.dt), CalcETFPortfolioSummary(dt=self.dt)]

    def input(self):
        return {
            'etf-db' : GetETFDbCSV(dt=self.dt).output(),
            'etf-tilts' : CalcETFTilts(dt=self.dt).output(),
            'portfolio-summary' : CalcETFPortfolioSummary(dt=self.dt).output()['summary-output']
        }

    def output(self):
        return {
            'output1':luigi.LocalTarget('data/%s/viz/viz1_1.csv' % (self.dt)),
            'output2':luigi.LocalTarget('data/%s/viz/viz1_2.csv' % (self.dt)),
            'output3':luigi.LocalTarget('data/%s/viz/viz1_3.csv' % (self.dt))
        }

    def run(self):
        # bring in inputs
        etf_db = pd.read_csv(self.input()['etf-db'].path)
        etf_tilts = pd.read_csv(self.input()['etf-tilts'].path)
        portfolio_summary = pd.read_csv(self.input()['portfolio-summary'].path)

        # format data for plot
        plot1 = etf_tilts.merge(etf_db, how = "inner", left_on = ['Ticker'], right_on = ['TICKER'])
        plot1['Size'] = plot1.AUM.apply(lambda x: math.log(1 + x/1000000000))
        plot1['Label'] = plot1.apply(lambda x: "Ticker: %s<br>Fund: %s<br>AUM: %1.1fB<br>E[Return]: %1.1f%%<br>SD(Return): %1.1f%%" % (x['Ticker'], x['FUND'], x['AUM']/1000000000, x['Expected_Return'], x['Expected_SD']), axis=1)
        plot1.rename(columns = {'Expected_SD':'X', 'Expected_Return':'Y', 'Expected_Sharpe':'Color'}, inplace = True)
        plot1 = plot1[['X','Y','Size','Label','Color']]
        plot1.sort_values(by = ['Size'], ascending = False, inplace = True)
        plot1['Opacity'] = np.linspace(0.3, 1, plot1.shape[0])
        area_scaler = 2 / plot1.Size.quantile(0.25)
        plot1['Size'] = plot1.Size * area_scaler

        plot2 = portfolio_summary.copy()
        plot2['Label'] = plot2.apply(lambda x: "Fund: %s<br>E[Return]: %1.1f%%<br>SD(Return): %1.1f%%" % (x['Portfolio'], x['Expected_Return'], x['Expected_SD']), axis=1)
        plot2.rename(columns = {'Expected_SD':'X', 'Expected_Return':'Y', 'Expected_Sharpe':'Color'}, inplace = True)
        plot2 = plot2[['X','Y','Label','Color']]
        plot2.sort_values(by = ['X'], ascending = True, inplace = True)

        plot3_1 = etf_tilts.loc[etf_tilts.Ticker.isin(META['BENCHMARKS']), ['Ticker', 'Expected_Return', 'Expected_Var', 'Expected_SD', 'Expected_Sharpe', 'Actual_Return', 'Actual_SD', 'Actual_Var', 'Actual_Sharpe', 'Max_Draw_Down']].rename(columns={'Ticker':'Portfolio'})
        plot3_2 = portfolio_summary[['Portfolio', 'Expected_Return', 'Expected_Var', 'Expected_SD', 'Expected_Sharpe', 'Actual_Return', 'Actual_SD', 'Actual_Var', 'Actual_Sharpe', 'Max_Draw_Down']]
        plot3 = pd.concat([plot3_1, plot3_2], axis=0)

        # export
        plot1.to_csv(self.output()['output1'].path, index = False)
        plot2.to_csv(self.output()['output2'].path, index = False)
        plot3.to_csv(self.output()['output3'].path, index = False)


class CumulativeReturnPlot(luigi.Task):

    dt = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return [AllETFReturns(dt=self.dt), CalcETFPortfolioSummary(dt=self.dt)]

    def input(self):
        return {
            'etf-returns':AllETFReturns(dt=self.dt).output(),
            'portfolio-returns':CalcETFPortfolioSummary(dt=self.dt).output()['returns-output']
        }

    def output(self):
        return luigi.LocalTarget('data/%s/viz/viz2.csv' % (self.dt))

    def run(self):
        # bring in inputs
        etf_returns = pd.read_csv(self.input()['etf-returns'].path)
        portfolio_returns = pd.read_csv(self.input()['portfolio-returns'].path)

        # filter to benchmarks
        etf_returns = etf_returns.loc[etf_returns.Ticker.isin(META['BENCHMARKS']), ['Ticker', 'Month', 'Return']]

        # cumulative returns and de-normalize
        returns = pd.concat([etf_returns, portfolio_returns], axis=0)
        returns.sort_values(by = ['Ticker', 'Month'], ascending = True, inplace = True)
        returns['Cumulative_Return'] = returns.groupby('Ticker')['Return'].transform(lambda x: (1 + x/100).cumprod())
        returns = returns.pivot(index = "Month", columns = "Ticker", values = "Cumulative_Return")

        # format month
        returns['Month'] = returns.index
        returns['Month'] = returns.Month.apply(lambda x: '%s-%s-%s' % (str(x)[0:4], str(x)[4:6], '01'))

        # export
        returns.to_csv(self.output().path, index = False)


class TiltsRadarPlot(luigi.Task):

    dt = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'etf-tilts': CalcETFTilts(dt=self.dt),
            'portfolio-tilts': CalcETFPortfolioTilts(dt=self.dt)
        }

    def output(self):
        return luigi.LocalTarget('data/%s/viz/viz4.csv' % (self.dt))

    def run(self):
        # bring in inputs
        etf_tilts = pd.read_csv(self.input()['etf-tilts'].path)
        portfolio_tilts = pd.read_csv(self.input()['portfolio-tilts'].path)

        # filter to benchmarks and combine
        etf_tilts = etf_tilts.loc[etf_tilts.Ticker.isin(META['BENCHMARKS']),:]
        tilts = pd.concat([etf_tilts, portfolio_tilts], axis=0)

        # rename variables
        tilts = tilts[['Ticker'] + list(META['FACTORS_NAMES'].keys())]
        tilts.columns = ['Ticker'] + list(META['FACTORS_NAMES'].values())

        # export
        tilts.to_csv(self.output().path, index = False)


class WeightsDonutPlot(luigi.Task):

    dt = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'etf-db': GetETFDbCSV(dt=self.dt),
            'portfolios': CreateAllETFPortfolios(dt=self.dt)
        }

    def output(self):
        return luigi.LocalTarget('data/%s/viz/viz3.csv' % (self.dt))

    def run(self):
        # bring in inputs
        etf_db = pd.read_csv(self.input()['etf-db'].path)
        portfolios = pd.read_csv(self.input()['portfolios'].path)

        # de-normalize
        portfolios = portfolios.pivot(index = "Ticker", columns = "Portfolio", values = "Weight")
        portfolios.fillna(0, inplace = True)
        portfolios['Total'] = portfolios.apply(np.sum, axis=1)
        portfolios['Ticker'] = portfolios.index
        portfolios.sort_values(by = ['Total'], ascending = False, inplace = True)

        # pull in names
        etf_db = etf_db[['TICKER', 'FUND']]
        portfolios = portfolios.merge(etf_db, how = "inner", left_on = ["Ticker"], right_on = ["TICKER"])
        portfolios['Ticker'] = portfolios.apply(lambda x: '%s (%s)' % (x['FUND'], x['Ticker']), axis=1)
        portfolios.drop(['Total', 'TICKER', 'FUND'], inplace = True, axis=1)

        # export
        portfolios.to_csv(self.output().path, index = False)


# make all visualizations
class MakeViz(luigi.Task):

    dt = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return CalcETFPortfolioTilts(dt=self.dt)

    # make sure that we've run pipeline first
    def complete(self):
        if not self.requires().complete():
            return False
        return all(t.complete() for t in self.to_run)

    @property
    def to_run(self):
        return [RiskVsReturnScatter(dt=self.dt), CumulativeReturnPlot(dt=self.dt), WeightsDonutPlot(dt=self.dt), TiltsRadarPlot(dt=self.dt)]

    def run(self):
        yield self.to_run
