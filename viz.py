import luigi, datetime, yaml
from base import RTask
from pipeline import *

# pull in meta data
with open('meta.yaml', 'rb') as f:
    META = yaml.load(f)


class RiskVsReturnScatter(RTask):

    dt = luigi.DateParameter(default=datetime.date.today())
    r_script = 'r/viz/risk_vs_return.R'

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


class CumulativeReturnPlot(RTask):

    dt = luigi.DateParameter(default=datetime.date.today())
    r_script = 'r/viz/cumulative_returns.R'

    def requires(self):
        return [AllETFReturns(dt=self.dt), CalcETFPortfolioSummary(dt=self.dt)]

    def input(self):
        return {
            'etf-returns':AllETFReturns(dt=self.dt).output(),
            'portfolio-returns':CalcETFPortfolioSummary(dt=self.dt).output()['returns-output']
        }

    def output(self):
        return luigi.LocalTarget('data/%s/viz/viz2.csv' % (self.dt))


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


class WeightsDonutPlot(RTask):

    dt = luigi.DateParameter(default=datetime.date.today())
    r_script = 'r/viz/weights.R'

    def requires(self):
        return {
            'etf-db': GetETFDbCSV(dt=self.dt),
            'portfolios': CreateAllETFPortfolios(dt=self.dt)
        }

    def output(self):
        return luigi.LocalTarget('data/%s/viz/viz3.csv' % (self.dt))


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
