import luigi, datetime, yaml, subprocess, requests, os, logging, io, functools, re, json, unqlite
from luigi.contrib import sqla
from sqlalchemy import String, Float
import pandas as pd
from base import RTask

# pull in meta data
with open('meta.yaml', 'rb') as f:
    META = yaml.load(f)

# task for fetching factor and unzipping
class GetFactorData(luigi.Task):

    _logger = logging.getLogger('luigi-interface')
    dt = luigi.DateParameter(default=datetime.date.today())
    factor = luigi.Parameter()
    data = None
    df = None

    def output(self):
        return luigi.LocalTarget('data/%s/%s.csv' % (self.dt, self.factor))

    def unzip(self, from_f):
        command = 'unzip -p %s' % (from_f)
        p = subprocess.Popen([command], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = p.communicate()

        if p.returncode != 0:
            self._logger.error(stderr.decode('utf-8'))
            raise Exception("There was an error while unzipping")

        return stdout.decode('utf-8')

    def fetch(self):
        url = META['FACTORS'][self.factor]['LINK']
        outfile_zipped = 'data/%s/%s' % (self.dt, url.split('/')[-1])
        response = requests.get(url)

        if not os.path.exists(os.path.dirname(outfile_zipped)):
            os.makedirs(os.path.dirname(outfile_zipped))

        with open(outfile_zipped, 'wb') as f:
            f.write(response.content)

        self.data = self.unzip(outfile_zipped)

    def extract(self):
        lines = [x.rstrip() for x in self.data.split('\n')]
        start = next(i for i in range(len(lines)) if META['FACTORS'][self.factor]['START'] in lines[i])
        end = next(i for i in range(len(lines)) if META['FACTORS'][self.factor]['END'] in lines[i])
        lines = lines[start:end]
        lines = [l.replace(' ','') for l in lines if l.replace(' ','') != '']
        self.df = pd.read_csv(io.StringIO('Month' + '\n'.join(lines))) # Month missing header

    def run(self):
        self.fetch() # stores in data
        self.extract() # stores in df
        self.df.to_csv(self.output().path, index=False)


# creates factors from data and merges all together
class GetAllFactorData(luigi.Task):

    _logger = logging.getLogger('luigi-interface')
    dt = luigi.DateParameter(default=datetime.date.today())
    factors = META['FACTORS'].keys()

    def requires(self):
        inputs = {}
        for f in self.factors:
            inputs[f] = GetFactorData(dt=self.dt, factor=f)
        return inputs

    def output(self):
        return luigi.LocalTarget('data/%s/ALL_FACTORS.csv' % (self.dt))

    def get_input(self, f):
        df = pd.read_csv(self.input()[f].path)
        if META['FACTORS'][f]['POST_PROCESS']:
            df[f] = df[META['FACTORS'][f]['HIGH_VAL']] - df[META['FACTORS'][f]['LOW_VAL']]
            df[f] = df[f].round(2)
            df = df[["Month", f]]
        return df

    def run(self):
        inputs = [self.get_input(f) for f in self.factors]
        all_factors = functools.reduce(lambda l,r: pd.merge(l,r,how="inner"), inputs)
        all_factors.to_csv(self.output().path, index=False)


# grab database of ETFs
class GetETFDb(luigi.Task):

    _logger = logging.getLogger('luigi-interface')
    dt = luigi.DateParameter(default=datetime.date.today())
    data = None

    def output(self):
        return luigi.LocalTarget('data/%s/etf.db' % (self.dt))

    def fetch(self):
        response = requests.get(META['ETF_DB']['URL'], headers = META['ETF_DB']['HEADERS'])
        self.data = json.loads(response.text)

    def run(self):
        self.fetch() # stores in data
        db = unqlite.UnQLite(self.output().path)
        etf = db.collection('etf')
        etf.create()
        etf.store(self.data)


# apply filters and parse out some key fields
class GetETFDbCSV(luigi.Task):

    _logger = logging.getLogger('luigi-interface')
    dt = luigi.DateParameter(default=datetime.date.today())
    data = None
    data_df = None

    def requires(self):
        return GetETFDb(dt=self.dt)

    def output(self):
        return luigi.LocalTarget('data/%s/etf_db.csv' % (self.dt))

    def filter(self):
        etf_db = unqlite.UnQLite(self.input().path)
        etf_list = etf_db.collection('etf')
        filter_lambda = eval(META['ETF_DB']['FILTER_LAMBDA'].format(**META['ETF_DB']['FILTERS']))
        self.data = etf_list.filter(filter_lambda)

    def extract(self):
        data_norm = {}
        for k,v in META['ETF_DB']['FIELDS'].items():
            self._logger.info(k)
            self._logger.info(v)
            data_norm[k] = list(map(eval(v), self.data))
        self.data_df = pd.DataFrame.from_dict(data_norm, orient='columns')

    def run(self):
        self.filter() # stores in data
        self.extract() # stores in data_df
        self.data_df.to_csv(self.output().path, index=False)


# grab database of ETFs
class GetETFPrices(sqla.CopyToTable):

    _logger = logging.getLogger('luigi-interface')
    dt = luigi.DateParameter(default=datetime.date.today())
    ticker = luigi.Parameter()

    columns = [
        (["Date", String(10)], {}),
        (["Open", Float(10)], {}),
        (["High", Float(10)], {}),
        (["Low", Float(10)], {}),
        (["Close", Float(10)], {}),
        (["Volume", Float(0)], {}),
        (["Adj Close", Float(10)], {}),
        (["Ticker", String(10)], {})
    ]
    table = "prices"

    @property
    def connection_string(self):
        return 'sqlite:///data/%s/etf_prices.db' % (self.dt)

    def fetch(self):
        params = {
            'STOCK':self.ticker,
            'START_DAY':1, 'START_MONTH':self.dt.month-1, 'START_YEAR':self.dt.year-5,
            'END_DAY':self.dt.day, 'END_MONTH':self.dt.month-1, 'END_YEAR':self.dt.year
        }
        url = META['PRICE_LOOKUP'].format(**params)

        response = requests.get(url)
        if response.status_code == 404:
            return []
        if response.status_code != 200:
            response.raise_for_status()

        df = pd.read_csv(io.StringIO(response.text))
        df['Ticker'] = self.ticker
        return df.to_records(index=False)

    def rows(self):
        for row in self.fetch():
            yield row


# grab all ETF prices
class GetAllETFPrices(luigi.Task):

    _logger = logging.getLogger('luigi-interface')
    dt = luigi.DateParameter(default=datetime.date.today())
    _tickers = None

    @property
    def tickers(self):
        if not self._tickers:
            etf_list = pd.read_csv(self.input().path)
            self._tickers = list(etf_list.TICKER)
        return self._tickers

    def requires(self):
        return GetETFDbCSV(dt=self.dt)

    def output(self):
        return luigi.LocalTarget('data/%s/etf_prices.db' % (self.dt))

    # need to overwrite 'complete' method to check dependency first!
    def complete(self):
        if not self.requires().complete():
            return False
        tasks = [GetETFPrices(dt=self.dt, ticker=ticker) for ticker in self.tickers]
        return all(t.complete() for t in tasks)

    def run(self):
        yield [GetETFPrices(dt=self.dt, ticker=ticker) for ticker in self.tickers]


# pull ETF prices out into CSV
class GetAllETFPricesCSV(luigi.Task):

    _logger = logging.getLogger('luigi-interface')
    dt = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return GetAllETFPrices(dt=self.dt)

    def output(self):
        return luigi.LocalTarget('data/%s/etf_prices.csv' % (self.dt))

    @property
    def connection_string(self):
        return 'sqlite:///data/%s/etf_prices.db' % (self.dt)

    @property
    def query(self):
        return """
            SELECT prices.* FROM prices
            INNER JOIN (
                SELECT ticker, COUNT(*) AS `n`
                FROM prices GROUP BY ticker HAVING n >= {N}
            ) AS x ON prices.ticker = x.ticker
        """.format(N = META['REGRESSION']['MIN_N']+1)

    def run(self):
        df = pd.read_sql(self.query, self.connection_string)
        df.to_csv(self.output().path, index=False)


# roll up and calculate monthly returns
class AllETFReturns(RTask):

    dt = luigi.DateParameter(default=datetime.date.today())
    r_script = 'r/returns.R'

    def requires(self):
        return {'prices':GetAllETFPricesCSV(dt=self.dt)}

    def output(self):
        return luigi.LocalTarget('data/%s/etf_returns.csv' % (self.dt))


# calculate the tilts for each ETF
class CalcETFTilts(RTask):

    dt = luigi.DateParameter(default=datetime.date.today())
    r_script = 'r/tilts.R'

    @property
    def extra_params(self):
        return {'use-aic':META['REGRESSION']['USE_AIC']}

    def requires(self):
        return {'returns':AllETFReturns(dt=self.dt), 'factors':GetAllFactorData(dt=self.dt)}

    def output(self):
        return luigi.LocalTarget('data/%s/etf_tilts.csv' % (self.dt))


# create the covariance matrix
class CreateETFCovarMatrix(RTask):

    dt = luigi.DateParameter(default=datetime.date.today())
    r_script = 'r/covar.R'

    def requires(self):
        return {
            'returns':AllETFReturns(dt=self.dt),
            'tilts':CalcETFTilts(dt=self.dt),
            'factors':GetAllFactorData(dt=self.dt)
        }

    def output(self):
        return luigi.LocalTarget('data/%s/etf_covar.csv' % (self.dt))


# cluster ETFs into buckets
class ClusterETF(RTask):

    dt = luigi.DateParameter(default=datetime.date.today())
    r_script = 'r/cluster.R'

    def requires(self):
        return {'tilts':CalcETFTilts(dt=self.dt)}

    def output(self):
        return luigi.LocalTarget('data/%s/etf_cluster.csv' % (self.dt))


# create an optimal portfolio with a given return
class CreateETFPortfolio(RTask):

    dt = luigi.DateParameter(default=datetime.date.today())
    target_return = luigi.IntParameter()
    r_script = 'r/optimize.R'

    @property
    def extra_params(self):
        return {'target-return':self.target_return}

    def requires(self):
        return {'tilts':ClusterETF(dt=self.dt), 'covar':CreateETFCovarMatrix(dt=self.dt)}

    def output(self):
        return luigi.LocalTarget('data/%s/etf_portfolio_%s.csv' % (self.dt, self.target_return))


# create portfolios with different target returns
class CreateAllETFPortfolios(luigi.Task):

    dt = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return [CreateETFPortfolio(dt=self.dt, target_return=target_return) for target_return in META['OPTIMIZATION']['TARGET_RETURNS']]

    def output(self):
        return luigi.LocalTarget('data/%s/etf_portfolios.csv' % (self.dt))

    def run(self):
        portfolios = [pd.read_csv(f.path) for f in self.input()]
        portfolios = pd.concat(portfolios, axis=0)
        portfolios.to_csv(self.output().path, index=False)


# summary of actual and expected returns
class CalcETFPortfolioSummary(RTask):

    dt = luigi.DateParameter(default=datetime.date.today())
    r_script = 'r/summary.R'

    def requires(self):
        return {'etf-returns':AllETFReturns(dt=self.dt), 'portfolios':CreateAllETFPortfolios(dt=self.dt)}

    def output(self):
        return {
            'returns-output':luigi.LocalTarget('data/%s/etf_portfolio_returns.csv' % (self.dt)),
            'summary-output':luigi.LocalTarget('data/%s/etf_portfolio_summary.csv' % (self.dt))
        }


# calculate overall tilts
class CalcETFPortfolioTilts(RTask):

    dt = luigi.DateParameter(default=datetime.date.today())
    r_script = 'r/tilts.R'

    @property
    def extra_params(self):
        return {'use-aic' : META['REGRESSION']['USE_AIC']}

    def requires(self):
        return [CalcETFPortfolioSummary(dt=self.dt), GetAllFactorData(dt=self.dt)]

    # need to overwrite to specify which output from CalcETFPortfolioSummary to use
    def input(self):
        return {'returns':CalcETFPortfolioSummary(dt=self.dt).output()['returns-output'], 'factors':GetAllFactorData(dt=self.dt).output()}

    def output(self):
        return luigi.LocalTarget('data/%s/etf_portfolio_tilts.csv' % (self.dt))
