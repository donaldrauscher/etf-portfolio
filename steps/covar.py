import luigi, abc
import pandas as pd
import numpy as np

# calculate covariance between assets
class CalcCovar(luigi.Task):

    @abc.abstractproperty
    def requires(self):
        pass

    @abc.abstractproperty
    def output(self):
        pass

    @abc.abstractproperty
    def meta(self):
        pass

    def run(self):
        # bring in inputs
        returns = pd.read_csv(self.input()['returns'].path)
        tilts = pd.read_csv(self.input()['tilts'].path)
        factors = pd.read_csv(self.input()['factors'].path)
        xvar, yvar = self.meta['XVAR'], self.meta['YVAR']

        # calculate factor covariance
        f2f_covar = factors[xvar].cov()
        factor_weights = tilts[xvar]
        factor_covar = np.dot(np.dot(factor_weights, f2f_covar), np.transpose(factor_weights))

        # calculate residual covariance
        if self.meta['UNCORRELATED_RESIDUALS'] == 1:
            residual_covar = np.diag(tilts.Residual_Variance)
        else:
            factors['Alpha'] = 1
            xvar2 = xvar + ['Alpha']
            factor_weights2 = tilts[xvar2]

            pred = np.dot(factors[xvar2], np.transpose(factor_weights2))
            pred = pd.DataFrame(pred, columns = tilts.Ticker, index = factors.Month)
            actuals = returns.pivot(index = "Month", columns = "Ticker", values = "Return")
            pd.Series(pred.columns == actuals.columns).value_counts() # qc; should all be True

            actuals = actuals.loc[actuals.index.isin(pred.index),:]
            pred = pred.loc[pred.index.isin(actuals.index),:]
            pd.Series(pred.index == actuals.index).value_counts() # qc; should all be True

            residuals = pred - actuals
            residual_covar = residuals.cov()

        # sum together and export
        all_covar = 12 * (factor_covar + residual_covar)
        all_covar = pd.DataFrame(all_covar, columns = tilts.Ticker, index = tilts.Ticker)
        all_covar.to_csv(self.output().path, index = False)
