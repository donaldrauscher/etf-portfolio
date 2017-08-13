import luigi, abc, math
import pandas as pd
import numpy as np
from cvxopt import matrix, solvers

# perform portfolio optimization
class Optimize(luigi.Task):

    @abc.abstractproperty
    def requires(self):
        pass

    @abc.abstractproperty
    def output(self):
        pass

    @abc.abstractproperty
    def target_return(self):
        pass

    @abc.abstractproperty
    def meta(self):
        pass

    def optimize(self, returns, covar, min_weight, max_weight):
        # min (1/2) x^TPx + q^T x
        # Gx <= h
        # Ax = b
        n = returns.size
        P = matrix(2*covar)
        q = matrix(np.zeros(n))
        G = matrix(np.vstack((np.diag(-np.ones(n)), np.diag(np.ones(n)), [-returns])))
        h = matrix(np.concatenate((np.repeat(-min_weight, n), np.repeat(max_weight, n), [-self.target_return])))
        A = matrix(np.ones(returns.size).reshape(1,-1))
        b = matrix(1.0)
        sol = solvers.qp(P, q, G, h, A, b)
        return((sol['primal objective'], np.array(list(sol['x']))))

    def run(self):
        # bring in inputs
        tilts = pd.read_csv(self.input()['tilts'].path)
        covar = pd.read_csv(self.input()['covar'].path)
        m = self.meta['OPTIMIZATION']
        min_weight, max_weight, outlier_cutoff = m['MIN_WEIGHT'], m['MAX_WEIGHT'], m['COEFF_OUTLIER_CUTOFF']

        # exclude outliers
        factor_names = list(self.meta['FACTORS_NAMES'].keys())
        is_outlier = tilts[factor_names].apply(lambda x: np.sum(x >= outlier_cutoff), axis=1)
        is_outlier = (is_outlier > 0)
        tilts2 = tilts.loc[~is_outlier, :]
        covar2 = covar.loc[~is_outlier, covar.columns[~is_outlier]]

        # first optimization
        portfolio1_obj, portfolio1_weights = self.optimize(tilts2.Expected_Return.as_matrix(), covar2.as_matrix(), 0, max_weight)

        # second optimization
        phase2_include = (portfolio1_weights >= min_weight)
        tilts3 = tilts2.loc[phase2_include,:]
        covar3 = covar2.loc[phase2_include, covar2.columns[phase2_include]]
        portfolio2_obj, portfolio2_weights = self.optimize(tilts3.Expected_Return.as_matrix(), covar3.as_matrix(), min_weight, max_weight)

        # pull in expected return and sd/var
        tilts4 = tilts3.copy()
        tilts4['Weight'] = portfolio2_weights
        tilts4['Portfolio'] = 'TR%02d' % (self.target_return)
        tilts4['Portfolio_Expected_Return'] = np.sum(tilts4.Weight * tilts4.Expected_Return)
        tilts4['Portfolio_Expected_Var'] = portfolio2_obj
        tilts4['Portfolio_Expected_SD'] = math.sqrt(portfolio2_obj)
        tilts4['Portfolio_Expected_Sharpe'] = tilts4.Portfolio_Expected_Return / tilts4.Portfolio_Expected_SD

        # export
        tilts4.to_csv(self.output().path, index = False)
