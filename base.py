import luigi, abc, subprocess, logging

# task for running an R script
class RTask(luigi.Task):

    _logger = logging.getLogger('luigi-interface')
    _command = None

    # should be overridden to specify the R script we want to run
    @abc.abstractproperty
    def r_script(self):
        pass

    # other params that are not inputs / outputs
    @property
    def extra_params(self):
        return {}

    # assumes requirements specified as dictionary and inputs/outputs are LocalTargets
    @property
    def _params(self):
        params = []
        outputs = self.output() if isinstance(self.output(), dict) else {'output':self.output()}
        for k,v in {**self.input(), **outputs, **self.extra_params}.items():
            v = v.path if isinstance(v, luigi.LocalTarget) else v
            params.append('--{k}={v}'.format(k=k,v=v))
        return ' '.join(params)

    @property
    def command(self):
        if self._command is None:
            self._command = "Rscript {r_script} {params}".format(r_script=self.r_script, params=self._params)
        return self._command

    def run_script(self):
        p = subprocess.Popen([self.command], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = [x.decode('utf-8') for x in p.communicate()]
        return (p.returncode, stdout, stderr)

    def run(self):
        return_code, stdout, stderr = self.run_script()
        self._logger.info(stdout)
        if return_code != 0:
            self._logger.error(stderr)
            raise Exception("There was an error while executing %s" % (self.command))
