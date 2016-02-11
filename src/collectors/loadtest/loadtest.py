# coding=utf-8

"""
Loadtest collector used to loadtest a handler.

#### Dependencies

 * None

"""
import random
import diamond.collector
import logging


class LoadtestCollector(diamond.collector.Collector):

    def process_config(self):
        super(LoadtestCollector, self).process_config()

    def get_default_config_help(self):
        config_help = super(LoadtestCollector,
                            self).get_default_config_help()
        config_help.update({
            'mpm': "metrics per minute to simulate",
        })
        return config_help

    def get_default_config(self):
        """
        Returns default configuration options.
        """
        config = super(LoadtestCollector, self).get_default_config()
        config.update({
            'mpm': 1000,
        })
        return config

    def __init__(self, *args, **kwargs):
        super(LoadtestCollector, self).__init__(*args, **kwargs)
        self.mpm = int(self.config['mpm'])
        self.metrics = {}

    def collect(self):
        """
        Publish fake loadtest metrics
        """

        for i in range(self.mpm):
            device = "Loadtest" + str(i)
            mval = random.uniform(1, 10000)
            try:
                self.metrics[device + ".Metric"] = float(mval)
            except Exception, e:
                logging.error("Exception Caught: {}".format(e))

        for metric in self.metrics.keys():
            self.publish(metric, self.metrics[metric])
        del self.metrics[:]
