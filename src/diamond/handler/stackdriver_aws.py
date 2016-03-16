# coding=utf-8

"""
Send metrics to [Stackdriver](http://www.stackdriver.com).

#### Copyright

Copyright 2016 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Author: dcochran@google.com

#### Dependencies
 * [requests]

#### Configuration

Add `diamond.handler.stackdriver_aws.StackdriverAWSHandler` to your handlers.
It has these options:

 * `gateway` - The stackdriver gateway url.
 * `apikey` - Stackdriver api key.
 * `instance_id` - AWS instance id

"""

import time
from Handler import Handler
import logging
import json
import requests


class StackdriverAWSHandler(Handler):

    def __init__(self, config=None):
        # Initialize Handler
        Handler.__init__(self, config)
        logging.debug("Initializing Stackdriver AWS Handler")

        # Initialize options
        self.gateway = self.config['gateway']
        self.apikey = self.config['apikey']
        self.instance_id = self.config['instance_id']

    def get_default_config_help(self):
        """
        Returns the help text for the configuration options for this handler
        """
        config = super(StackdriverAWSHandler, self).get_default_config_help()

        config.update({
            'gateway': 'Stackdriver gateway',
            'apikey': 'Stackdriver api key',
            'instance_id': 'AWS Instance ID'
        })

        return config

    def get_default_config(self):
        """
        Return the default config for the handler
        """
        config = super(StackdriverAWSHandler, self).get_default_config()

        config.update({
            'gateway': 'https://custom-gateway.stackdriver.com/v1/custom',
            'apikey': None,
            'instance_id': None
        })

        return config

    def process(self, metric):
        """
        Process an AWS metric by sending it to stackdriver
        :param metric: metric to process
        """
        # data_point = { 'name': name, 'value': value, 'collected_at': int(time.time()), 'instance': 'i-xxxxxxxx' }
        logging.debug("Metric received: {}".format(metric))
        data_point = self._metric_to_stackdriver_event(metric)
        logging.debug("Data point to be sent: {}".format(data_point))

        self._send(data_point)

    def _metric_to_stackdriver_event(self, metric):
        path = '%s.%s' % (metric.getCollectorPath(),
                metric.getMetricPath())
        return {
            'name': path,
            'value': metric.value,
            'collected_at': metric.timestamp,
            'instance': self.instance_id,
        }

    def _send(self, data_point):
        headers = {
            'content-type': 'application/json',
            'x-stackdriver-apikey': self.apikey
        }
        gateway_msg = {
            'timestamp': int(time.time()),
            'proto_version': 1,
            'data': data_point,
        }
        resp = requests.post(
            self.gateway,
            data=json.dumps(gateway_msg),
            headers=headers)

        if not resp.ok:
            logging.error("Failed to submit custom metric.  Response: {}".format(resp.content))
        else:
            logging.debug("Custom metric submitted.  Response: {}".format(resp.content))

