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
 * [ApiClient](https://github.com/google/google-api-python-client/).
 * [Oauth2Client](https://github.com/google/oauth2client/).

#### Configuration

Add `diamond.handler.stackdriver_gcp.StackdriverGCPHandler` to your handlers.
It has these options:



"""

from Handler import Handler
import logging
import datetime
import time
import Queue

try:
    from apiclient import discovery
    from oauth2client.client import GoogleCredentials
except ImportError:
    discovery = None
    GoogleCredentials = None


class StackdriverGCPHandler(Handler):

    def __init__(self, config=None):
        """
        Initializations for the Handler.
        """
        # Initialize Handler
        Handler.__init__(self, config)
        logging.debug("[StackdriverGCPHandler] Initializing Stackdriver GCP Handler")

        if discovery is None:
            logging.error("[StackdriverGCPHandler] Failed to load apiclient.discovery")
            return
        elif GoogleCredentials is None:
            logging.error("[StackdriverGCPHandler] Failed to load "
                          "oauth2client.client.GoogleCredentials")
            return

        # Initialize options
        self.project_resource = "projects/{0}".format(self.config["project_id"])
        self.instance_type = self.config['instance_type']
        self.batch_size = int(self.config['batch_size'])
        self.blacklist = []
        tags_items = self.config['tags']
        self.tags = {}
        if tags_items is not None and len(tags_items) > 0:
            for item in tags_items:
                k, v = item.split(':')
                self.tags[k] = v

        if self.config['blacklist']:
            if type(self.config['blacklist']) is not list:
                logging.debug("[StackdriverGCPHandler] Converting blacklist {} to list"
                              .format(self.config['blacklist']))
                self.blacklist.append(self.config['blacklist'])
            else:
                logging.debug("[StackdriverGCPHandler] Blacklist passed in as list.")
                self.blacklist = self.config['blacklist']

        # Initialize Queue
        self.q = Queue.PriorityQueue(int(self.config['max_queue_size']))
        self.last_q_push = int(time.time())
        self.queue_buffer_time = self.config['queue_buffer_time']

        # Statistics
        self.statistics = self.config['statistics']
        self.stats_start_time = time.time()
        self.stats_count = 0

    def get_default_config_help(self):
        """
        Returns the help text for the configuration options for this handler
        """
        config = super(StackdriverGCPHandler, self).get_default_config_help()

        config.update({
            'project_id': 'Project name to send metrics to',
            'instance_type': 'Instance type',
            'batch_size': 'Number of metrics in batch',
            'max_queue_size': 'Max number of metrics to hold in queue',
            'queue_buffer_time': 'Amount of buffer time in queue',
            'tags': 'Tags',
            'blacklist': 'Blacklist of collectors to not process',
            'statistics': 'Get stats',
        })

        return config

    def get_default_config(self):
        """
        Return the default config for the handler
        """
        config = super(StackdriverGCPHandler, self).get_default_config()

        config.update({
            'project_id': None,
            'instance_type': 'gce_instance',
            'batch_size': 1,
            'max_queue_size': 10000,
            'queue_buffer_time': 120,
            'tags': None,
            'blacklist': None,
            'statistics': False,
        })

        return config

    def _format_rfc3339(self, datetime_instance=None):
        """Formats a datetime per RFC 3339.
        :param datetime_instance: Datetime instance to format
        """
        return datetime_instance.isoformat("T") + "Z"

    def _remove_tags(self, t):
        """
        Formats a tags dictionary to remove extraneous tags
        before sending to Stackdriver
        :param t: Tags dictionary to format
        """
        tags = dict(t)
        if "keyspace" in tags:
            del tags["keyspace"]
        if "zone" in tags:
            del tags["zone"]

        return tags

    def _create_timeseries(self, metric):
        """
        Creates a Stackdriver timeseries out of a metric object.
        :param metric: Metric object
        """
        path = '%s/%s' % (metric.getCollectorPath().replace(".", "/"),
                          metric.getMetricPath().replace(".", "/"))
        metric_time = self._format_rfc3339(
            datetime.datetime.fromtimestamp(metric.timestamp))
        custom_metric_type = "custom.googleapis.com/{}".format(path)
        if metric.tags is not None:
            self.tags.update(metric.tags)
        zone = self.tags.get("zone", "us-central1-a")
        tags = self._remove_tags(self.tags)

        timeseries_data = {
            "metric": {
                "type": custom_metric_type,
                "labels": tags
            },
            "resource": {
                "type": self.instance_type,
                "labels": {
                    'instance_id': metric.host,
                    'zone': zone
                }
            },
            "points": [
                {
                    "interval": {
                        "startTime": metric_time,
                        "endTime": metric_time
                    },
                    "value": {
                        "doubleValue": metric.value
                    }
                }
            ]
        }

        return timeseries_data

    def _get_client(self):
        """Builds an http client authenticated with the service account
        credentials."""
        credentials = GoogleCredentials.get_application_default()
        client = discovery.build('monitoring', 'v3', credentials=credentials)
        return client

    def _send(self, num):
        """
        Sending the timeseries to Stackdriver
        :param ts: Timeseries
        """
        logging.debug("[StackdriverGCPHandler] Queue size beginning send: {}"
                      .format(self.q.qsize()))
        metrics = []
        ts_metrics = []
        for i in range(num):
            metrics.append(self.q.get_nowait())
        try:
            for i in range(len(metrics)):
                ts_metrics.append(metrics[i][1])
            body = {'timeSeries': ts_metrics}
            client = self._get_client()
            request = client.projects().timeSeries().create(
                name=self.project_resource, body=body)
            request.execute()
            logging.info("[StackdriverGCPHandler] {} custom metrics sent successfully"
                         .format(num))
            logging.debug("[StackdriverGCPHandler] timeseries sent: {}"
                          .format(body))
        except Exception, e:
            # put back on queue.
            for i in range(len(metrics)):
                self.q.put_nowait(metrics[i])
            logging.error("[StackdriverGCPHandler] Error sending metrics: {}"
                          .format(e))
        finally:
            # clear list
            del metrics[:]
            del ts_metrics[:]
            logging.info("[StackdriverGCPHandler] Resetting last push time to {}"
                         .format(int(time.time())))
            self.last_q_push = int(time.time())  # reset last time tried to send

    def _check_metric(self, metric):
        """
        Check metric to ensure the collector providing it is not blacklisted from
        sending.
        :param metric: Metric to check
        :return:  True or False depending on if blacklisted.
        """
        ret_code = True
        if self.blacklist:
            logging.debug("[StackdriverGCPHandler] Blacklist {} found processing metrics"
                          .format(self.blacklist))
            if metric.collector not in self.blacklist:
                logging.debug("[StackdriverGCPHandler] Metric not in blacklist.")
            else:
                logging.debug("[StackdriverGCPHandler] Metric in blacklist, don't send.")
                ret_code = False

        return ret_code

    def _flush_expired_metrics(self):
        """
        Flush expired metrics from the queue.
        """
        cutoff_time = time.time() - self.queue_buffer_time
        flush = True
        while flush:
            metric = self.q.get_nowait()
            metric_ts = metric[0]
            if metric_ts > cutoff_time:
                # first metric is good, put back on queue and end flush
                # otherwise drop metric and pull next.
                self.q.put_nowait(metric)
                flush = False
                logging.debug("[StackdriverGCPHandler] Nothing to flush.")
            else:
                # Flush this #2 away
                logging.debug("[StackdriverGCPHandler] flushing {}."
                              .format(metric))

    def _add_to_queue(self, ts, msg):
        """
        Adds metric to queue.
        :param msg: metric to add to queue
        """
        try:
            logging.debug("[StackdriverGCPHandler] Adding metric to queue.")
            tup = (ts, msg)
            self.q.put_nowait(tup)
            if self.statistics:
                if time.time() >= self.stats_start_time + 60:
                    logging.info("[StackdriverGCPHandler][Statistics] "
                                  "{} metrics in last {} secs."
                                  .format(self.stats_count,
                                          (time.time() - self.stats_start_time)))
                    self.stats_start_time = time.time()
                    self.stats_count = 0
                self.stats_count += 1
        except Queue.Full:
            logging.error("[StackdriverGCPHandler] Queue Full...please investigate!")
        except Exception, e:
            logging.error("[StackdriverGCPHandler] Exception: {}".format(e))

    def _process_queue(self):
        """
        Process the queue instigating a send of a batch of metrics to Stackdriver
        when necessary conditions met.
        """
        self._flush_expired_metrics()
        if self.q.qsize() >= self.batch_size:
            logging.debug("[StackdriverGCPHandler] Sending {} metrics to Stackdriver."
                          .format(self.batch_size))
            self._send(self.batch_size)
        else:
            logging.debug("[StackdriverGCPHandler] qsize {}, batch_size {}"
                          .format(self.q.qsize(), self.batch_size))

    def process(self, metric):
        """
        Process a metric and send to Stackdriver
        :param metric: metric to process
        """
        logging.debug("[StackdriverGCPHandler] Metric received: {}".format(metric))
        if self._check_metric(metric):
            timeseries = self._create_timeseries(metric)
            self._add_to_queue(metric.timestamp, timeseries)
            self._process_queue()

    def _close(self):
        """
        Nothing to do since Stackdriver publishes to a Rest API
        """
        self.client = None

    def __del__(self):
        self._close()
