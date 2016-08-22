# coding=utf-8

"""
Send metrics to [Google Pub/Sub](https://cloud.google.com/pubsub/).

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

 In addition if you are not running this on a host in your GCE project you
 will need to have the GOOGLE_APPLICATION_CREDENTIALS environment variable
 pointing to a credentials file for the user
 you are running diamond as.

#### Configuration

Add `diamond.handler.pubsub.PubsubHandler` to your handlers.
It has these options:

 * `topic` - The Pub/Sub topic to publish to.
 * `scopes` - Comma separated list of Pub/Sub scopes to use.
 * `retries` - Number of retries for failed publish attempts.
 * `batch` - Whether to batch msgs or not.  Values:
                - None
                - count (batch by count of msgs...i.e 7 to batch in 7 msgs
                  increments.
                - size (batch by total size of batch in bytes...i.e 64000
                  to send in 64K increments.
 * `batch_size` - If msgs are to be batched this will contain either the
                  count number or size in bytes.
 * `tags` - Comma separated free-form field for additional key/value pairs
            to be sent.
"""

from Handler import Handler
import logging
import base64
import json
import Queue
import time

try:
    from apiclient import discovery
    from oauth2client.client import GoogleCredentials
except ImportError:
    discovery = None
    GoogleCredentials = None

HARD_LIMIT = 1000  # Google imposed hard message limit for Pub/Sub


class PubsubHandler(Handler):

    def __init__(self, config=None):
        # Initialize Handler
        Handler.__init__(self, config)

        if discovery is None:
            logging.error("Failed to load apiclient.discovery")
            return
        elif GoogleCredentials is None:
            logging.error("Failed to load "
                          "oauth2client.client.GoogleCredentials")
            return

        # Initialize options
        self.topic = self.config['topic']
        self.scopes = self.config['scopes']
        self.retries = int(self.config['retries'])
        self.batch = self.config['batch']
        self.batch_size = int(self.config['batch_size'])
        self.max_queue_size = int(self.config['max_queue_size'])
        self.max_queue_time = int(self.config['max_queue_time'])
        self.msgs_to_cull = int(self.config['msgs_to_cull'])
        tags_items = self.config['tags']
        self.tags = {}
        if tags_items is not None and len(tags_items) > 0:
            for item in tags_items:
                k, v = item.split(':')
                self.tags[k] = v

        # vars for batch processing by size
        self.avg_msg_size = 0

        # make sure the number of messages tried does not exceed
        # the message size limit.  1000 is the hard limit.
        if self.batch == 'count':
            if self.batch_size > HARD_LIMIT:
                self.batch_size = HARD_LIMIT
                logging.warning("Batch message count set too high, "
                                "changed to {}".format(HARD_LIMIT))

        # Initialize Queue
        self.q = Queue.Queue(self.max_queue_size)
        self.last_q_push = int(time.time())
        self.time_to_wait = 0

        # Back-off vars
        self.backoff_enable = False
        self.backoff_current_fib = 1
        self.backoff_max_fib = int(self.config['max_fibonacci'])
        self.safe_push = int(self.config['safe_push'])

        # Initialize client
        credentials = GoogleCredentials.get_application_default()
        if credentials.create_scoped_required():
            credentials = credentials.create_scoped(self.scopes)
        self.client = discovery.build('pubsub', 'v1', credentials=credentials)

    def get_default_config_help(self):
        """
        Returns the help text for the configuration options for this handler
        """
        config = super(PubsubHandler, self).get_default_config_help()

        config.update({
            'topic': 'Pub/Sub Topic',
            'scopes': 'Pub/Sub Scopes',
            'retries': 'Number of retries to publish a metric',
            'max_queue_size': 'Max size for internal queue of metric msgs',
            'batch': 'Should msgs be batched.  Values: None, count, or size',
            'batch_size': 'If batch msgs, will contain the count number or size'
                          ' in bytes',
            'max_queue_time': 'Max time a msg should stay in the queue in seconds',
            'max_fibonacci': 'Max value used to generate fibonacci number',
            'safe_push': 'Min secs between pushes to Pub/Sub',
            'msgs_to_cull': 'Number of msgs to remove from send window for overhead',
            'tags': 'Comma separated free-form field to hold additional'
                    ' key/value pairs to be sent.',
        })

        return config

    def get_default_config(self):
        """
        Return the default config for the handler
        """
        config = super(PubsubHandler, self).get_default_config()

        config.update({
            'topic': 'projects/my-project/topics/default-topic',
            'scopes': 'https://www.googleapis.com/auth/pubsub',
            'retries': 3,
            'max_queue_size': 100000,
            'batch': None,
            'batch_size': 0,
            'max_queue_time': 120,
            'max_fibonacci': 10,
            'safe_push': 6,
            'msgs_to_cull': 5,
            'tags': ''
        })

        return config

    def process(self, metric):
        """
        Process a metric by sending it to pub/sub
        :param metric: metric to process
        """
        converted_metric = self._convert_to_pubsub(metric)
        self.avg_msg_size = (self.avg_msg_size + len(json.dumps(converted_metric))) / 2
        logging.debug("Avg msg size: {}".format(self.avg_msg_size))
        if self.backoff_enable:
            if self.last_q_push + self.time_to_wait > int(time.time()):
                logging.debug("IN BACK OFF - Queueing metric. "
                              "fib seq #: {}, "
                              "ttw: {}".format(self.backoff_current_fib, self.time_to_wait))
                logging.debug("Current time: {}".format(int(time.time())))
                logging.debug("Next Push: {}".format(self.last_q_push + self.time_to_wait))
                self._add_to_queue(converted_metric)
            else:
                self._add_to_queue(converted_metric)
                self._process_queue()
        else:
            self._add_to_queue(converted_metric)
            self._process_queue()

    def _process_queue(self):
        """
        Does the actual processing logic and sends messages to queue or pub/sub
        :param metric: metric to process
        """
        # Make sure messages in queue are not staying past the max
        # time they should.
        current_time = int(time.time())
        if current_time >= self.last_q_push + self.max_queue_time:
            logging.info("Max queue time reached...pushing metrics.  "
                         "Current time {}, Last push time {}, "
                         "Max queue time {}".format(current_time,
                                                    self.last_q_push,
                                                    self.max_queue_time))
            logging.debug("Queue size: {}  |  Batch size: {}"
                          .format(self.q.qsize(), self.batch_size))

            # Push the max we can to clear queue
            if self.q.qsize() < HARD_LIMIT:
                logging.debug("Qsize - Pushing {} metrics.".format(self.q.qsize()))
                self._send(self.q.qsize())
            else:
                logging.debug("HardLimit - Pushing {} metrics.".format(HARD_LIMIT))
                self._send(HARD_LIMIT)
        else:
            if self.batch is None:
                # each metric sent as it comes in
                self._send(1)
            else:
                # Safety measure to not overwhelm Pub/Sub.
                if current_time - self.last_q_push >= self.safe_push:
                    if self.batch == 'count':
                        # batch up by number of msgs
                        if self.q.qsize() >= self.batch_size:
                            self._send(self.batch_size)
                    else:
                        num_to_send = int(self.batch_size / self.avg_msg_size) - self.msgs_to_cull
                        if self.q.qsize() >= num_to_send:
                            logging.debug("Number to send: {}".format(num_to_send))
                            self._send(num_to_send)
                else:
                    logging.debug("Pausing....{} sec(s)"
                                  .format(self.safe_push - (current_time - self.last_q_push)))

        logging.debug("Queue size at end of process: {}".format(self.q.qsize()))

    def _add_to_queue(self, msg):
        """
        Adds metric to queue.
        :param msg: metric to add to queue
        """
        try:
            self.q.put_nowait(msg)
        except Queue.Full:
            logging.error("Queue Full...please investigate!")
            # raise Exception("Queue Full...please investigate!")
        except Exception, e:
            raise Exception("Exception: {}".format(e))

    def _convert_to_pubsub(self, metric):
        """
        Convert a metric to a dictionary representing a Pub/Sub event.
        Each metric should be loaded into a separate data slot
        """
        self.tags['keyspace'] = metric.getCollectorPath() + '.' + metric.getMetricPath()
        payload = {
            'host': metric.host,
            'service': metric.path,
            'time': metric.timestamp,
            'metric': float(metric.value),
            'ttl': metric.ttl,
            'tags': self.tags,
            }
        data = base64.b64encode(json.dumps(payload))

        return {'data': data}

    def _fib(self, n):
        """
        Function takes in a number and returns the fibonacci sequence associated
        with it.
        :param n:  number you want fibonacci sequence of.
        :return:  fibonacci sequence
        """
        if n == 1 or n == 2:
            return 1
        return self._fib(n - 1) + self._fib(n - 2)

    def _send(self, msg_num):
        """
        Send data to pub/sub.
        """
        logging.debug("Queue size beginning send: {}".format(self.q.qsize()))
        metrics = []
        try:
            for i in range(msg_num):
                metrics.append(self.q.get_nowait())
            body = {'messages': metrics}

            resp = self.client.projects().topics().publish(
                topic=self.topic, body=body).execute(num_retries=self.retries)
            logging.info("Number of messages being sent: %s", len(metrics))
            logging.debug("Size of message batch being sent: %s",
                          len(json.dumps(body)))
            self.msg_count = 0
            self.msg_total_size = 0
            # clear list
            del metrics[:]
            # clear back off
            if self.backoff_enable:
                logging.info("Clearing back off")
                self.backoff_enable = False
                self.backoff_current_fib = 1
        except Queue.Empty:
            logging.warn("Queue Empty caught")
            pass
        except Exception, e:
            logging.error("Error sending event to Pub/Sub: {}\n at time: {}"
                          .format(e, int(time.time())))
            # put messages back on queue.
            logging.debug("Putting messages not sent back on queue.")
            for m in metrics:
                self._add_to_queue(m)
            # reset counters
            self.msg_count = 0
            self.msg_total_size = 0
            # clear list
            del metrics[:]
            # manage back off
            if self.backoff_enable:
                if self.backoff_current_fib < self.backoff_max_fib:
                    logging.debug("Incrementing back off")
                    self.backoff_current_fib += 1
            else:
                logging.info("Enabling back off")
                self.backoff_enable = True
            # raise Exception("Error sending event to Pub/Sub : %s", e)

        finally:
            logging.info("Resetting last push time to {}".format(int(time.time())))
            self.last_q_push = int(time.time())  # reset last time tried to send
            logging.debug("Queue size at end of send: {}".format(self.q.qsize()))
            self.time_to_wait = int(self._fib(self.backoff_current_fib)) * 60

    def _close(self):
        """
        Nothing to do since Pub/Sub publishes to a Rest API
        """
        self.client = None

    def __del__(self):
        self._close()

