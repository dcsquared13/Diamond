import unittest
import configobj
import base64

from diamond.handler.pubsub import PubsubHandler
from diamond.metric import Metric

class TestPubsubHandler(unittest.TestCase):

    def test_convert_to_pubsub_metric(self):
        config = configobj.ConfigObj()
        config['topic'] = 'projects/your-project/topics/your-topic'
        config['scopes'] = 'https://www.googleapis.com/auth/pubsub'
        config['retries'] = 3
        config['batch'] = None
        config['batch_size'] = 0
        config['tags'] = ['key1:value1', 'key2:value2']
        config['max_queue_size'] = 100000

        handler = PubsubHandler(config)
        metric = Metric('servers.com.example.www.cpu.total.idle',
                        0,
                        timestamp=1234567,
                        host='com.example.www',
                        ttl=120)

        event = handler._convert_to_pubsub(metric)
        data = eval(base64.b64decode(event.get('data')))

        expected = {
            "host": "com.example.www",
            "service": "servers.cpu.total.idle",
            "time": 1234567,
            "metric": 0.0,
            "ttl": 120,
            "tags": {"key1": "value1", "key2": "value2"}
        }

        self.assertEqual(data, expected)

    def test_batch_metrics_count(self):
        config = configobj.ConfigObj()
        config['topic'] = 'projects/your-project/topics/your-topic'
        config['scopes'] = 'https://www.googleapis.com/auth/pubsub'
        config['retries'] = 3
        config['batch'] = 'count'
        config['batch_size'] = 3
        config['tags'] = ['key1:value1', 'key2:value2']
        config['max_queue_size'] = 100000

        handler = PubsubHandler(config)
        metric = Metric('servers.com.example.www.cpu.total.idle',
                        0,
                        timestamp=1234567,
                        host='com.example.www',
                        ttl=120)

        handler.process(metric)
        handler.process(metric)
        handler.process(metric)
        self.assertEquals(handler.q.qsize(), 3)

    def test_batch_count_resize(self):
        config = configobj.ConfigObj()
        config['topic'] = 'projects/your-project/topics/your-topic'
        config['scopes'] = 'https://www.googleapis.com/auth/pubsub'
        config['retries'] = 3
        config['batch'] = 'count'
        config['batch_size'] = 1500
        config['tags'] = None
        config['max_queue_size'] = 100000

        handler = PubsubHandler(config)
        self.assertEquals(handler.batch_size, 1000)

    def test_full_queue_exception(self):
        config = configobj.ConfigObj()
        config['topic'] = 'projects/your-project/topics/your-topic'
        config['scopes'] = 'https://www.googleapis.com/auth/pubsub'
        config['retries'] = 3
        config['batch'] = 'count'
        config['batch_size'] = 1500
        config['tags'] = None
        config['max_queue_size'] = 100

        handler = PubsubHandler(config)
        metric = Metric('servers.com.example.www.cpu.total.idle',
                        0,
                        timestamp=1234567,
                        host='com.example.www',
                        ttl=120)

        with self.assertRaises(Exception):
            for i in range(101):
                handler.process(metric)

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestPubsubHandler)
    unittest.TextTestRunner(verbosity=2).run(suite)
