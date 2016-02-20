"""
Custom Diamond collector for reading Ruby metrics from /stats.json to be collected into graphite
"""
import httplib
import urlparse
import json
import diamond.collector


class NLRubyHomeCollector(diamond.collector.Collector):

    def __init__(self, *args, **kwargs):
        super(NLRubyHomeCollector, self).__init__(*args, **kwargs)

    def process_config(self):
        super(NLRubyHomeCollector, self).process_config()

    def get_default_config_help(self):
        config_help = super(NLRubyHomeCollector,
                            self).get_default_config_help()
        config_help.update({
            'path': "Path",
            'url': "URL"
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(NLRubyHomeCollector, self).get_default_config()
        config.update({
          'path':    'ruby',
          'url':     'http://localhost:9090/stats.json'
        })
        return config

    def collect(self):
        # Parse Url
        parts = urlparse.urlparse(self.config['url'])
        # Parse host and port
        endpoint = parts[1].split(':')
        if len(endpoint) > 1:
            service_host = endpoint[0]
            service_port = int(endpoint[1])
        else:
            service_host = endpoint[0]
            service_port = 80
        # Parse path
        service_path = parts[2]

        connection = httplib.HTTPConnection(service_host, service_port)
        try:
            connection.request("GET", "%s" % parts[2])
        except Exception, e:
            self.log.error("Error retrieving Ruby stats: %s" % e)
            print "FAILED TO CONNECT"
            return

        response = connection.getresponse()
        data = json.loads(response.read())['metrics']
        for k,v in data.iteritems():
            metric_name  = k
            metric_value = v['data']
            try:
                self.publish(metric_name, metric_value)
            except ValueError:
                pass