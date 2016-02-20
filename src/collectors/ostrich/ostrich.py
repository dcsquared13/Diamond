import httplib
import urlparse
import json

import diamond.collector


def flatten(init, lkey):
   ret = {}
   for rkey,val in init.items():
     key = lkey+rkey
     if type(val) is dict:
       ret.update(flatten(val, key+'.'))
     else:
       ret[key] = val
   return ret


class OstrichCollector(diamond.collector.Collector):

    def __init__(self, *args, **kwargs):
        super(OstrichCollector, self).__init__(*args, **kwargs)

    def process_config(self):
        super(OstrichCollector, self).process_config()

    def get_default_config_help(self):
        config_help = super(OstrichCollector,
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
        config = super(OstrichCollector, self).get_default_config()
        config.update({
            'path':     'ostrich',
            'url':      'http://localhost:9900/stats.json'
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
            connection.request("GET", "%s?%s" % (parts[2], parts[4]))
        except Exception, e:
            self.log.error("Error retrieving HTTPD stats. %s" % e)
            print "FAILED TO CONNECT"
            return

        response = connection.getresponse()
        data = json.loads(response.read())
        for k,v in sorted(flatten(data,'').iteritems()):
            metric_name = k
            try:
                float(v)
                metric_value = v
                self.publish(metric_name, metric_value)
            except ValueError:
                pass