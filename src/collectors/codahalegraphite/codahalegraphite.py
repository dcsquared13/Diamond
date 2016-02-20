import httplib
import urlparse
import diamond.collector


class CodaHaleGraphiteCollector(diamond.collector.Collector):

    def __init__(self, *args, **kwargs):
        super(CodaHaleGraphiteCollector, self).__init__(*args, **kwargs)

    def process_config(self):
        super(CodaHaleGraphiteCollector, self).process_config()

    def get_default_config_help(self):
        config_help = super(CodaHaleGraphiteCollector,
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
        config = super(CodaHaleGraphiteCollector, self).get_default_config()
        config.update({
            'path':     'codahale',
            'url':	'http://localhost:42001/nestlabsadmin/metrics/graphite'
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
        data = response.read()
        for line in data.split('\n'):
            try:
                metric_name, metric_value, time_stamp = line.split()
                self.publish(metric_name, metric_value)
            except Exception, e:
                self.log.error("Error parsing line. %s line: %s" % (e, line))
                pass
