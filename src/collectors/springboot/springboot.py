import httplib
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


class SpringBootCollector(diamond.collector.Collector):
    _DEFAULT_HOST = 'localhost'

    def __init__(self, *args, **kwargs):
        super(SpringBootCollector, self).__init__(*args, **kwargs)

    def process_config(self):
        super(SpringBootCollector, self).process_config()

    def get_default_config_help(self):
        config_help = super(SpringBootCollector,
                            self).get_default_config_help()
        config_help.update({
            'path': "Path",
            'uri': "URI",
            'instances': "Comma separated list of instances"
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(SpringBootCollector, self).get_default_config()
        config.update({
            'path': 'springboot',
            'uri':  '/metrics',
            'instances': 'test:1234,test:2345'
        })
        return config

    def load_instances_config(self):
        instance_list = self.config['instances']
        service_path = self.config['uri']

        if isinstance(instance_list, basestring):
            instance_list = [instance_list]

        instances = {}
        for instance in instance_list:
            (nickname, hostport) = instance.split(':', 1)
            instances[nickname] = (self._DEFAULT_HOST, hostport, service_path)

        self.log.debug("Configured instances: %s" % instances.items())
        return instances

    def collect(self):
        instances = self.load_instances_config()
        for nick in instances.keys():
            (host, port, service_path) = instances[nick]
            self.log.debug("Instance: %s => (%s)" % (nick, instances[nick]))
            try:
                self.collect_instance(nick, host, int(port), service_path)
            except Exception, e:
                self.log.error("Error retrieving metrics for %s => (%s). %s" % (nick, instances[nick], e))

    def collect_instance(self, nick, host, port, service_path):
        """Collect metrics from a single Spring Boot instance
:param str nick: nickname of SpringBoot instance
:param str host: SpringBoot host
:param int port: SpringBoot port
:param str service_path: url path of metric
        """
        connection = httplib.HTTPConnection(host, port)
        connection.request("GET", "%s" % service_path)

        response = connection.getresponse()
        data = json.loads(response.read())
        for k,v in sorted(flatten(data,'').iteritems()):
            metric_name = nick + '.' + k
            try:
                float(v)
                metric_value = v
                self.publish(metric_name, metric_value)
                self.log.debug("Pushed metric %s=%s" % (metric_name, metric_value))
            except ValueError, e:
                self.log.error("Failed to parse metric value. %s" % e)
                pass
