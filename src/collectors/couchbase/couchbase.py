
#
# originally from: https://github.com/pythianliappis/couchbase_collector
#   modified to support multiple buckets and fixed auth failing
#
#


import diamond.collector
import urllib2
import base64
import json


class CouchBaseCollector(diamond.collector.Collector):

    def __init__(self, *args, **kwargs):
        super(CouchBaseCollector, self).__init__(*args, **kwargs)

    def process_config(self):
        super(CouchBaseCollector, self).process_config()

    def get_default_config_help(self):
        config_help = super(CouchBaseCollector,
                            self).get_default_config_help()
        config_help.update({
            'host': "Hostname",
            'port': "Port",
            'username': "Username",
            'password': "Password"
        })
        return config_help

    def get_default_config(self):
        config = super(CouchBaseCollector, self).get_default_config()
        config.update({
          'host':         'localhost',
          'port':         8091
        })

        return config

    def get_cb_data(self, url):

        # self.log.info( "get_cb_data: fail:" + str(self.config['host']))
        # for thing in self.config:
        #    self.log.info( "dump: %s" % thing )

        base64string = base64.encodestring('%s:%s' % (self.config['username'], self.config['password'])).replace('\n', '')
        request = urllib2.Request(url)
        request.add_header("Authorization", "Basic %s" % base64string)

        try:
            f = urllib2.urlopen(request) #self.make_conn(url))
        except urllib2.HTTPError, err:
            self.log.error("%s: %s", url, err)
            return False
        else:
            return json.load(f)

    def collect(self):
        self.config['paths'] = [str(node['name']) for node in self.get_cb_data('http://localhost:8091/pools/default/buckets')]
        for path in self.config['paths']:
            # the path key sets the namespace in graphite
            self.config['path'] = path

            self.log.info("Starting to collect!")
            self.log.info("collecting from host: " + self.config['host'] + " port: " + str(self.config['port']))

            self.log.info("Trying URL: " + 'http://localhost:8091/pools/default/buckets/' + str(path))
            # self.log.info( "collect: fail:" + str(self.config) )
            data = self.get_cb_data('http://localhost:8091/pools/default/buckets/' + path)

            if data:
                statobjname = "basicStats"
                [self.publish("%s.%s" % (statobjname,basicStatsKey), data[statobjname][basicStatsKey]) for basicStatsKey in data[statobjname].keys()]

                statobjname = "nodes"

                for nodeelem in data[statobjname]:
                    if "thisNode" in nodeelem.keys():
                        nodepath = "thisnode"

                        [self.publish("%s.%s" % (statobjname,interestingStatsKey), nodeelem["interestingStats"][interestingStatsKey]) for interestingStatsKey in nodeelem["interestingStats"].keys()]

                statobjname = "quota"
                [self.publish("%s.%s" % (statobjname,quotaKey), data[statobjname][quotaKey]) for quotaKey in data[statobjname].keys()]

                self.log.info("collected!")