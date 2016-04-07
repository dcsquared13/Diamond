# coding=utf-8

"""
Collect memcached stats
Uses SASL encryption


#### Dependencies

 * subprocess
 * bmemcached

#### Example Configuration

MemcachedSaslCollector.conf

```
    enabled = True
    connection = localhost:11211
    username = memcached
    password = password

```

"""

import diamond.collector
import bmemcached


class MemcachedSaslCollector(diamond.collector.Collector):
    GAUGES = [
        'bytes',
        'connection_structures',
        'curr_connections',
        'curr_items',
        'threads',
        'reserved_fds',
        'limit_maxbytes',
        'hash_power_level',
        'hash_bytes',
        'hash_is_expanding',
        'uptime'
    ]

    def __init__(self, *args, **kwargs):
        super(MemcachedSaslCollector, self).__init__(*args, **kwargs)
        username = self.config['username']
        password = self.config['password']
        self.connection = self.config['connection']
        self.client = bmemcached.Client((self.connection,),
                                        username, password)

    def process_config(self):
        super(MemcachedSaslCollector, self).process_config()

    def get_default_config_help(self):
        config_help = super(MemcachedSaslCollector, self).get_default_config_help()
        config_help.update({
            'publish':
                "Which rows of 'status' you would like to publish." +
                " Telnet host port' and type stats and hit enter to see the" +
                " list of possibilities. Leave unset to publish all.",
            'connection':
                "Connection string to memcached.",
            'username':
                "Authentication user for memcached.",
            'password':
                "Authentication password for memcached.",
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(MemcachedSaslCollector, self).get_default_config()
        config.update({
            # Which rows of 'status' you would like to publish.
            # 'telnet host port' and type stats and hit enter to see the list of
            # possibilities.
            # Leave unset to publish all
            # 'publish': '',

            # Connection settings
            'connection': 'localhost:11211',
        })
        return config

    def get_stats(self):
        # stuff that's always ignored, aren't 'stats'
        ignored = ('libevent', 'pointer_size', 'time', 'version',
                   'repcached_version', 'replication', 'accepting_conns',
                   'pid')
        pid = None

        stats = {}

        raw_data = self.client.stats()
        data = raw_data[raw_data.keys()[0]]

        # build stats dictionary
        for key, value in data.items():
            if key in ignored:
                continue
            elif key == 'pid':
                pid = value
                continue
            else:
                stats[key] = float(value)

        # get max connection limit
        self.log.debug('pid %s', pid)

        return stats

    def collect(self):
        stats = self.get_stats()
        desired = self.config.get('publish', stats.keys())
        # for everything we want
        for stat in desired:
            if stat in stats:
                # we have it
                if stat in self.GAUGES:
                    self.publish_gauge(stat, stats[stat])
                else:
                    self.publish_counter(stat, stats[stat])
            else:
                # we don't, must be something configured in publish so we
                # should log an error about it
                self.log.error("No such key '%s' available, issue 'stats' "
                               "for a full list", stat)
