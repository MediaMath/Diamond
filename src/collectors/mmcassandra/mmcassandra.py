import subprocess, socket, math

import diamond.collector


def parse_line(line):
    metric_name, rhs = line.strip().split(':', 1)
    rhs = rhs.strip()
    if ' ' in rhs:
        str_value, units = rhs.split(' ', 1)
        if units not in ('ms', 'ms.'):
            raise ValueError("Cannot parse " + repr(line))
    else:
        str_value = rhs

    try:
        value = float(str_value)
    except:
        value = str_value
    return metric_name, value


class Keyspace(object):
    def __init__(self, name, stats, tables):
        self.name = name
        self.stats = stats
        self.tables = tables

class Table(object):
    def __init__(self, name, stats):
        self.name = name
        self.stats = stats

def clean_key(key):
    return key.replace(' ', '_').replace(',', '_').replace('(', '').replace(')', '')


bad_keyspaces = ('system', 'system_traces')

class ColumnFamilyStatsCollector(diamond.collector.Collector):
    def collect(self):
        for keyspace in self.cfstats():
            if keyspace.name not in bad_keyspaces:
                for (key, value) in keyspace.stats:
                    name = 'cassandra.cfstats.{}.{}'.format(
                        keyspace.name, key)
                    self.publish(name, value)
                for table in keyspace.tables:
                    for (key, value) in table.stats:
                        name = 'cassandra.cfstats.{}.{}.{}'.format(
                            keyspace.name, table.name, key)
                        self.publish(name, value)


    def cfstats(self):
        output = subprocess.check_output(['nodetool', 'cfstats'])
        lines = [line for line in output.splitlines()
                 if line and (line != '----------------')]

        # cfstats output is structured in a very specific way: all lines are
        # key: value pairs prefixed by tabs. everything indented belongs to the

        keyspaces = []
        for line in lines:
            try:
                tab_count = len(line) - len(line.lstrip('\t'))
                if tab_count == 0:
                    key, value = parse_line(line)
                    assert key == 'Keyspace'
                    keyspaces.append(Keyspace(value, [], []))
                elif tab_count == 1:
                    key, value = parse_line(line)
                    if not math.isnan(value):
                        key, value = parse_line(line)
                        keyspaces[-1].stats.append((clean_key(key), value))
                elif tab_count == 2:
                    key, value = parse_line(line)
                    if key == 'Table':
                        keyspaces[-1].tables.append(Table(value, []))
                    else:
                        if not math.isnan(value):
                            keyspaces[-1].tables[-1].stats.append((clean_key(key), value))
                else:
                    raise ValueError
            except ValueError:
                self.log.error("Unable to parse line: %s" % line)

        return keyspaces
