#!/usr/bin/env python
"""
Connection stress test tool for Cassandra's binary protocol based on python driver
"""
import sys
import socket
import struct
import select
import logging
import time
import random
import argparse
import itertools
import threading
import signal
from cassandra.cluster import Cluster

log = logging.getLogger('cql-stress')

# create one connection first

class Connections(object):
    """
    Tracks a single connection to Cassandra.
    """
    session = None

    def connect(self, nodes):
        cluster = Cluster(nodes)
        metadata = cluster.metadata
	self.session = cluster.connect()
        log.info('connect to cluster: ' + metadata.cluster_name) 
        
    def close(self):
        self.session.cluster.shutdown()
        log.info('Connection closed.')

class Pool(object):
    """
    Manages multiple connections to one or more Cassandra clusters.
    """
    def __init__(self, keyspace, srcip=['']):
        self.keyspace = keyspace
        
    def run(self, hosts, totalconns, addcount=30, addrate=1, constant=False):

        client =  Connections()
        log.info('hosts=%s', hosts)
        client.connect(['10.236.134.27'])
        time.sleep(60)
        client.close       


class FullHelpParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)


if __name__ == '__main__':
    parser = FullHelpParser(prog='cql3-stress', usage='%(prog)s [options] host...')
    parser.add_argument('-k', '--keyspace', dest='keyspace', action='store', type=str,
                        default='system',
                        help='Keyspace to connect to')
    parser.add_argument('-q', '--query', dest='query', action='store', type=str,
                        default='select * from schema_keyspaces limit 1',
                        help='Query to issue')
    parser.add_argument('-n', '--nconns', dest='nconns', action='store', type=int,
                        default=100,
                        help='Number of connections to establish to each given Cassandra host')
    parser.add_argument('-a', '--addrate', dest='addrate', action='store', type=str,
                        default='20/2',
                        help='Add new connections every interval (count / seconds)')
    parser.add_argument('-c', '--constant', dest='constant', action='store_true',
                        help='Keep a constant transaction rate even as connections are added')
    parser.add_argument('-r', '--rate', dest='rate', action='store', type=float,
                        default='0.1',
                        help='Number of queries per second per connection')
    parser.add_argument('-s', '--srcip', dest='srcip', action='append', type=str,
                        help='Bind to source IP (can be specified multiple times)')
    parser.add_argument('hosts', nargs='+')

    opts = parser.parse_args()

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s')
    log.setLevel(logging.INFO)

    log.info('conns=%d keyspace=%s query=%s IP=%s' , opts.nconns, opts.keyspace, opts.query, opts.hosts)

# create 1000 connections to cluster

    p = Pool(opts.keyspace, opts.srcip or [''])
#    p.set_query(opts.query, opts.rate)
    addcount, addrate = opts.addrate.split('/')
    p.run(opts.hosts, opts.nconns, int(addcount), int(addrate), opts.constant)
