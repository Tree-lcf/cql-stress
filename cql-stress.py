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

class Connection(object):
    """
    
    """
    session = None

    def connect(self, nodes):
        cluster = Cluster(nodes)
        metadata = cluster.metadata
	self.session = cluster.connect()
        log.info('connect to cluster: ' + metadata.cluster_name)

    def run_query(self, query, rate, keyspace):
	self.query = query
	self.keyspace = 'use ' + keyspace
	self.session.execute(self.keyspace)
	self.session.execute(self.query)

    def close(self):
        self.session.cluster.shutdown()
        log.info('Connection closed.')

class myThread (threading.Thread):
    def __init__(self,object,host,query,rate,keyspace):
	self.object = object
	self.host = host
	self.query = query
	self.rate = rate
	log.info('host= %s', host)
	self.object.connect(self.host)
	
    def run(self):
	self.thread_number = threading.currentThread()
	log.info('in thread number %d', self.thread_number)
 
"""
	while True:
	    self.object.run_query(query, rate, keyspace)
	    time.sleep(1)
"""

class Pool(object):
    """
    Manages multiple connections to one or more Cassandra clusters.
    """
    def __init__(self, keyspace, srcip=['']):
        self.keyspace = keyspace
        self.connections = {}
        self.ready = []
        self.running = True
        self.set_query(None, 0)

    def set_query(self, query, rate):
	self.query_string = query
   	self.query_rate = rate

    def run(self, hosts, totalconns):
	needed = totalconns
	last_conn_time = 0
	conn_threads = []
	log.info('create a client object')
	client = Connection()
	"""
	Adds in new connections to the host at the given rate,
	until the total count is reached.
	"""
	while self.running:
            for i in range(needed):
                conn_threads.append(myThread(client,hosts,self.query_string,self.query_rate,self.keyspace))
                time.sleep(1)

	    log.info('done connectiong')
	    break

	while self.running:
	    now = time.time()
	    time.sleep(1)
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

    p = Pool(opts.keyspace, opts.srcip or [''])
    p.set_query(opts.query, opts.rate)
    p.run(opts.hosts, opts.nconns)
