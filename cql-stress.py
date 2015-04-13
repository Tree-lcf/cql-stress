#!/usr/bin/env python
"""
Connection stress test tool for Cassandra's binary protocol based on python driver
"""
import sys
import select
import logging
import time
import argparse
import threading
import signal
from cassandra.cluster import Cluster

log = logging.getLogger('cql-stress')
running = True

def stophandler(signum, frame):
    log.info('Terminate threads and exit')
    global running
    running = False

class Connection(object):
    """
    Using datastax driver to connect and run queries 
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
	threading.Thread.__init__(self)
	self.object = object
	self.host = host
	self.query = query
	self.rate = rate
	self.keyspace = keyspace
	self.object.connect(self.host)
	
    def run(self):
	global running
	while running:
	    self.object.run_query(self.query, self.rate, self.keyspace)
	    time.sleep(1/self.rate)
	log.info('%s ends', self.name)
	exit(0)

class Pool(object):
    """
    Manages multiple connections to one or more Cassandra clusters.
    """
    def __init__(self, keyspace, srcip=['']):
        self.keyspace = keyspace
        self.set_query(None, 0)

    def set_query(self, query, rate):
	self.query_string = query
   	self.query_rate = rate

    def run(self, hosts, totalconns):
	needed = totalconns
	last_conn_time = 0
	conn_threads = []
	global running
	log.info('create a client object')
	client = Connection()
	"""
	Adds in new connections to the host at the given rate,
	until the total count is reached.
	"""
	while running:
            for i in range(needed):
                conn_threads.append(myThread(client,hosts,self.query_string,self.query_rate,self.keyspace))
                time.sleep(1)

	    for i in range(needed):
		conn_threads[i].setDaemon(True)
	        conn_threads[i].start()

	    log.info('done connecting')
	    break

	signal.signal(signal.SIGINT, stophandler)
	signal.pause()
	log.info('received keyboardInterrupt')
	log.info('waiting for threads to exit')
	time.sleep(2*1/opts.rate)
	client.close

class FullHelpParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)

if __name__ == '__main__':
    parser = FullHelpParser(prog='cql-stress', usage='%(prog)s [options] host...')
    parser.add_argument('-k', '--keyspace', dest='keyspace', action='store', type=str,
                        default='system',
                        help='Keyspace to connect to')
    parser.add_argument('-q', '--query', dest='query', action='store', type=str,
                        default='select * from schema_keyspaces limit 1',
                        help='Query to issue')
    parser.add_argument('-n', '--nconns', dest='nconns', action='store', type=int,
                        default=100,
                        help='Number of driver connection requests to each given Cassandra host, it create 3 connections in the designated host and 2 in its neighbors')
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
