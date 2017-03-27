#!/usr/bin/env python2

'''
Copyright (c) 2017, Kenneth Langga (klangga@gmail.com)
All rights reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''

from settings import *
import argparse
import logging
import subprocess
import sys
import time

logger = logging.getLogger()


def parse_arguments():
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action="store_true")
    args = parser.parse_args()
    return args


def setup_logging(args):
    # Check verbosity for console
    if args.verbose:
        LOG_LEVEL = logging.DEBUG
    else:
        LOG_LEVEL = logging.INFO

    # Set log level
    logger.setLevel(LOG_LEVEL)

    # Setup console logging
    ch = logging.StreamHandler(sys.stdout)
    ch_formatter = logging.Formatter('%(filename)s \
(%(levelname)s,%(lineno)d): %(message)s')
    ch.setFormatter(ch_formatter)
    logger.addHandler(ch)


def check_cluster_status(up_nodes):
    # Check cluster status
    cluster_ok = False
    dbcon = None
    try:
        # Try connecting to galera vip
        logger.info('Connecting to galera cluster db: % s and checking \
status...', CLUSTER['dbhost'])
        dbcon = MySQLdb.connect(host=CLUSTER['dbhost'],
                                port=CLUSTER['dbport'],
                                user=CLUSTER['dbuser'],
                                passwd=CLUSTER['dbpass'])
    except Exception:
        logger.exception('Error connecting to database!')

        # Check previously up nodes
        if len(up_nodes) >= 3:
            logger.error('Up nodes already >= 3! \
Stopping VIP on this node...')
            # Stop cluster services on current node
            subprocess.call(['pcs', 'cluster', 'stop'])
            # Exit if this script hasn't already stopped
            exit(1)

    # Get cluster status
    if dbcon:
        logger.info('Getting cluster status...')
        dbcur = dbcon.cursor()
        dbcur.execute("SHOW GLOBAL STATUS LIKE 'wsrep_cluster_status'")
        row = dbcur.fetchone()
        if row[1] == 'Primary':
            logger.info('Galera cluster db: %s is ok.', CLUSTER['dbhost'])
            cluster_ok = True
        dbcur.close()
        dbcon.close()

    return cluster_ok


def check_mysqld_on_nodes():
    # Check each node status
    logger.info('Checking nodes...')
    up_nodes = []
    down_nodes = []
    for node in cluster['nodes']:
        # Check mysqld status
        logger.info('Checking mysqld status on %s...', node)
        ps_list = ['/usr/bin/ssh', cluster['nodeuser'] + '@' + node,
                   'ps', 'auxww', '|', 'grep', 'mysqld']
        ps_cmd = ' '.join(ps_list)
        logger.debug('ps_cmd: %s', ps_cmd)
        try:
            res = subprocess.Popen(ps_cmd, shell=True)
            if len(res.split('\n')) > 1:
                up_nodes.append(node)
                is_up = True
        except Exception:
            logger.exception('Error getting status for %s!', node)

        # If node is down, get seq. no
        if not is_up:
            logger.info('Getting seq. no on %s...', node)
            state_cmd = ['/usr/bin/ssh', cluster['nodeuser'] + '@' + node,
                         'cat', '/var/lib/mysql/grastate.dat']
            logger.debug('state_cmd: %s', state_cmd)
            try:
                res = subprocess.check_output(state_cmd)
                for line in res.split('\n'):
                    if 'seqno' in line:
                        tokens = line.strip().split()
                        seqno = int(tokens[-1])
                        down_nodes.add((seqno, node))
            except Exception:
                logger.exception('Error getting seq. no. for %s!', node)

    logger.info('Up: %s', up_nodes)
    logger.info('Down: %s', down_nodes)

    return up_nodes, sorted(down_nodes)


def start_mariadb(node):
    logger.info('Starting mariadb service on %s...', node)
    start_cmd = ['/usr/bin/ssh', cluster['nodeuser'] + '@' + node,
                 'sudo', 'systemctl', 'restart', 'mariadb']
    logger.debug('start_cmd: %s', start_cmd)
    try:
        res = subprocess.check_call(start_cmd)
        time.sleep(5 * 60)
    except Exception:
        logger.exception('Error starting mariadb service on %s!', node)


if __name__ == '__main__':

    # Parse arguments
    args = parse_arguments()

    # Setup logging
    setup_logging(args)

    up_nodes = []
    while True:

        # Check cluster status
        cluster_ok = check_cluster_status(up_nodes)

        # Check each node
        up_nodes, down_nodes = check_mysqld_on_nodes()

        # Start cluster nodes
        if not cluster_ok:
            # If there are no up nodes, and there is at least 1 accessible
            # down node, start node with high seq. no
            sqno_node = down_nodes.pop()
            if len(up_nodes) == 0 and len(down_nodes) >= 1:
                logger.info('Initializing cluster...')
                start_mariadb(sqno_node[1])

        # Start all down nodes if there's at least 1 up node, and there are
        # down nodes
        if len(up_nodes) >= 1 and len(down_nodes) > 0:
            for _, node in down_nodes:
                start_mariadb(node)