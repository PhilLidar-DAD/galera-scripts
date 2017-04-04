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

from pprint import pformat
from settings import *
import argparse
import logging
import MySQLdb
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


def check_cluster_status():
    # Check cluster status
    cluster_up = False
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

    # Get cluster status
    if dbcon:
        logger.info('Getting cluster status...')
        dbcur = dbcon.cursor()
        dbcur.execute("SHOW GLOBAL STATUS LIKE 'wsrep_cluster_status'")
        row = dbcur.fetchone()
        if row[1] == 'Primary':
            logger.info('Cluster %s is up.', CLUSTER['dbhost'])
            cluster_up = True
        dbcur.close()
        dbcon.close()

    return cluster_up


def check_mysqld_on_nodes():
    # Check each node status
    logger.info('Checking nodes...')
    up_nodes = []
    down_nodes = []
    for node in CLUSTER['nodes']:
        is_up = False

        # Check mysqld status
        logger.info('Checking mysqld status on %s...', node)
        ps_list = ['ssh', node,
                   "'ps", 'auxww', '|', 'grep', 'mysqld', '|', 'grep', '-v',
                   "grep'"]
        ps_cmd = ' '.join(ps_list)
        logger.debug('ps_cmd: %s', ps_cmd)
        try:
            ps = subprocess.Popen(ps_cmd, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, shell=True)
            ps.wait()
            res, _ = ps.communicate()
            logger.debug('res: %s', res)
            if res and len(res.split('\n')) >= 1:
                up_nodes.append(node)
                is_up = True
        except Exception:
            logger.exception('Error getting status for %s!', node)

        # If node is down, get seq. no
        if not is_up:
            logger.info('Getting seq. no on %s...', node)
            state_cmd = ['ssh', node, 'cat', '/var/lib/mysql/grastate.dat']
            logger.debug('state_cmd: %s', ' '.join(state_cmd))
            try:
                res = subprocess.check_output(state_cmd)
                for line in res.split('\n'):
                    if 'seqno' in line:
                        tokens = line.strip().split()
                        seqno = int(tokens[-1])
                        down_nodes.append((seqno, node))
            except Exception:
                logger.exception('Error getting seq. no. for %s!', node)

    return up_nodes, sorted(down_nodes)


def start_mariadb(node, new_cluster=False):
    logger.info('Starting mariadb service on %s...', node)
    if new_cluster:

        logger.info('Setting safe_to_bootstrap on %s...', node)
        sed_lst = ['ssh', node,
                   '"sed', '-i',
                   "'s/safe_to_bootstrap: 0/safe_to_bootstrap: 1/g'",
                   '/var/lib/mysql/grastate.dat"']
        sed_cmd = ' '.join(sed_lst)
        logger.debug('sed_cmd: %s', sed_cmd)
        try:
            res = subprocess.check_call(sed_cmd, shell=True)
        except Exception:
            logger.exception('Error setting safe_to_bootstrap on %s...',
                             node)
            logger.error('Exiting!')
            exit(1)

        start_cmd = ['ssh', node, 'galera_new_cluster']
    else:
        start_cmd = ['ssh', node, 'systemctl', 'restart', 'mariadb']
    logger.debug('start_cmd: %s', ' '.join(start_cmd))
    try:
        res = subprocess.check_call(start_cmd)
    except Exception:
        logger.exception('Error starting mariadb service on %s!', node)
    logger.info('NODE START: Sleeping for %smin/s...', DELAY)
    time.sleep(DELAY * 60)


def update_down_counters(up_nodes, down_nodes, down_counters):
    # Decrease down counters for up nodes
    for _, node in up_nodes:
        # Initialize if not present
        if node not in down_counters:
            down_counters[node] = 0
        # Decrease down counter
        down_counters[node] -= 1
        # Reset to 0 if negative
        if down_counters[node] < 0:
            down_counters[node] = 0

    # Increase down counters for down nodes
    for _, node in up_nodes:
        # Initialize if not present
        if node not in down_counters:
            down_counters[node] = 0
        # Increase down counter
        down_counters[node] += 1

    logger.info('Down counters:\n%s', pformat(down_counters, width=40))

    # If node has been down for more than the threshold, reboot the node
    for node, counter in down_counters.viewitems():
        if counter >= DOWN_THRESHOLD:
            logger.error('Cannot start %s! Rebooting node...', node)
            # Rebooting node
            subprocess.call(['reboot'])
            # Reset counter
            down_counters[node] = 0
            # Wait node to finish rebooting
            logger.info('NODE REBOOT: Sleeping for %smin/s...', DELAY)
            time.sleep(DELAY * 60)

if __name__ == '__main__':

    # Parse arguments
    args = parse_arguments()

    # Setup logging
    setup_logging(args)

    up_nodes = []
    down_counters = {}
    while True:

        logger.info('#' * 40)

        # Check cluster status
        cluster_up = check_cluster_status()

        # Check each node
        up_nodes, down_nodes = check_mysqld_on_nodes()

        logger.info('Up: %s', up_nodes)
        logger.info('Down: %s', down_nodes)

        # Update down counters
        update_down_counters(up_nodes, down_nodes, down_counters)

        # Start cluster nodes
        if not cluster_up:
            # If there are no up nodes, and there is at least 1 accessible
            # down node, start node with high seq. no
            seqno, node = down_nodes.pop()
            if len(up_nodes) == 0 and len(down_nodes) >= 1:
                logger.info('Initializing cluster...')
                start_mariadb(node, new_cluster=True)

        # Start all down nodes if there's at least 1 up node, and there are
        # down nodes
        if len(up_nodes) >= 1 and len(down_nodes) > 0:
            for _, node in down_nodes:
                start_mariadb(node)

        logger.info('LOOP: Sleeping for %smin/s...', DELAY)
        time.sleep(DELAY * 60)
