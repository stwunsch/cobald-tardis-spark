import argparse
import os
import socket
import sqlite3

import requests

parser = argparse.ArgumentParser()
parser.add_argument('path', help='Path to the database.')
parser.add_argument(
    '--create', help='Create the drones db from scratch.', action='store_true')
parser.add_argument(
    '--print', help='Print the drones db.', action='store_true')
parser.add_argument(
    '--insert_nm', help='Insert this machine as a nodemanager in the database',
    action='store_true')
args = parser.parse_args()


class YarnResourceManager(object):
    def __init__(self, hostname, ip=8088):
        self.hostname = hostname
        self.ip = ip
        self.base = 'http://{HOSTNAME}:{IP}'.format(HOSTNAME=hostname, IP=ip)

    def _get_nodes(self):
        r = requests.get(self.base + '/ws/v1/cluster/nodes')
        nodes = []
        data = r.json()
        for node in data['nodes']['node']:
            nodes.append(node['id'])
        return nodes

    @property
    def nodes(self):
        return self._get_nodes()

    def get_resources(self, node):
        r = requests.get(self.base + '/ws/v1/cluster/nodes/' + node)
        data = r.json()

        totalresources = data['node']['totalResource']

        return {'id': node,
                'cores': totalresources['vCores'],
                'memory': totalresources['memory'], }

    def _get_apps(self):
        r = requests.get(self.base + '/ws/v1/cluster/apps')
        data = r.json()
        apps = []
        try:
            for app in data['apps']['app']:
                id_ = app['id']
                state = app['state']
                memory = app['allocatedMB']
                cpus = app['allocatedVCores']
                apps.append({'id': id_, 'cpus': cpus,
                             'memory': memory, 'state': state})
        except:
            pass
        return apps

    @property
    def apps(self):
        return self._get_apps()


def create_db():
    sqlconn = sqlite3.connect(args.path)
    sqlite_create_table_query = '''CREATE TABLE yarn_nm (
                                name TEXT PRIMARY KEY,
                                cpus INTEGER NOT NULL,
                                allocated_vcores INTEGER NOT NULL,
                                allocated_memory_mb INTEGER NOT NULL);'''
    cursor = sqlconn.cursor()
    cursor.execute(sqlite_create_table_query)
    sqlconn.commit()
    print('SQLite table created')

    cursor.close()
    sqlconn.close()
    print('SQLite connection is closed')


def insert_nm_in_db(hostname):
    rm = YarnResourceManager(os.environ['YARN_RESOURCEMANAGER'])
    sqlconn = sqlite3.connect(args.path)
    # Insert current nodemanager into the database
    insert_nm_query = '''
        INSERT INTO yarn_nm
        (name, cpus, allocated_vcores, allocated_memory_mb)
        VALUES
        (?, ?, ?, ?)'''

    # Retrieve full node address comparing the current hostname with the list
    # of node addresses in the cluster
    node_address = next(node for node in rm.nodes if hostname in node)
    node_data = rm.get_resources(node_address)

    cursor = sqlconn.cursor()
    cursor.execute(insert_nm_query, (node_data['id'],
                                     os.cpu_count(),
                                     node_data['cores'],
                                     node_data['memory']))
    sqlconn.commit()
    print(f'Nodemanager {node_address} added to table yarn_nm')

    cursor.close()
    sqlconn.close()
    print('SQLite connection is closed')


def print_db():
    sqliteConnection = sqlite3.connect(args.path)
    with sqliteConnection:
        cursor = sqliteConnection.cursor()
        cursor.execute('SELECT * FROM yarn_nm')
        for line in cursor.fetchall():
            print(line)


if __name__ == '__main__':
    if args.create:
        create_db()
    elif args.print:
        print_db()
    elif args.insert_nm:
        insert_nm_in_db(socket.gethostname())
