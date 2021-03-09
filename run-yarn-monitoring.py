import sqlite3
from datetime import datetime
from os import environ
from time import sleep

import requests

# The nodemanager database contains the number of logical cores of each NM
filename_nmdb = environ['COBALD_TARDIS_NODEMANAGER_DATABASE']
cpus_query = 'SELECT cpus FROM yarn_nm WHERE name = ?'

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
        resourceutilization = data['node']['resourceUtilization']

        nmconn = sqlite3.connect(filename_nmdb)
        with nmconn:
            nmcursor = nmconn.cursor()
            # Retrieve cpus from current nodemanager entry in the database
            cpus_query_result = nmcursor.execute(
                cpus_query, (node, )).fetchall()

            # Adjust containersCPUUsage value from the YARN REST API. Normalize
            # it to the number of cpus actually in use by YARN on this node
            nm_cpus = cpus_query_result[0][0]
            containers_cpu_usage = resourceutilization['containersCPUUsage'] * \
                nm_cpus / totalresources['vCores']

        return {
            'id': node,
            'cores': totalresources['vCores'],
            'memory': totalresources['memory'],
            'nodeCPUUsage': round(resourceutilization['nodeCPUUsage'], 2),
            'containersCPUUsage': round(containers_cpu_usage, 2), }

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


rm = YarnResourceManager(environ['YARN_RESOURCEMANAGER'])

while True:
    # Time
    print('time', datetime.now())

    # Nodes and allocated resources
    nodes = rm.nodes
    for node in nodes:
        data = rm.get_resources(node)
        print('node', data)

    # Applications
    apps = rm.apps
    for app in apps:
        print('app', app)

    sleep(10)
