import requests
from time import sleep
from os import environ
from datetime import datetime
import sqlite3

# Connection to NM database needed to retrieve total cpus of each node
filename_nmdb = environ['COBALD_TARDIS_NODEMANAGER_DATABASE']
nmconn = sqlite3.connect(filename_nmdb)
nmcursor = nmconn.cursor()
cpus_query = "SELECT cpus FROM yarn_nm WHERE name = ?"

class YarnResourceManager:
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

        resources_dict = {'cores': totalresources['vCores'],
                          'memory': totalresources['memory']}

        # Retrieve cpus from current nodemanager entry in the database
        cpus_query_result = nmcursor.execute(cpus_query, (node, )).fetchall()
        if(cpus_query_result):
            # Adjust containersCPUUsage value from the YARN REST API
            # Normalize it to the number of cpus actually in use by YARN on this node
            nm_cpus = cpus_query_result[0][0]
            adjusted_containers_cpu_usage = resourceutilization['containersCPUUsage'] * nm_cpus / totalresources['vCores']
        
            resources_dict.update({'nodeCPUUsage': round(resourceutilization['nodeCPUUsage'], 2),
                                   'containersCPUUsage': round(adjusted_containers_cpu_usage, 2),})

        return resources_dict

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
                apps.append({'id': id_, 'cpus': cpus, 'memory': memory, 'state': state})
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
        data['id'] = node
        print('node', data)

    # Applications
    apps = rm.apps
    for app in apps:
        print('app', app)

    sleep(10)
