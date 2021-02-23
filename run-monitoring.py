import requests
from time import sleep
from os import environ
from datetime import datetime

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
        resources = data['node']['totalResource']
        return {'cores': resources['vCores'], 'memory': resources['memory']}

    def _get_apps(self):
        r = requests.get(self.base + '/ws/v1/cluster/apps')
        data = r.json()
        apps = []
        for app in data['apps']['app']:
            id_ = app['id']
            state = app['state']
            memory = app['allocatedMB']
            cpus = app['allocatedVCores']
            apps.append({'id': id_, 'cpus': cpus, 'memory': memory, 'state': state})
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
