import requests

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

    def set_resources(self, node, cores, memory):
        payload = {"resource": {"memory": memory, "vCores": cores}, "overCommitTimeout": -1}
        r = requests.post(self.base + '/ws/v1/cluster/nodes/' + node + '/resource', json=payload)

rm = YarnResourceManager('portal1')
nodes = rm.nodes
for node in nodes:
    print(f'Set resources for node {node}')
    rm.set_resources(node, cores=4, memory=5000)
