import requests

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--cores", type=int, help="Number of cores to allocate on the nodes.")
parser.add_argument("--memory", type=int, help="Amount of memory (MB) to allocate on the nodes.")
parser.add_argument("--nodes", nargs="+", help="Space separated list of nodes which resources should be changed.")
args = parser.parse_args()

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
cluster_nodes = rm.nodes

thiscores = args.cores if args.cores else 4
thismemory = args.memory if args.memory else 5000

if args.nodes:
    for nodename in args.nodes:
        print(f'Selected node name {nodename}')
        name_in_address = [nodename in fulladdress for fulladdress in cluster_nodes]
        if any(name_in_address):
            node = cluster_nodes[name_in_address.index(True)]
            print(f'Set resources for node {node}: ')
            rm.set_resources(node, cores=thiscores, memory=thismemory)
            print('Node resources: ', rm.get_resources(node))
        else:
            print('Selected node name is not available')
            print(f'Available nodes {cluster_nodes}')
else:
    for node in cluster_nodes:
        print(f'Set resources for node {node}: ')
        rm.set_resources(node, cores=thiscores, memory=thismemory)
        print('Node resources: ', rm.get_resources(node))
