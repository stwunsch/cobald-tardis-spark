import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from json import loads
import numpy as np


def read(filename):
    logs = []
    for line in open(filename):
        line = line.strip()
        if line.startswith('time'):
            dt = datetime.strptime(line, 'time %Y-%m-%d %H:%M:%S.%f')
            logs.append({'time': dt})
        elif line.startswith('node'):
            last = logs[-1]
            datastr = line.replace('node ', '').replace("'", '"')
            data = loads(datastr)
            if not 'node' in last:
                last['node'] = {}
            last['node'][data['id']] = {}
            for key in [k for k in data if k != 'id']:
                last['node'][data['id']][key] = data[key]
        elif line.startswith('app'):
            # TODO: Code duplication
            last = logs[-1]
            datastr = line.replace('app ', '').replace("'", '"')
            data = loads(datastr)
            if not 'app' in last:
                last['app'] = {}
            last['app'][data['id']] = {}
            for key in [k for k in data if k != 'id']:
                last['app'][data['id']][key] = data[key]
        else:
            raise Exception('Read error')
    return logs


def plot(logs, skip_front = 0, skip_end = 0):
    # Select nodes and apps to be considered
    nodes = ['sg01', 'sg02', 'sg03', 'sg04'] # stupid substring matching!
    apps = ['_0001', '_0002', '_0003'] # stupid substring matching!

    # Gather arrays with the information
    time = []
    nodes_cores = {node: [] for node in nodes}
    apps_cores = {app: [] for app in apps}
    for entry in logs[skip_front:-(skip_end + 1)]:
        # Time
        time.append(entry['time'])

        # Nodes
        for node in nodes:
            if not 'node' in entry:
                nodes_cores[node].append(0)
            else:
                found = False
                for key in entry['node']:
                    if node in key:
                        nodes_cores[node].append(entry['node'][key]['cores'])
                        found = True
                        break
                if not found:
                    nodes_cores[node].append(0)

        # Apps
        # TODO: Code duplication
        for app in apps:
            if not 'app' in entry:
                apps_cores[app].append(0)
            else:
                found = False
                for key in entry['app']:
                    if app in key:
                        apps_cores[app].append(max(entry['app'][key]['cpus'], 0))
                        found = True
                        break
                if not found:
                    apps_cores[app].append(0)

    # Plot
    plt.figure(figsize=(4.5, 4), dpi=300)
    begin = time[0]
    delta = [(t - begin).total_seconds() / 60 for t in time]

    # Cluster capacity
    sum_cores = np.zeros(len(apps_cores[apps[0]]))
    for app in apps:
        sum_cores += np.array(apps_cores[app])
    for i, app in enumerate(apps):
        plt.fill_between(delta, 0, sum_cores,
                label='Application #' + str(i + 1), color='C' + str(i))
        sum_cores -= np.array(apps_cores[app])

    sum_cores = np.array(nodes_cores[nodes[0]])
    for node in nodes[1:]:
        sum_cores += np.array(nodes_cores[node])
    plt.plot(delta, sum_cores, label='Cluster capacity', color='C3')

    def swap(h):
        tmp = h.pop(0)
        h.append(tmp)

    handles, labels = plt.gca().get_legend_handles_labels()
    swap(handles)
    swap(labels)
    plt.gca().legend(handles, labels, bbox_to_anchor=(0., 1.02, 1., .102), loc=3, ncol=2, mode="expand", borderaxespad=0.)

    plt.xlim(delta[0], delta[-1])
    plt.ylim(0, np.max(sum_cores) + 1)
    plt.xlabel('Runtime in minutes')
    plt.ylabel('Cores')
    plt.savefig('plot.png', bbox_inches='tight')


if __name__ == '__main__':
    logs = read('monitoring.log')
    plot(logs)
