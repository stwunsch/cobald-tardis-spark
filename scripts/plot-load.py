import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from json import loads
import numpy as np
import re


def read_load(filename):
    logs = []
    for line in open(filename):
        line = line.strip()

        matches = re.search('time (.*), num_cpu (.*), load_avg \((.*), (.*), (.*)\)', line)
        if not matches:
            raise Exception(f'Failed to parse {line} in {filename}')
        groups = matches.groups()

        time = datetime.strptime(groups[0], '%Y-%m-%d %H:%M:%S.%f')
        load = float(groups[2])

        logs.append((time, load))
    return logs


def read_monitoring(filename):
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


def plot(nodes, load_logs, mon_logs, skip_front = 0, skip_end = 0):
    # Gather arrays with the information
    time = []
    nodes_cores = {node: [] for node in nodes}
    for entry in mon_logs[skip_front:-(skip_end + 1)]:
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

    # Time difference from begin
    begin = time[0]
    end = time[-1]
    delta = [(t - begin).total_seconds() / 60 for t in time]

    # Plot
    plt.figure(figsize=(4.5, 4), dpi=300)

    # Cluster capacity
    sum_cores = np.array(nodes_cores[nodes[0]])
    for node in nodes[1:]:
        sum_cores += np.array(nodes_cores[node])
    plt.plot(delta, sum_cores - len(nodes), # subtract the application master for each node
            label='Cluster capacity', color='C3')

    # Load
    load = {}
    ticks = 30.0 # one tick every thirty seconds
    time_load_secs = list(range(int(delta[-1] * 60.0 / ticks )))
    load_nodes = {node: np.zeros(len(time_load_secs)) for node in nodes}
    for node in nodes:
        for time, load in load_logs[node]:
            idx = int((time - begin).total_seconds() / ticks)
            if idx < 0 or idx >= len(time_load_secs):
                continue
            load_nodes[node][idx] = max(load, load_nodes[node][idx]) # take the average load in the last tick

    for node in nodes: # subract 13 cores from sg04
        if 'sg04' in node:
            load_nodes[node] -= 13

    sum_load = np.zeros(len(time_load_secs))
    for node in nodes:
        sum_load += load_nodes[node]

    time_load_mins = np.array(time_load_secs) / 60.0 * ticks
    colors = ['C0', 'C1', 'C2', 'C4']
    for i in reversed(range(len(nodes))):
        plt.fill_between(time_load_mins, 0, sum_load, color=colors[i], label=nodes[i])
        sum_load -= load_nodes[nodes[i]]

    # Legend
    handles, labels = plt.gca().get_legend_handles_labels()
    handles = reversed(handles)
    labels = reversed(labels)
    plt.gca().legend(handles, labels, bbox_to_anchor=(0., 1.02, 1., .102), loc=3, ncol=2, mode="expand", borderaxespad=0.)

    plt.xlim(delta[0], delta[-1])
    plt.ylim(0, np.max(sum_cores))
    plt.xlabel('Runtime in minutes')
    plt.ylabel('Cores')

    plt.savefig('plot.png', bbox_inches='tight')


if __name__ == '__main__':
    mon_logs = read_monitoring('yarn-monitoring.log')
    nodes = [f'sg0{i}.etp.kit.edu' for i in range(1, 5)]
    load_logs = {node: read_load(f'load-monitoring-{node}.log') for node in nodes}
    plot(nodes, load_logs, mon_logs)
