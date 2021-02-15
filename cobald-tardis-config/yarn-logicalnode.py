import argparse
import requests
import json
import time
import sqlite3
from filelock import FileLock
import os

# Define command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("drone_uuid", help="The uuid for this logical node assigned by tardis.")
parser.add_argument("cores", help="Number of logical cores of this drone on its YARN nodemanager.", type=int)
parser.add_argument("memory", help="Amount of memory in units of 1384 MB of this drone on its YARN nodemanager.", type=int)
args = parser.parse_args()

drone_cores = args.cores
drone_memory = args.memory * 1384 # 1384 MB is the minimum required memory for a single YARN executor


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



if __name__ == "__main__":
    print('Enter drone script')
    filename_lock = os.path.join(os.environ['COBALD_TARDIS_CONFIG_DIR'], 'yarn-logicalnode.lock')
    print(f'Path to filelock: {filename_lock}')
    lock = FileLock(filename_lock)

    filename_dronesdb = os.path.join(os.environ['COBALD_TARDIS_CONFIG_DIR'], 'yarn_drones.db')
    print(f'Path to drones database: {filename_dronesdb}')
    dronesconn = sqlite3.connect(filename_dronesdb)
    with dronesconn:
        dronescursor = dronesconn.cursor()
        insert_drone = """
            INSERT OR IGNORE INTO yarn_drones
            (drone_uuid, nm, status)
            VALUES
            (?, 'sg01.etp.kit.edu', 'Available')"""

        dronescursor.execute(insert_drone, (args.drone_uuid, ))
        dronesconn.commit()
        print(f"Succesfully inserted {args.drone_uuid} into database")

    rm = YarnResourceManager('portal1')
    nodes = rm.nodes

    # Insert a nodemanager into db if it doesn't exist yet
    filename_nmdb = os.path.join(os.environ['COBALD_TARDIS_CONFIG_DIR'], 'yarn_nm.db')
    print(f'Path to nodemanager database: {filename_nmdb}')
    nmconn = sqlite3.connect(filename_nmdb)

    with nmconn:
        nmcursor = nmconn.cursor()
        insert_nm = """
            INSERT OR IGNORE INTO yarn_nm
            (name, allocated_vcores, allocated_memory_mb)
            VALUES
            ('sg01.etp.kit.edu', ?, ?)"""
        nmcursor.execute(insert_nm, (1, 1024))
        nmconn.commit()

    # Retrieve current allocation and increase it
    with lock:
        # Update database
        nmcursor = nmconn.cursor()
        status_query = "SELECT allocated_vcores, allocated_memory_mb FROM yarn_nm WHERE name = 'sg01.etp.kit.edu'"
        allocated_vcores, allocated_memory = nmcursor.execute(status_query).fetchall()[0]
        print(f'Current resources in Yarn are {allocated_vcores} cores and {allocated_memory} memory')

        update_query = """
            UPDATE yarn_nm
            SET allocated_vcores = ?, allocated_memory_mb = ?
            WHERE name = 'sg01.etp.kit.edu'"""
        new_vcores = allocated_vcores + drone_cores
        new_memory = allocated_memory + drone_memory
        nmcursor.execute(update_query, (new_vcores, new_memory))
        nmconn.commit()

        # Update Yarn
        rm.set_resources(nodes[0], cores=new_vcores, memory=new_memory)

        print(f'Updated the database and Yarn to {new_vcores} cores and {new_memory} memory')
        time.sleep(1) # give yarn some time to process request sequentially

    while(True):
        time.sleep(10)
        # get Draining state from the db
        with dronesconn:
            status_query = "SELECT status FROM yarn_drones WHERE drone_uuid = ?"
            dronescursor = dronesconn.cursor()
            results = dronescursor.execute(status_query, (args.drone_uuid, )).fetchall()
            drone_status = results[0][0]
            print(f"Current drone status: {drone_status}")
        if drone_status == "Draining":
            # Remove cores and memory from the nm db
            print('Remove drone from the nodemanager database')
            with lock:
                # Remove cores from the db
                nmcursor = nmconn.cursor()
                status_query = "SELECT allocated_vcores, allocated_memory_mb FROM yarn_nm WHERE name = 'sg01.etp.kit.edu'"
                allocated_vcores, allocated_memory = nmcursor.execute(status_query).fetchall()[0]
                print(f'Current resources in Yarn are {allocated_vcores} cores and {allocated_memory} memory')

                update_query = """
                    UPDATE yarn_nm
                    SET allocated_vcores = ?, allocated_memory_mb = ?
                    WHERE name = 'sg01.etp.kit.edu'"""
                new_vcores = allocated_vcores - drone_cores
                new_memory = allocated_memory - drone_memory
                nmcursor.execute(update_query, (new_vcores, new_memory))
                nmconn.commit()

                # Update Yarn
                rm.set_resources(nodes[0], cores=new_vcores, memory=new_memory)
                print(f'Updated the database and Yarn to {new_vcores} cores and {new_memory} memory')

                time.sleep(1) # give yarn some time to process request sequentially

            # set NotAvailable in the db and leave the script
            print('Set drone as NotAvailable in the drones database')
            with dronesconn:
                drain_query = """UPDATE yarn_drones
                                 SET status = 'NotAvailable'
                                 WHERE drone_uuid = ?"""
                dronescursor = dronesconn.cursor()
                dronescursor.execute(drain_query, (args.drone_uuid, ))
                dronesconn.commit()

            # leave script
            break

    dronesconn.close()
    nmconn.close()

    print('Leave drone script')
