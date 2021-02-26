from os import getloadavg, sched_getaffinity
from datetime import datetime
from time import sleep


def main():
    while True:
        print(f'time {datetime.now()}, num_cpu {len(sched_getaffinity(0))}, load_avg {getloadavg()}')
        sleep(10)


if __name__ == '__main__':
    main()
