# Integration of Spark into the Cobald/Tardis system

Setup scripts and documentation to integrate Spark into the Cobald/Tardis system

## Setup

1. Clone this repository including the submodules

```bash
git clone --recursive https://github.com/stwunsch/cobald-tardis-spark
```

2. Install the required software

The `install.sh` script installs the required Python and Java software.

```bash
cd cobald-tardis-spark/
./install.sh
```

3. Set the configuration

Have a look at the `config.sh` file, set the correct configuration and run the `configure.sh` script.

```bash
./configure.sh
```

## Test Spark running on Yarn

1. Adapt the config in `hadoop-config/yarn-site.xml` and set the number for `yarn.nodemanager.resource.cpu-vcores` to at least 2 and set the number for `yarn.nodemanager.resource.memory-mb` to at least 2500.

2. Go to the machine which should act as the master (aka resourcemanager in Yarn) and run

```bash
./run-resourcemanager.sh
```

3. Go to the machine which should act as the worker (aka nodemanager in Yarn) and run

```bash
./run-nodemanager.sh
```

3. Run the test script

```bash
./test-spark.sh
```
