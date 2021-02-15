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
