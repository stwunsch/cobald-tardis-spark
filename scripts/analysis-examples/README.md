# Analysis examples

## Environment setup

On top of the things needed for the distributed system itself (Spark, YARN,
CoBalD/TARDIS, HTCondor), the scripts in this folder need to be run in an
environment with ROOT >= 6.24.

## Examples description

### cpubound.py

Creates an empty RDataFrame with one billion entries. Then sums the entries N
times, where N can be input from the command line (default 1).

```
usage: cpubound.py [-h] [--out OUT] [--nops NOPS]

optional arguments:
  -h, --help   show this help message and exit
  --out OUT    Name of the csv where the execution time of the analysis should be written.
  --nops NOPS  How many sum operations to run.
```

### dimuon.py

Run the Dimuon analysis. Number of replica files can be chosen.

```
usage: dimuon.py [-h] [--out OUT] [--nfiles NFILES]

optional arguments:
  -h, --help       show this help message and exit
  --out OUT        Name of the csv where the execution time of the analysis should be written.
  --nfiles NFILES  How many dimuon files replicas to run the analysis with. Accepts values in range [1,100].
```

### htt

The htt subfolder contains the scripts needed to run the HiggsTauTau analysis.

* full.py : Run the full analysis (skim + histograms) in one go.
* skim.py : Run the skimming part of the analysis and output partial snapshots
  on a location accessible by the distributed workers.
* histos.py : Create histograms from the partial snapshot files.
* skim.h : Header with functions needed in the skimming part of the analysis.