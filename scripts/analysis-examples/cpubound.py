import argparse
import os
from datetime import datetime

import pyspark
import ROOT

ROOT.gROOT.SetBatch(True)
RDataFrame = ROOT.RDF.Experimental.Distributed.Spark.RDataFrame

parser = argparse.ArgumentParser()
parser.add_argument("--out", help="Name of the csv where the execution time of the analysis should be written.")
parser.add_argument("--nops", help="How many sum operations to run.", type=int)
args = parser.parse_args()

# Retrieve current time, just needed to give a name to the output csv file if
# it wasn't given already as command line argument
start_time = datetime.now()

# Configuration to connect to the YARN cluster
conf = {
    "spark.master": "yarn",
    "spark.executorEnv.LD_LIBRARY_PATH": os.environ["LD_LIBRARY_PATH"],
    "spark.yarn.queue": "cpubound"
}

sconf = pyspark.SparkConf().setAll(conf.items())
sc = pyspark.SparkContext(conf=sconf)

# Create a distributed RDataFrame with a billion entries and 500 partitions
nentries = int(1e9)
df = RDataFrame(nentries, sparkcontext=sc, npartitions=500)

# Depending on how many "Sum" actions are requested we can tweak the runtime of the app
nops = args.nops if args.nops else 1
sum_op_list = [df.Sum("rdfentry_") for i in range(nops)]

# Start a stopwatch and trigger the execution of the computation graph.
t = ROOT.TStopwatch()
sum_value = sum_op_list[0].GetValue()
t.Stop()
realtime = round(t.RealTime(), 2)

# Decide the name of the output csv to store runtime information.
filename = f"test1_{start_time.year}_{start_time.month}_{start_time.day}_{start_time.hour}_{start_time.minute}.csv"
outcsv = args.out if args.out else filename

with open(outcsv, "a+") as f:
    f.write(str(realtime))
    f.write("\n")
