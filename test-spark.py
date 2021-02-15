import random
import os

from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName('Calculate Pi')
    .master('yarn')
    .config('spark.pyspark.python', os.environ['PYTHON_BINARY'])
    .getOrCreate()
)

def inside(p):
    x, y = random.random(), random.random()
    return x * x + y * y < 1

num_samples = 1e6
num_partitions = 128

count = (
    spark.sparkContext
         .parallelize(range(int(num_samples)), num_partitions)
         .filter(inside)
         .count()
)

print("Pi is roughly %.4f" % (4.0 * count / num_samples))

