import random
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName('Calculate Pi')
    .master('yarn')
    .getOrCreate()
)

def inside(p):
    x, y = random.random(), random.random()
    return x * x + y * y < 1

num_samples = 1e10
num_partitions = 1024

count = (
    spark.sparkContext
         .parallelize(range(int(num_samples)), num_partitions)
         .filter(inside)
         .count()
)

print("Pi is roughly %.4f" % (4.0 * count / num_samples))

