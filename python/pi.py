import sys
import time
from random import random
from pyspark.sql import SparkSession

def dart_mapper(_):
    x = 2 * random() - 1
    y = 2 * random() - 1
    return 1 if (x * x + y * y <= 1) else 0

def dart_reducer(a, b):
    return a + b

# Approximates Pi by throwing darts at random at a unit circle

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: pi.py <slices>", file=sys.stderr)
        sys.exit(-1)

    slices = int(sys.argv[1])
    number_of_throws = 100000 * slices
    print(f'About to throw {number_of_throws} darts, ready? Stay away from the target!')

    start = time.perf_counter()

    spark = (SparkSession.builder
            # Use .master('local') for one thread/core,
            # .master('local[2]') for two, * for all logical cores
            .master('local[*]')
            .appName('Pi Approximation')
            .getOrCreate()
            )
    end = time.perf_counter()
    print(f'Session initialized in {end-start}s')
    start = end

    # You could do this:
    # df = spark.createDataFrame(range(number_of_throws), "int")
    # rdd = df.rdd
    # ...but this is slightly more sophisticated:
    rdd = spark.sparkContext.parallelize(range(number_of_throws), slices)
    end = time.perf_counter()
    print(f'Initial dataframe built in {end-start}s')
    start = end

    rdd = rdd.map(dart_mapper)
    end = time.perf_counter()
    print(f'Throwing darts done in {end-start}s')
    start = end

    darts_in_circle = rdd.reduce(dart_reducer)
    end = time.perf_counter()
    print(f'Analyzing result in in {end-start}s')
    start = end

    print(f'Pi is roughly {4.0 * darts_in_circle / number_of_throws}')
