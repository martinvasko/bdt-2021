import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Lazy evaluation in Spark

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: laziness.py <input1.csv>", file=sys.stderr)
        sys.exit(-1)

    start = time.perf_counter()

    # 1. Get session

    spark = SparkSession.builder.master('local').appName('Efficient Laziness').getOrCreate()
    end = time.perf_counter()
    print(f'1. Creating a session {end-start}')
    start = end

    # 2. Load dataset

    df_src = spark.read.format('csv').option('header', True).load(sys.argv[1])
    end = time.perf_counter()
    print(f'2. Loading initial dataset {end-start}')
    start = end

    print(f'Initial # of records {df_src.count()}')

    # 3. Build larger dataset

    df = df_src
    for i in range(60):
        df = df.union(df_src)
    end = time.perf_counter()
    print(f'3. Building full dataset {end-start}')
    start = end

    # 4. Clean up

    df = (df
            .withColumnRenamed('Lower Confidence Limit', 'lcl')
            .withColumnRenamed('Upper Confidence Limit', 'ucl')
            )

    end = time.perf_counter()
    print(f'4. Clean-up {end-start}')
    start = end

    # 5. Perform transformation

    df = (df
            .withColumn('avg', expr('(lcl+ucl)/2'))
            .withColumn('lcl2', df.lcl)
            .withColumn('ucl2', df.ucl)
            )

    df = (df
            .drop('avg')
            .drop('lcl2')
            .drop('ucl2')
            )

    end = time.perf_counter()
    print(f'5. Transformation {end-start}')
    start = end

    # Step 6: Action

    df = df.collect()

    end = time.perf_counter()
    print(f'6. Final action {end-start}')

    print(f'Final # of records {len(df)}')
