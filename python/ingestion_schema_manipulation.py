import sys
from pyspark.sql import SparkSession

def setup_spark_session():
    '''Creates a session on a local master.'''
    return SparkSession.builder.master('local').appName('Load Restaurants').getOrCreate()

def read_file_source(spark, source):
    '''Reads a CSV file with header and stores it in a dataframe.'''
    return spark.read.format('csv').option('header', True).load(source)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: ingestion_schema_manipulation <input.csv>", file=sys.stderr)
        sys.exit(-1)

    spark = setup_spark_session()

    df = read_file_source(spark, sys.argv[1])

    print('*** Right after ingestion')
    df.show(n=5)
    df.printSchema()
    print(f'We have {df.count()} records')

    spark.stop()
