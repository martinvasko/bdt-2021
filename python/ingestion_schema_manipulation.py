import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat


def setup_spark_session():
    '''Creates a session on a local master.'''
    return SparkSession.builder.master('local').appName('Load Restaurants').getOrCreate()

def read_file_source(spark, source):
    '''Reads a CSV file with header and stores it in a dataframe.'''
    return spark.read.format('csv').option('header', True).load(source)

def transform_columns(df):
    '''Transforms a bunch of columns.'''
    return (df
            .withColumn('county', lit('Wake'))
            .withColumnRenamed('HSISID', 'datasetId')
            .withColumnRenamed('NAME', 'name')
            .withColumnRenamed('ADDRESS1', 'address1')
            .withColumnRenamed('ADDRESS2', 'address2')
            .withColumnRenamed('CITY', 'city')
            .withColumnRenamed('STATE', 'state')
            .withColumnRenamed('POSTALCODE', 'zip')
            .withColumnRenamed('PHONENUMBER', 'tel')
            .withColumnRenamed('RESTAURANTOPENDATE', 'dateStart')
            .withColumnRenamed('FACILITYTYPE', 'type')
            .withColumnRenamed('X', 'geoX')
            .withColumnRenamed('Y', 'geoY')
            .drop('OBJECTID')
            .drop('PERMITID')
            .drop('GEOCODESTATUS')
            )

def add_id_column(df):
    '''Adds a synthetic id column.'''
    return (df
            .withColumn('id', concat(
                df.state,
                lit('_'),
                df.county,
                lit('_'),
                df.datasetId
                )
            )
        )

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: ingestion_schema_manipulation <input.csv>", file=sys.stderr)
        sys.exit(-1)

    # Setup

    spark = setup_spark_session()

    # Ingestion (Exercise 5)

    df = read_file_source(spark, sys.argv[1])

    print('*** Right after ingestion')
    df.show(n=5)
    df.printSchema()
    print(f'We have {df.count()} records')

    # Transformation (Exercise 6)

    df = transform_columns(df)

    print('*** After transformation')
    df.show(n=5)
    df.printSchema()

    # Unique identifier transformation (Exercise 7)
    df = add_id_column(df)

    # Exercise 8, I suppose
    print('*** After adding new unique ID')
    df.show(n=5)
    df.printSchema()

    # Repartitioning (Exercise 9)

    print(f'Partition count before repartition: {df.rdd.getNumPartitions()}')
    df = df.repartition(4)
    print(f'Partition count after repartition: {df.rdd.getNumPartitions()}')

    spark.stop()

# UNIT TESTS
# XXX THIS IS NOT BEST PRACTICE.
# Functionality and test cases should be separated into classes.
import unittest
class TestIngestTransform(unittest.TestCase):

    def setUp(self):
        self.spark = setup_spark_session()

    def test_ingest(self):
        # XXX TODO Adjust this to your environment
        path_csv_file = '../data/input/spark/Restaurants_in_Wake_County.csv'
        self.df = read_file_source(self.spark, path_csv_file)
        self.assertGreater(len(self.df.columns), 0)
        self.assertGreaterEqual(self.df.count(), 0)
