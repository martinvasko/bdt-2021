import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, concat, element_at


def setup_spark_session():
    '''Creates a session on a local master.'''
    return SparkSession.builder.master('local').appName('Load Restaurants').getOrCreate()

def read_file_source(spark, source):
    '''Reads a CSV file with header and stores it in a dataframe.'''
    return spark.read.format('csv').option('header', True).load(source)

def read_json_source(spark, source):
    '''Reads a JSON file and stores it in a dataframe.'''
    return spark.read.format('json').load(source)

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

def transform_json_columns(df):
    '''Transforms a bunch of columns.'''
    return (df
            # Single nested fields can not be renamed in PySpark, Scala, etc
            .selectExpr('fields.*')
            .withColumn('county', lit('Durham'))
            .withColumnRenamed('id', 'datasetId')
            .withColumnRenamed('premise_name', 'name')
            .withColumnRenamed('premise_address1', 'address1')
            .withColumnRenamed('premise_address2', 'address2')
            .withColumnRenamed('premise_city', 'city')
            .withColumnRenamed('premise_state', 'state')
            .withColumnRenamed('premise_zip', 'zip')
            .withColumnRenamed('premise_phone', 'tel')
            .withColumnRenamed('opening_date', 'dateStart')
            #.withColumnRenamed('closing_date', 'dateEnd')
            .withColumnRenamed('type_description', 'type')
            # Bit of magic here...easier to read this than to find out
            # what the correct function is
            .withColumn('geoX', element_at('geolocation', 1))
            .withColumn('geoY', element_at('geolocation', 2))
            .drop('geolocation')
            .drop('est_group_desc')
            .drop('hours_of_operation')
            .drop('insp_freq')
            .drop('risk')
            .drop('rpt_area_desc')
            .drop('seats')
            .drop('sewage')
            .drop('smoking_allowed')
            .drop('status')
            .drop('transitional_type_desc')
            .drop('water')
            .drop('closing_date')
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
    if len(sys.argv) < 2:
        print("Usage: ingestion_schema_manipulation <input1.csv> [<input2.json>]", file=sys.stderr)
        sys.exit(-1)

    # Setup

    spark = setup_spark_session()

    # Ingestion (Exercise 5)

    df = read_file_source(spark, sys.argv[1])

    print('========\n*** Right after ingestion')
    df.show(n=5)
    df.printSchema()
    print(f'We have {df.count()} records')

    # Transformation (Exercise 6)

    df = transform_columns(df)

    print('========\n*** After transformation')
    df.show(n=5)
    df.printSchema()

    # Unique identifier transformation (Exercise 7)
    df = add_id_column(df)

    # Exercise 8, I suppose
    print('========\n*** After adding new unique ID')
    df.show(n=5)
    df.printSchema()

    # Repartitioning (Exercise 9)

    print(f'Partition count before repartition: {df.rdd.getNumPartitions()}')
    df = df.repartition(4)
    print(f'Partition count after repartition: {df.rdd.getNumPartitions()}')

    # JSON file & unions (Exercise 10+)
    # Only do this if we also have a JSON file
    if len(sys.argv) >= 3:
        df_json = read_json_source(spark, sys.argv[2])
        print('========\n*** Raw JSON data')
        df_json.show(n=5)
        df_json.printSchema()

        # Transform JSON dataframe (Exercise 11)
        df_json = transform_json_columns(df_json)
        print('========\n*** After transformation')
        df_json.show(n=5)
        df_json.printSchema()

        df_json = add_id_column(df_json)
        print('========\n*** After adding new unique ID')
        df_json.show(n=5)
        df_json.printSchema()

        # Repartition (Exercise 12)
        print(f'Partition count before repartition: {df_json.rdd.getNumPartitions()}')
        df_json = df_json.repartition(4)
        print(f'Partition count after repartition: {df_json.rdd.getNumPartitions()}')

        # Unions (Exercise 13)
        # 1. unionAll() (union() is just an alias for this)
        df_all = df.unionAll(df_json)
        print('========\n*** UNION ALL')
        df_all.show(n=5)
        print('...')
        for row in df_all.tail(5):
            print(row)
        df_all.printSchema()
        print(f'UNION ALL results in {df_all.count()} records, {df_all.rdd.getNumPartitions()} partitions')
        # 2. unionByName
        df_union = df.unionByName(df_json)
        print('========\n*** UNION BY NAME')
        df_union.show(n=5)
        print('...')
        for row in df_union.tail(5):
            print(row)
        df_union.printSchema()
        print(f'UNION BY NAME results in {df.count()} records, {df_union.rdd.getNumPartitions()} partitions')

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
