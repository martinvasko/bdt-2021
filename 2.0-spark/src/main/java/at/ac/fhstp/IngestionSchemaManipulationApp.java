package at.ac.fhstp;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class IngestionSchemaManipulationApp {


    public Dataset ingestJSON(String source) {
        return null;
    }

    public Dataset ingestCSV(SparkSession spark, String source) {

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load(source);  
        
        df.show(5);
        df.printSchema();

        System.out.println("We have " + df.count() + " records.");

        System.out.println("**** Transforming the data ****");
        df = df.withColumn("county", lit("Wake"))
                .withColumnRenamed("HSISID", "datasetId")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address2")
                .withColumnRenamed("CITY", "city")
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY")
                .drop("OBJECTID")
                .drop("PERMITID")
                .drop("GEOCODESTATUS");
        df.show(5);
        df.printSchema();

        df = df.withColumn("id", concat(
                df.col("state"),
                lit("_"),
                df.col("county"), lit("_"),
                df.col("datasetId")));

        // Shows at most 5 rows from the dataframe
        System.out.println("*** Dataframe transformed");
        df.show(5);

        System.out.println("*** Looking at partitions");
        Partition[] partitions = df.rdd().partitions();
        int partitionCount = partitions.length;
        System.out.println("Partition count before repartition: " +
                partitionCount);

        df = df.repartition(4);
        System.out.println("Partition count after repartition: " +
                df.rdd().partitions().length);

        return df;
    }
}