package at.ac.fhstp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.expr;

public class TransformationAndActionApp {
    public void start(SparkSession spark, String source, String mode) {
        long t0 = System.currentTimeMillis();
        // Step 1 - Reads a CSV file with header, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load(source);
        Dataset<Row> initalDf = df;
        long t1 = System.currentTimeMillis();
        System.out.println("Loading initial dataset ...... " + (t1 - t0));

        // Step 2 - Build a bigger dataset
        for (int i = 0; i < 60; i++) {
            df = df.union(initalDf);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("Building full dataset ........ " + (t2 - t1));

        // Step 3 - Cleanup. preparation
        df = df.withColumnRenamed("Lower Confidence Limit", "lcl");
        df = df.withColumnRenamed("Upper Confidence Limit", "ucl");
        long t3 = System.currentTimeMillis();
        System.out.println("Clean-up ..................... " + (t3 - t2));

        // Step 4 - Transformation
        if (mode.compareToIgnoreCase("transform") == 0) {
            df = df
                    .withColumn("avg", expr("(lcl+ucl)/2"))
                    .withColumn("lcl2", df.col("lcl"))
                    .withColumn("ucl2", df.col("ucl"));
        }
            if (mode.compareToIgnoreCase("drop") == 0) {
                df = df
                    .withColumn("avg", expr("(lcl+ucl)/2"))
                    .withColumn("lcl2", df.col("lcl"))
                    .withColumn("ucl2", df.col("ucl"));
                df = df
                        .drop(df.col("avg"))
                        .drop(df.col("lcl2"))
                        .drop(df.col("ucl2"));
            }
        
        long t4 = System.currentTimeMillis();
        System.out.println("Transformations  ............. " + (t4 - t3));

        // Step 5 - Action
        df.collect();
        long t5 = System.currentTimeMillis();
        System.out.println("Final action ................. " + (t5 - t4));

        System.out.println("");
        System.out.println("# of records .................... " + df.count());
    }
}
