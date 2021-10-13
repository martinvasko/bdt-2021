package at.ac.fhstp;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Approximate Pi!
 *
 */
public class App {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Approximate Pi").master("local[*]").getOrCreate();
        ApproximatePi app = new ApproximatePi();
        app.start(spark, 10);
        spark.stop();
    }
}
