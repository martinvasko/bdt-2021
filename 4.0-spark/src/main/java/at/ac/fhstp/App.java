package at.ac.fhstp;

import org.apache.spark.sql.SparkSession;

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
