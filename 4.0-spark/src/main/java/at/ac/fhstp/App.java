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
        System.out.println("Running Ingestion");
        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }
        SparkSession spark = SparkSession.builder().appName("Approximate Pi").master("local[*]")
                .getOrCreate();
       ApproximatePi app = new ApproximatePi();
        spark.stop();
    }
}
