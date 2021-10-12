package at.ac.fhstp;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Ingest the data!
 *
 */
public class App {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        System.out.println("Running Transform and Action");
        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }
       
        SparkSession spark = SparkSession.builder().appName("Transformation and Action").master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("OFF");
        TransformationAndActionApp app = new TransformationAndActionApp();
        app.start(spark, args[0], "transform");
        spark.stop();
    }
}
