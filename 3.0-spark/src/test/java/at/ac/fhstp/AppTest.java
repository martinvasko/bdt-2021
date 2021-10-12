package at.ac.fhstp;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Unit test for simple App.
 */
public class AppTest {
    // Adopt to your local setup: /Users/martin/Software/FHSTP/repos
    String pathCSVFile = "/Users/martin/Software/FHSTP/repos/bdt-2021/data/input/spark/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv";

    TransformationAndActionApp cut;
    SparkSession spark;

    @Before
    public void tearUp() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        spark = SparkSession.builder().appName("Tranform and Action").master("local").getOrCreate();
        spark.sparkContext().setLogLevel("OFF");
        cut = new TransformationAndActionApp();
    }

    @After
    public void tearDown() {
        spark.stop();
    }

    @Test
    public void shouldTransform() {
        cut.start(spark, pathCSVFile, "transform");
    }

    @Test
    public void shouldDrop() {
        cut.start(spark, pathCSVFile, "drop");
    }
}
