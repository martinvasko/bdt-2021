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
    ApproximatePi cut;
    SparkSession spark;

    @Before
    public void tearUp() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        spark = SparkSession.builder().appName("Restaurants in Wake County, NC").master("local").getOrCreate();
        spark.sparkContext().setLogLevel("OFF");
        cut = new ApproximatePi();
    }

    @After
    public void tearDown() {
        spark.stop();
    }

    @Test
    public void shouldApproximatePi() {
        cut.start(spark, 10);
    }
}
