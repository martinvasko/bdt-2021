package at.ac.fhstp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {
    // Adopt to your local setup: /Users/martin/Software/FHSTP/repos
    String pathCSVFile = "/Users/martin/Software/FHSTP/repos/bdt-2021/data/input/spark/Restaurants_in_Wake_County.csv";
    String pathJSONFile = "";

    IngestionSchemaManipulationApp cut;
    SparkSession spark;

    @Before
    public void tearUp() {
        spark = SparkSession.builder()
                .appName("Restaurants in Wake County, NC")
                .master("local")
                .getOrCreate();
        cut = new IngestionSchemaManipulationApp();
    }

    @After
    public void tearDown() {
        spark.stop();
    }

    @Test
    public void shouldIngestCSV() {
        Dataset<Row> df = cut.ingestCSV(spark, pathCSVFile);
        assertThat(df, is(notNullValue()));

        assertThat((int) df.count(), is(greaterThan(0)));
    }
}
