package at.ac.fhstp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class UnionAppTest {
    // Adopt to your local setup: /Users/martin/Software/FHSTP/repos
    String pathCSVFile = "/Users/martin/Software/FHSTP/repos/bdt-2021/data/input/spark/Restaurants_in_Wake_County.csv";
    String pathJSONFile = "/Users/martin/Software/FHSTP/repos/bdt-2021/data/input/spark/Restaurants_in_Durham_County_NC.json";

    UnionApp cut;
    IngestionSchemaManipulationApp ingest;
    Dataset<Row> wakeDf;
    Dataset<Row> durhamDf;
    SparkSession spark;

    @Before
    public void tearUp() {
        spark = SparkSession.builder().appName("Union Test App").master("local").getOrCreate();
        ingest = new IngestionSchemaManipulationApp();
        wakeDf = ingest.ingestCSV(spark, pathCSVFile);
        durhamDf = ingest.ingestJSON(spark, pathJSONFile);
        cut = new UnionApp();
    }

    @After
    public void tearDown() {
        spark.stop();
    }

    @Test
    public void shouldUnionDFs() {
        Dataset<Row> df = cut.union(wakeDf, durhamDf);
        assertThat(df, is(notNullValue()));
    }

    @Test
    public void shouldUnionAll() {
        Dataset<Row> df = cut.union(durhamDf, wakeDf);
        assertThat((int) df.count(), is(5668));
    }

}
