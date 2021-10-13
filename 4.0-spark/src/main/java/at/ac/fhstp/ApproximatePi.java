package at.ac.fhstp;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ApproximatePi implements Serializable {
  private static final long serialVersionUID = -1546L;
  private static long counter = 0;

  private final class DartMapper implements MapFunction<Row, Integer> {
    private static final long serialVersionUID = 38446L;

    @Override
    public Integer call(Row r) throws Exception {
      double x = Math.random() * 2 - 1;
      double y = Math.random() * 2 - 1;
      counter++;
      if (counter % 100000 == 0) {
        System.out.println("" + counter + " darts thrown so far");
      }
      return (x * x + y * y <= 1) ? 1 : 0;
    }
  }

  private final class DartReducer implements ReduceFunction<Integer> {
    private static final long serialVersionUID = 12859L;

    @Override
    public Integer call(Integer x, Integer y) {
      return x + y;
    }
  }

  public void start(SparkSession spark, int slices) {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    long t0 = System.currentTimeMillis();
    int numberOfThrows = 100000 * slices;
    System.out.println("About to throw " + numberOfThrows + " darts, ready? Stay away from the target!");
    List<Integer> listOfThrows = new ArrayList<>(numberOfThrows);
    for (int i = 0; i < numberOfThrows; i++) {
      listOfThrows.add(i);
    }
    Dataset<Row> incrementalDf = spark.createDataset(listOfThrows, Encoders.INT()).toDF();
    long t1 = System.currentTimeMillis();
    System.out.println("Initial dataframe built in " + (t1 - t0) + " ms");

    Dataset<Integer> dartsDs = incrementalDf.map(new DartMapper(), Encoders.INT());

    long t2 = System.currentTimeMillis();
    System.out.println("Throwing darts done in " + (t2 - t1) + " ms");

    int dartsInCircle = dartsDs.reduce(new DartReducer());
    long t3 = System.currentTimeMillis();
    System.out.println("Analyzing result in " + (t3 - t2) + " ms");

    System.out.println("Pi is roughly " + 4.0 * dartsInCircle / numberOfThrows);
  }
}
