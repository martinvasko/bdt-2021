package at.ac.fhstp;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class UnionApp {
    public Dataset<Row> union(Dataset<Row> df1, Dataset<Row> df2) {
        Dataset<Row> df = df1.unionByName(df2);
        df.show(5);
        df.printSchema();
        System.out.println("We have " + df.count() + " records.");

        Partition[] partitions = df.rdd().partitions();
        int partitionCount = partitions.length;
        System.out.println("Partition count: " + partitionCount);

        return df;
    }
}
