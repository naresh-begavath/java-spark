package com.java.spark.code_challanges;

import com.java.spark.InitSparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static java.util.Arrays.asList;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class SalesPerProduct implements InitSparkSession {
    public static Dataset<Row> loadSalesData() {
        StructType schema = new StructType()
                .add("transaction_id", DataTypes.IntegerType, false)
                .add("product_id", DataTypes.StringType, false)
                .add("product_name", DataTypes.StringType, false)
                .add("quantity", DataTypes.IntegerType, false)
                .add("price_per_unit", DataTypes.IntegerType, false);

        Row[] data = new Row[]{
                RowFactory.create(1, "P001", "Laptop", 2, 600),
                RowFactory.create(2, "P002", "Phone", 3, 300),
                RowFactory.create(3, "P001", "Laptop", 1, 600),
                RowFactory.create(4, "P003", "Tablet", 5, 150),
                RowFactory.create(5, "P002", "Phone", 1, 300)
        };

        return spark.createDataFrame(asList(data), schema);
    }

    public static Dataset<Row> computeTotalSalesPerProduct(Dataset<Row> df) {
        return df.withColumn("total_sales",
                        col("quantity").multiply(col("price_per_unit")))
                .groupBy("product_id", "product_name")
                .agg(sum("total_sales").alias("total_sales"));
    }

    public static Dataset<Row> filterHighSalesProducts(Dataset<Row> df, int threshold) {
        return df.filter(col("total_sales").gt(threshold));
    }

    public static void process() {
        Dataset<Row> salesDF = loadSalesData();
        Dataset<Row> summaryDF = computeTotalSalesPerProduct(salesDF);
        Dataset<Row> filteredDF = filterHighSalesProducts(summaryDF, 1000);

        filteredDF.show(false);
    }

}
