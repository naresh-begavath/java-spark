package com.java.spark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class DatasetTransformations implements InitSparkSession {

    public static Dataset<Row> CreateDataFrame() {
        //  Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/students_sample_data.csv");
        // dataset.show(true);
        //  return dataset;
        /* OR */
        return spark.read().option("header", true).csv("src/main/resources/students_sample_data.csv");
    }

    public static void BasicTransformations() {

        // Import dataset from 'CreateDataFrame' method
        Dataset<Row> dataset = DatasetTransformations.CreateDataFrame();

        // Count of the records
        long numOfRecords = dataset.count();
        System.out.println("There are " + numOfRecords + " records");

        Row firstRow = dataset.first();

        // Fetch first row
        String row = firstRow.toString();
        System.out.println("The first row is - " + row);

        // Fetch first row for a particular column using index
        String subject = firstRow.get(2).toString();
        System.out.println("The first row for 'subject' column by index is - " + subject);

        // Fetch first row for a particular column using value
        String  grade = firstRow.getAs("grade").toString();
        System.out.println("The first row for 'grade' column by value is - " + grade);

        /*
            'getAs'-> automatically infer the data type of the value, but if we are reading CSV file all are type 'String'.
            So we can use parser to convert data type
         */
        int year = Integer.parseInt(firstRow.getAs("year"));
        System.out.println("The first row for 'year' column by value is - " + year);
    }

    public static void FilterTransformations(){
        // Import dataset from 'CreateDataFrame' method
        Dataset<Row> dataset = DatasetTransformations.CreateDataFrame();
        dataset.show(true);

        // Filter transformations on dataframe - conditionExpr
        Dataset<Row> mathConditionExpr = dataset.filter("subject = 'Math' AND year >= 2007 ");
        mathConditionExpr.show(true);


        // Filter transformations on dataframe - ColumnExpr
        Column subjectColumn = dataset.col("subject");
        Column yearColumn = dataset.col("year");

        Dataset<Row> mathColumnExpr = dataset.filter(subjectColumn.equalTo("Math").and(yearColumn.geq(2007)));
        mathColumnExpr.show(true);

        // Filter transformations on dataframe - ColExpr
        Dataset<Row> mathColExpr = dataset.filter(col("subject").equalTo("Math").and(col("year").geq(2007)));
        mathColExpr.show(true);
    }

}
