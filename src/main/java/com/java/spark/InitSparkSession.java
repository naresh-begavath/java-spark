package com.java.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public interface InitSparkSession {

    // Define SparkSession
    SparkSession spark = SparkSession
            .builder()
            .appName("Spark Java Project")
            .master("local[*]")
            .getOrCreate();

    // Define JavaSparkContext from SparkSession
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

}
