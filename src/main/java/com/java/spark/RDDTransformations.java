package com.java.spark;

import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;

public class RDDTransformations implements InitSparkSession {

    public static void RDDReduce() {
        // Define an empty list type double
        List<Double> inputData = new ArrayList<>();

        // Add values to the list
        inputData.add(35.5);
        inputData.add(12.49943);
        inputData.add(90.32);
        inputData.add(20.32);

        // Create an RDD
        JavaRDD<Double> myRdd = sc.parallelize(inputData);

        // Perform transformation on RDD
        Double result = myRdd.reduce((x, y) -> x + y);
        System.out.println(result);

    }

}
