package com.java.spark;

import com.java.spark.code_challanges.SalesPerProduct;

public class Main {
    public static void main(String[] args) {

        RDDTransformations.RDDReduce();

        DatasetTransformations.BasicTransformations();
        DatasetTransformations.FilterTransformations();

        SalesPerProduct.process();
    }
}
