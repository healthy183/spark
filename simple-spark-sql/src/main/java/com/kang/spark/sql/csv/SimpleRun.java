package com.kang.spark.sql.csv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * User:
 * Description:
 * Date: 2018-09-24
 * Time: 17:02
 */
public class SimpleRun {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("jsonDemo")
                .config("spark.some.config.option", "some-value")
                .master("spark://192.168.59.130:7077")
                .config("spark.driver.host","192.168.59.3")
                .getOrCreate();

        Dataset<Row> peopleDFCsv = spark.read().format("csv")
                .option("sep", ";")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("hdfs://192.168.59.130:9000/csv/people.csv");
        peopleDFCsv.show();
    }
}
