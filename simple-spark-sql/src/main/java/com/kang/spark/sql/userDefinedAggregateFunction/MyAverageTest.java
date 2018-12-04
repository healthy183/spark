package com.kang.spark.sql.userDefinedAggregateFunction;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * User:
 * Description:
 * Date: 2018-09-23
 * Time: 2:23
 */
public class MyAverageTest {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        String[] array = {"D:\\idea_workspace\\spark\\simple-spark-sql\\target\\spark-wordcount-java.jar"};
        sparkConf.setJars(array);
        sparkConf.set("spark.driver.host","192.168.59.3");
        sparkConf.set("spark.some.config.option", "some-value");

        SparkSession spark = SparkSession
                .builder()
                .appName("javaBeanTxtDataSet")
                .master("spark://192.168.59.130:7077")
                .config(sparkConf)
                .getOrCreate();
        spark.udf().register("myAverage", new MyAverage());
        Dataset<Row> df = spark.read().json("hdfs://192.168.59.130:9000/jsonDemoDir/employees.json");
        df.createOrReplaceTempView("employees");
        //df.show();
        Dataset<Row> result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees");
        result.show();
    }
}
