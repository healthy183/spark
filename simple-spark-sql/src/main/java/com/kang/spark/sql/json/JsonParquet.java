package com.kang.spark.sql.json;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * json 转 parquet
 * User:
 * Description:
 * Date: 2018-09-24
 * Time: 2:13
 */
public class JsonParquet {

    private static  final String JSON_PATH
            = "hdfs://192.168.59.130:9000/jsonDemoDir/people.json";

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("jsonDemo")
                .config("spark.some.config.option", "some-value")
                .master("spark://192.168.59.130:7077")
                .config("spark.driver.host","192.168.59.3")
                .getOrCreate();

        Dataset<Row> peopleDF = spark.read()
                .option("multiline","true").json(JSON_PATH)
                .cache();

        //这里写了保存在本地
        peopleDF.write().parquet("people.parquet");
        //这里又读不到本地本地文件
        Dataset<Row> parquetFileDF = spark.read().option
                ("multiline","true").parquet("people.parquet");

        parquetFileDF.createOrReplaceTempView("parquetFile");
        Dataset<Row> namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19");
        Dataset<String> namesDS = namesDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING());
        namesDS.show();
        //Partition Discovery
    }
}
