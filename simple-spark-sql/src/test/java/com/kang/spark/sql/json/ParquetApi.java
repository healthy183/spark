package com.kang.spark.sql.json;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Test;

/**
 * User:
 * Description:
 * Date: 2018-10-21
 * Time: 17:13
 */
@Deprecated
public class ParquetApi extends  BaseTest {

    private static  final String JSON_PATH
            = "hdfs://192.168.59.130:9000/jsonDemoDir/people.json";

    @Test //exception
    @Deprecated //Unable to infer schema for Parquet. It must be specified manually
    public void simpleRead(){
        Dataset<Row> peopleDF = spark.read().json(JSON_PATH);
        // DataFrames can be saved as Parquet files, maintaining the schema information
        peopleDF.write().parquet("people.parquet");
        //exception
        //Unable to infer schema for Parquet. It must be specified manually
        Dataset<Row> parquetFileDF = spark.read().format("parquet").load("people.parquet");
        //Dataset<Row> parquetFileDF = spark.read().parquet("people.parquet");

        // Parquet files can also be used to create a temporary view and then used in SQL statements
        parquetFileDF.createOrReplaceTempView("parquetFile");
        Dataset<Row> namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19");
        Dataset<String> namesDS = namesDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING());
        namesDS.show();
    }


}
