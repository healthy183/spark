package com.kang.spark.sql.jdbc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * User:
 * Description:
 * Date: 2018-09-24
 * Time: 14:54
 */
public class SimpleJDBC {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("jsonDemo")
                .config("spark.some.config.option", "some-value")
                .master("spark://192.168.59.130:7077")
                .config("spark.driver.host","192.168.59.3")
                .getOrCreate();

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:postgresql:dbserver")
                .option("dbtable", "schema.tablename")
                .option("user", "username")
                .option("password", "password")
                .load();

        java.util.Properties connectionProperties = new java.util.Properties();
        connectionProperties.put("user", "username");
        connectionProperties.put("password", "password");
        Dataset<Row> jdbcDF2 = spark.read()
                .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);

        //Saving data to a JDBC source
        jdbcDF.write()
                .format("jdbc")
                .option("url", "jdbc:postgresql:dbserver")
                .option("dbtable", "schema.tablename")
                .option("user", "username")
                .option("password", "password")
                .save();

        jdbcDF2.write()
                .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);

        // Specifying create table column data types on write
        jdbcDF.write()
                .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
                .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);



    }
}
