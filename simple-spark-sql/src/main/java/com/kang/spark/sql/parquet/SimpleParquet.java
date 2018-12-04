package com.kang.spark.sql.parquet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * User:
 * Description:
 * Date: 2018-09-24
 * Time: 1:33
 */
public class SimpleParquet {

    private static  final String PARQUET_PATH = "hdfs://192.168.59.130:9000/parquets/users.parquet";

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("jsonDemo")
                .config("spark.some.config.option", "some-value")
                .master("spark://192.168.59.130:7077")
                .config("spark.driver.host","192.168.59.3")
                .getOrCreate();
        Dataset<Row> usersDF = spark.read().load(PARQUET_PATH);
        //Exception in thread "main" org.apache.hadoop.security.AccessControlException:
        //Permission denied: user=healthy, access=WRITE, inode="/parquets":root:supergroup:drwxr-xr-x
        //usersDF.select("name", "favorite_color").write().save("hdfs://192.168.59.130:9000/parquets/namesAndFavColors.parquet");
        //保存到项目根路径
        /*usersDF.select("name", "favorite_color").write().save("namesAndFavColors.parquet");
        usersDF.write().partitionBy("favorite_color")
                .format("parquet").save("namesPartByColor.parquet");*/
        usersDF.show();

        Dataset<Row> sqlDF =
                spark.sql("SELECT * FROM parquet.`"+PARQUET_PATH+"`");
        //sqlDF.show();


        Dataset<Row> peopleDF =
                spark.read().format("json").load("hdfs://192.168.59.130:9000/jsonDemoDir/people.json");
        //peopleDF.select("name", "age").write().format("parquet").save("namesAndAges.parquet");

        peopleDF.write().bucketBy(42, "name")
                .sortBy("age").saveAsTable("people_bucketed");

        Dataset<Row> bucketedDF = spark.sql("SELECT * FROM people_bucketed");
        //bucketedDF.show();
    }
}
