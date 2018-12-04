package com.kang.spark.sql.json;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * User:
 *
 *json串转Dataset
 * Description:
 * Date: 2018-09-24
 * Time: 14:37
 */
public class JsonStr {

    public static void main(String[] args) {
        String  HDFS_IP ="192.168.59.130";

        SparkSession spark = SparkSession
                .builder()
                .appName("jsonDemo")
                .config("spark.some.config.option", "some-value")
                .master("spark://192.168.59.130:7077")
                .config("spark.driver.host","192.168.59.3")
                .getOrCreate();

        List<String> jsonData = Arrays.asList(
                "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
        Dataset<String> anotherPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
        Dataset<Row> anotherPeople = spark.read().json(anotherPeopleDataset);
        anotherPeople.show();
        //AccessControlException: Permission denied: user=healthy,
        //access=WRITE, inode="/jsonDemoDir":root:supergroup:drwxr-xr-x
        String JSON_PATH =
                "hdfs://"+HDFS_IP+":9000/jsonDemoDir/saveJson.json";
        anotherPeople.select(anotherPeople.col("name")).write().format("json").save(JSON_PATH);
    }
}
