package com.kang.spark.sql.json;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;

/**
 * User:
 * Description:
 * Date: 2018-10-01
 * Time: 17:24
 */
public class BaseTest {
    protected SparkSession spark;
    private static final String IP  = "192.168.59.4";
    private static final String LOCAL_IP  = "192.168.59.3";

    @Before
    public void init(){
        SparkConf sparkConf = new SparkConf();
        String[] array = {"D:\\idea_workspace\\spark\\simple-spark-sql\\target\\simple-spark-sql.jar"};
        sparkConf.setJars(array);
        sparkConf.set("spark.driver.host",LOCAL_IP);
        spark = SparkSession
                .builder()
                .appName("jsonDemo")
                .config(sparkConf)
                .master("spark://"+IP+":7077")
                //.config("spark.driver.host",LOCAL_IP)
                .getOrCreate();
    }
}
