package com.kang.spark.sql.json;

import com.kang.spark.sql.dto.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * User:
 * Description:
 * Date: 2018-10-21
 * Time: 15:01
 */
@Deprecated
public class JavaRDDApi {

    private SparkSession spark;
    private static final String IP  = "192.168.59.130";
    private static final String LOCAL_IP  = "192.168.59.3";
    private static final String TXT_PATH = "hdfs://192.168.59.130:9000/mydemo/people.txt";

    /**
     *setJars单元测试貌似不行，只能通过main方法运行
     * 看 JavaBeanTxtDataSet 吧,这里运行不知道为什么异常。。。
     */
    @Deprecated
    public void init(){
        SparkConf sparkConf = new SparkConf();
        String[] array = {"D:\\idea_workspace\\spark\\simple-spark-sql\\target\\spark-wordcount-java.jar"};
        sparkConf.setJars(array);
        sparkConf.set("spark.driver.host","192.168.59.3");
        sparkConf.set("spark.some.config.option", "some-value");
        spark = SparkSession
                .builder()
                .appName("javaBeanTxtDataSet")
                .master("spark://192.168.59.130:7077")
                .config(sparkConf)
                .getOrCreate();
    }

}
