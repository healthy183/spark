package com.kang.spark.sql.json;

import com.kang.spark.sql.dto.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Collections;

/**
 * User:
 * Description:
 * Date: 2018-09-03
 * Time: 1:55
 */
public class JavaBeanJsonDataSet {

    public static void main(String[] args) {
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);

        SparkConf sparkConf = new SparkConf();
        String[] array = {"D:\\idea_workspace\\spark\\simple-spark-sql\\target\\spark-wordcount-java.jar"};
        sparkConf.setJars(array);
        sparkConf.set("spark.driver.host","192.168.59.3");
        sparkConf.set("spark.some.config.option", "some-value");

        SparkSession spark = SparkSession
                .builder()
                .appName("jsonDemo")
                .master("spark://192.168.59.130:7077")
                .config(sparkConf)
                .getOrCreate();

        Encoder<Person> personEncoder  = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person), personEncoder
        );
        javaBeanDS.show();

        //exception
       /*
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map(
                (MapFunction<Integer, Integer>) value -> value + 1,
                integerEncoder);
        transformedDS.collect();*/
        String JSON_PATH = "hdfs://192.168.59.130:9000/jsonDemoDir/simpleJson.json";
        Dataset<Person> peopleDS = spark.read().option("multiline","true")
                .json(JSON_PATH).as(personEncoder);
        /*java.io.InvalidClassException: com.kang.spark.sql.dto.Person; local class incompatible
         stream classdesc serialVersionUID = -5871880941802444567,
          local class serialVersionUID = -6698938698464120046*/
        /*peopleDS.foreach((p)->{
            System.out.println(p.toString());
        });*/
        peopleDS.show();
    }
}
