package com.kang.spark.sql.txt;

import com.kang.spark.sql.dto.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * User:
 * Description:
 * Date: 2018-09-03
 * Time: 1:55
 */
//Interoperating with RDDs
public class JavaBeanTxtDataSet {

    private static final String TXT_PATH = "hdfs://192.168.59.130:9000/mydemo/people.txt";

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
        rddMap(spark);
        structField(spark);
        }
    /**
     *
     * @param spark
     */
    private static void rddMap(SparkSession spark) {
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile(TXT_PATH)
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Person sparkPerson = new Person();
                    sparkPerson.setName(parts[0]);
                    sparkPerson.setAge(Integer.parseInt(parts[1].trim()));
                    return sparkPerson;
                });
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
        peopleDF.createOrReplaceTempView("people");
        Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                stringEncoder);
        teenagerNamesByIndexDF.show();
        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
                stringEncoder);
        teenagerNamesByFieldDF.show();
    }

    private static void structField(SparkSession spark) {
        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile(TXT_PATH, 1).toJavaRDD();
        //The schema is encoded in a string
        String schemaString = "name age";
        //Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);
        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });

        // Apply the schema to the RDD
        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);
        // Creates a temporary view using the DataFrame
        peopleDataFrame.createOrReplaceTempView("people");

        // SQL can be run over a temporary view created using DataFrames
        Dataset<Row> results = spark.sql("SELECT name,age FROM people");
        results.show();
        // The results of SQL queries are DataFrames and support all the normal RDD operations
        // The columns of a row in the result can be accessed by field index or by field name
        Dataset<String> namesDS = results.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING());
        namesDS.show();
    }
}
