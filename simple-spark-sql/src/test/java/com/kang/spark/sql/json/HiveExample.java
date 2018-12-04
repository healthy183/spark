/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kang.spark.sql.json;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Deprecated
public class HiveExample extends BaseTest {

  // $example on:spark_hive$
  public static class Record implements Serializable {
    private int key;
    private String value;

    public int getKey() {
      return key;
    }

    public void setKey(int key) {
      this.key = key;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }
  }
  // $example off:spark_hive$

  @Test
  public void hiveRun(){
    // $example on:spark_hive$
    // warehouseLocation points to the default location for managed databases and tables
    String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
    String IP  = "192.168.59.4";
    String LOCAL_IP  = "192.168.59.3";
    SparkConf sparkConf = new SparkConf();
    String[] array = {"D:\\idea_workspace\\spark\\simple-spark-sql\\target\\simple-spark-sql.jar"};
    sparkConf.setJars(array);
    //sparkConf.set("spark.driver.host",LOCAL_IP);
    sparkConf.set("spark.sql.warehouse.dir", warehouseLocation); //Spark 2.x
    sparkConf.set("spark.sql.catalogImplementation","hive");
    //sparkConf.set(org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION.key,"hive");
    //sparkConf.set(CATALOG_IMPLEMENTATION.key, "hive");
    spark = SparkSession
            .builder()
            .appName("abc")
            .config(sparkConf)
            .master("spark://"+IP+":7077")
            //.config("spark.driver.host",LOCAL_IP)
            .enableHiveSupport()
            .getOrCreate();


   /* spark.sql("use default");
    spark.sql("select student.Sname,course.Cname,sc.Grade from student join sc " +
            "on student.Sno=sc.Sno " +
            "join course on sc.cno=course.cno").show();*/
    //spark.sql("use hive");
    //spark.sql(" select * from hive.AUX_TABLE").show();
    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive");
    spark.sql("show databases").show();
    //spark.sql("use default");
    spark.sql("show tables").show();

  /*  HiveContext hiveContext = new HiveContext(spark);
    hiveContext.sql("show tables").show();;*/
    spark.sql("SELECT * FROM pokes").show();

    //spark.sql("CREATE TABLE IF NOT EXISTS frist_table (key INT, value STRING)");
    spark.sql("CREATE TABLE frist_table (foo INT,bar STRING)");

    spark.sql("LOAD DATA LOCAL INPATH 'D:\\idea_workspace\\spark\\simple-spark-sql\\src\\main\\resources\\kv1.txt' INTO TABLE frist_table");

    // Queries are expressed in HiveQL
    spark.sql("SELECT * FROM src").show();
    // +---+-------+
    // |key|  value|
    // +---+-------+
    // |238|val_238|
    // | 86| val_86|
    // |311|val_311|
    // ...

    // Aggregation queries are also supported.
    spark.sql("SELECT COUNT(*) FROM src").show();
    // +--------+
    // |count(1)|
    // +--------+
    // |    500 |
    // +--------+

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key");

    // The items in DataFrames are of type Row, which lets you to access each column by ordinal.
    Dataset<String> stringsDS = sqlDF.map(
        (MapFunction<Row, String>) row -> "Key: " + row.get(0) + ", Value: " + row.get(1),
        Encoders.STRING());
    stringsDS.show();
    // +--------------------+
    // |               value|
    // +--------------------+
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // ...

    // You can also use DataFrames to create temporary views within a SparkSession.
    List<Record> records = new ArrayList<>();
    for (int key = 1; key < 100; key++) {
      Record record = new Record();
      record.setKey(key);
      record.setValue("val_" + key);
      records.add(record);
    }
    Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
    recordsDF.createOrReplaceTempView("records");

    // Queries can then join DataFrames data with data stored in Hive.
    spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show();
    // +---+------+---+------+
    // |key| value|key| value|
    // +---+------+---+------+
    // |  2| val_2|  2| val_2|
    // |  2| val_2|  2| val_2|
    // |  4| val_4|  4| val_4|
    // ...
    // $example off:spark_hive$

    spark.stop();
  }
}
