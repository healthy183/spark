package com.kang.hive

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * User: 
  * Description: 
  * Date: 2018-11-22
  * Time: 11:42
  */
object HiveExample {

  def getSparkClassLoader: ClassLoader = getClass.getClassLoader;

  def main(args: Array[String]): Unit = {

   /* org.apache.spark.util.Utils.classForName("org.apache.spark.sql.hive.HiveSessionStateBuilder");
    Utils.classForName("org.apache.hadoop.hive.conf.HiveConf");*/
    //val otherClassLoader = this.getClass.getClassLoader;
    //val classLoader = Thread.currentThread().getContextClassLoader;
    println();

    val target = Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader);
     //= null;
    Class.forName("org.apache.hadoop.hive.conf.HiveConf",
      true,target);


    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val IP = "192.168.59.4"
    val LOCAL_IP = "192.168.59.3"
    val sparkConf = new SparkConf
    val array = Array("D:\\idea_workspace\\spark\\scala-spark\\target\\scala-spark-1.0-SNAPSHOT.jar")
    //sparkConf.setJars(array)
    //sparkConf.set("spark.driver.host", LOCAL_IP)
    sparkConf.set("spark.sql.warehouse.dir", warehouseLocation) //Spark 2.x
    //sparkConf.set("spark.sql.catalogImplementation", "hive")
    //sparkConf.set(org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION.key,"hive");
    //sparkConf.set(CATALOG_IMPLEMENTATION.key, "hive");
    var spark = SparkSession.builder.appName("abc")
      .config(sparkConf).master("spark://" + IP + ":7077")
      .enableHiveSupport //.config("spark.driver.host",LOCAL_IP)
      .getOrCreate;

    spark.sql("show databases").show
    //spark.sql("use default");
    spark.sql("show tables").show

    //var hives = new HiveContext(spark:SparkSession);



  }

}
