package com.kang.base

import com.kang.spark.SimpleSpark.LOCAL_IP
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * User: 
  * Description: 
  * Date: 2018-11-11
  * Time: 12:08
  */
object SparkBase {

  private val IP = "192.168.59.4";
  private val LOCAL_IP = "192.168.59.3";
  private var spark: SparkSession = null;

  def getSparkSession():SparkSession = {
    if(spark != null){
      return spark;
    }
    val sparkConf = new SparkConf();
    /*sparkConf.set("spark.driver.host", "192.168.59.3");
    val array = Array("D:\\idea_workspace\\spark\\simple-spark-sql\\target\\spark-wordcount-java.jar");
    sparkConf.setJars(array);
    sparkConf.set("spark.some.config.option", "some-value");*/
    spark = SparkSession
      .builder
      .appName("jsonDemo")
      .config(sparkConf)
      .master("spark://" + IP + ":7077")
      .config("spark.driver.host", LOCAL_IP)
      .getOrCreate();
    return spark;
  }



}
