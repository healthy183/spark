package com.kang.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * User: 
  * Description: 
  * Date: 2018-11-11
  * Time: 1:00
  */
object SimpleSpark {

  protected var spark: SparkSession = null
  private val IP = "192.168.59.4"
  private val LOCAL_IP = "192.168.59.3"
  private val HDFS_IP = "192.168.59.4";

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.set("spark.driver.host",LOCAL_IP)
    spark = SparkSession
      .builder
      .appName("jsonDemo")
      .config(sparkConf)
      .master("spark://" + IP + ":7077")
      .getOrCreate();
    val JSON_PATH: String =
    "hdfs://" + HDFS_IP + ":9000/jsonDemoDir/simpleJson.json";
    var df = spark.read.option("multiline", "true")
        .json(JSON_PATH).cache();
    df.show();
  }

}
