package com.kang.srun

import org.apache.spark.ml.linalg.Vectors

/**
  * User: 
  * Description: 
  * Date: 2018-11-11
  * Time: 0:34
  */
object ScalaObjectRun {

  def objectRun(): Unit ={
    println("objectRun");
  }

  //scala call java
  def main(args: Array[String]): Unit = {
    /*var simple = new SimpleRun();
    simple.simpleRun();*/

   var dense = Vectors.dense(1.0,2.0,3.0);
    println(dense);
  }

}
