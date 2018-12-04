package com.kang.ml.pipeline

import com.kang.base.SparkBase
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap
;
/**
  * User: 
  * Description: 
  * Date: 2018-11-11
  * Time: 12:32
  */
object SimplePipeline {

  def simple():Unit ={
    var sparkSession = SparkBase.getSparkSession();
    /*var seq = Seq(
      (1.0,"[0.0,1.1,0.1]"),
      (0.0,"[2.0,1.0,-1.0]"),
      (0.0,"[2.0,1.3,1.0]"),
      (1.0,"[0.0,1.2,-0.5]"));*/

    var seq = Seq(
      (1.0,"[0.0,1.1,0.1]"),
      (0.0,"[2.0,1.0,-1.0]"),
      (0.0,"[2.0,1.3,1.0]"),
      (1.0,"[0.0,1.2,-0.5]"));

    var training = sparkSession
      .createDataFrame(seq)
      .toDF("label","features");
    //seqDF.show();
    var lr = new LogisticRegression();
    lr.setMaxIter(10).setRegParam(0.01);

    var modell = lr.fit(training);

    val paramMap = ParamMap(lr.maxIter -> 20)
      .put(lr.maxIter,30)
      .put(lr.regParam -> 0.1, lr.threshold -> 0.55);

    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability");

    val paramMapCombined = paramMap ++ paramMap2;

    val model2 = lr.fit(training,paramMapCombined);

    var seqTest = Seq(
      (1.0,"[-1.0,1.5,1.3]"),
      (0.0,"[3.0,2.0,-0.1]"),
      (1.0,"[0.0,2.2,-1.5]"));

    var trainingTest = sparkSession
      .createDataFrame(seqTest)
      .toDF("label","features");

    var result = model2.transform(trainingTest)
      .select("features","label","myProbability","prediction")
      .collect();
    println(result);

      /*.foreach{case Row(features:Vector,label:Double,prod:Vector,prediction:Double)=>
        println(s"($features,$label) -> prod=$prod,prediction=$prediction");}*/
  }

  def main(args: Array[String]): Unit = {
    simple();
  }

}
