package com.kang.spark.sql.json;

import com.kang.spark.sql.userDefinedAggregateFunction.MyAverage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * User:
 * Description:
 * Date: 2018-10-13
 * Time: 21:21
 */
public class UdfTest extends  BaseTest {

    private Dataset<Row> studentDataset;

    @Before
    public void initDataSet(){
        List<String> studentStr = Arrays.asList(
                "[" +
                        "{\"id\":\"1\",\"name\":\"Yin\",\"clazzid\":\"123\",\"scope\":\"98\",\"Date\":\"2016-01-07 05:30:00.0\"}," +
                        "{\"id\":\"2\",\"name\":\"gao\",\"clazzid\":\"123\",\"scope\":\"95\",\"Date\":\"2016-01-17 05:30:00.0\"}," +
                        "{\"id\":\"3\",\"name\":\"ge\", \"clazzid\":\"124\",\"scope\":\"71\",\"Date\":\"2016-01-27 05:30:00.0\"}," +
                        "{\"id\":\"4\",\"name\":\"ge\", \"clazzid\":\"125\",\"scope\":\"88\",\"Date\":\"2016-02-07 05:30:00.0\"}," +
                        "{\"id\":\"5\",\"name\":\"guo\",\"clazzid\":\"124\",\"scope\":\"78\",\"Date\":\"2016-02-07 05:30:00.0\"}," +
                        "{\"id\":\"6\",\"name\":\"tom\",\"clazzid\":\"124\",\"scope\":\"78\",\"Date\":\"2015-01-07 05:30:00.0\"} " +
                        "]");
        Dataset<String> studentStrDataset = spark.createDataset(studentStr, Encoders.STRING());
        studentDataset = spark.read().json(studentStrDataset);
        studentDataset.show();

    }

    /*@Test
    public void simple(){
        UserDefinedFunction addByCurryRegister =
                spark.udf().register("addByCurryRegister", addVal(1));
        studentDataset.select(addByCurryRegister(studentDataset.col("name"))).show();
    }
    private UserDefinedFunction addVal(Integer i){
         new UserDefinedFunction((int i)->{
         })
        return null;
    }*/

    @Test
    public void binDemo(){
        spark.udf().register("myAverage", new MyAverage());
        Dataset<Row> df = spark.read().json("hdfs://192.168.59.130:9000/jsonDemoDir/employees.json");
        df.createOrReplaceTempView("employees");
        //df.show();
        Dataset<Row> result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees");
        result.show();
    }
}

