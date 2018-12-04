package com.kang.spark.sql.json;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.DenseRank;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * User:
 * Description: 聚合函数
 * Date: 2018-10-04
 * Time: 16:07
 */
public class DataSetWindowFuntion extends BaseTest {

    private Dataset<Row> studentDataset;

    private  WindowSpec partitionWindow;

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

        partitionWindow = Window.partitionBy
                (studentDataset.col("clazzid"))
                .orderBy(studentDataset.col("scope").desc());
    }

    @Test
    public void fifterTest(){
        studentDataset.filter("year(Date)==2016").show();
        /*
        |Date|clazzid| id|name|scope|
        +--------------------+-------+---+----+-----+
        |2016-01-07 05:30:...|    123|  1| Yin|   98|
        |2016-01-17 05:30:...|    123|  2| gao|   95|
        |2016-01-27 05:30:...|    124|  3|  ge|   71|
        |2016-02-07 05:30:...|    125|  4|  ge|   88|
        +--------------------+-------+---+----+-----+*/
    }


    @Test
    public void avgTest() {
        studentDataset
                .groupBy(window(studentDataset.col("Date"),"1 week"))
                .agg(avg("scope").as("avgScope")).show();
        /*
        +--------------------+--------+
        |window|avgScope|
        +--------------------+--------+
        |[2016-01-21 08:00...|    71.0|
        |[2016-01-14 08:00...|    95.0|
        |[2016-02-04 08:00...|    88.0|
        |[2015-01-01 08:00...|    78.0|
        |[2015-12-31 08:00...|    98.0|
        +--------------------+--------+
        * */
        /* cannot resolve '`window.end`' given input columns: [id, Date, name, clazzid, scope];;'Project [name#7, 'window.end, 'avgScope]
       studentDataset.sort("name").
                select("name","window.end","avgScope").
                show(false);*/
    }

    @Test
    public void partitionByRankTest(){
        //sql 用clazzid来分区,然后scope排序做窗口RANK()查询
        /*SELECT id,name,clazzid,scope,Date,RANK() OVER(partition by clazzid ORDER BY scope asc) as rank FROM emp;
         */
        Column over = rank().over(partitionWindow);
        studentDataset.select(
                studentDataset.col("id"),
                studentDataset.col("name"),
                studentDataset.col("clazzid"),
                studentDataset.col("scope"),
                studentDataset.col("Date"),
                over.as("rank")).show();
        /*
        +---+----+-------+-----+--------------------+----+
        | id|name|clazzid|scope|                Date|rank|
        +---+----+-------+-----+--------------------+----+
        |  4|  ge|    125|   88|2016-02-07 05:30:...|   1|
        |  5| guo|    124|   78|2016-02-07 05:30:...|   1|
        |  6| tom|    124|   78|2015-01-07 05:30:...|   1|
        |  3|  ge|    124|   71|2016-01-27 05:30:...|   3|
        |  1| Yin|    123|   98|2016-01-07 05:30:...|   1|
        |  2| gao|    123|   95|2016-01-17 05:30:...|   2|
        +---+----+-------+-----+--------------------+----+
        * */
    }

    @Test
    public void denseRank(){
        //sql
        /*
         用clazzid来分区,然后scope排序做窗口DENSE_RANK()查询
        /*SELECT id,name,clazzid,scope,Date,DENSE_RANK() OVER(partition by clazzid ORDER BY scope asc) as rank FROM emp;
         */
        Column over = dense_rank().over(partitionWindow);
        studentDataset.select(
                studentDataset.col("id"),
                studentDataset.col("name"),
                studentDataset.col("clazzid"),
                studentDataset.col("scope"),
                studentDataset.col("Date"),
                over.as("rank")).show();
        /*
        +---+----+-------+-----+--------------------+----+
        | id|name|clazzid|scope|                Date|rank|
        +---+----+-------+-----+--------------------+----+
        |  4|  ge|    125|   88|2016-02-07 05:30:...|   1|
        |  5| guo|    124|   78|2016-02-07 05:30:...|   1|
        |  6| tom|    124|   78|2015-01-07 05:30:...|   1|
        |  3|  ge|    124|   71|2016-01-27 05:30:...|   2|
        |  1| Yin|    123|   98|2016-01-07 05:30:...|   1|
        |  2| gao|    123|   95|2016-01-17 05:30:...|   2|
        +---+----+-------+-----+--------------------+----+
        * */
    }


    @Test //percent_rank不知道有什么用
    public void percentRank(){
        //sql
        /*
         用clazzid来分区,然后scope排序做窗口DENSE_RANK()查询
        /*SELECT id,name,clazzid,scope,Date,percent_rank() OVER(partition by clazzid ORDER BY scope asc) as rank FROM emp;
         */
        Column over = percent_rank().over(partitionWindow);
        studentDataset.select(
                studentDataset.col("id"),
                studentDataset.col("name"),
                studentDataset.col("clazzid"),
                studentDataset.col("scope"),
                studentDataset.col("Date"),
                over.as("percentRank")).show();
        /*
       +---+----+-------+-----+--------------------+-----------+
        | id|name|clazzid|scope|                Date|percentRank|
        +---+----+-------+-----+--------------------+-----------+
        |  4|  ge|    125|   88|2016-02-07 05:30:...|        0.0|
        |  5| guo|    124|   78|2016-02-07 05:30:...|        0.0|
        |  6| tom|    124|   78|2015-01-07 05:30:...|        0.0|
        |  3|  ge|    124|   71|2016-01-27 05:30:...|        1.0|
        |  1| Yin|    123|   98|2016-01-07 05:30:...|        0.0|
        |  2| gao|    123|   95|2016-01-17 05:30:...|        1.0|
        +---+----+-------+-----+--------------------+-----------+
        * */
    }


    @Test //查询伪列
    public void rowNumber(){
        /*SELECT id,name,clazzid,scope,Date,row_number() OVER(partition by clazzid ORDER BY scope asc) as rank FROM emp;
         */
        Column over = row_number().over(partitionWindow);
        studentDataset.select(
                studentDataset.col("id"),
                studentDataset.col("name"),
                studentDataset.col("clazzid"),
                studentDataset.col("scope"),
                studentDataset.col("Date"),
                over.as("rowNumber")).show();
        /*
        +---+----+-------+-----+--------------------+---------+
        | id|name|clazzid|scope|                Date|rowNumber|
        +---+----+-------+-----+--------------------+---------+
        |  4|  ge|    125|   88|2016-02-07 05:30:...|        1|
        |  5| guo|    124|   78|2016-02-07 05:30:...|        1|
        |  6| tom|    124|   78|2015-01-07 05:30:...|        2|
        |  3|  ge|    124|   71|2016-01-27 05:30:...|        3|
        |  1| Yin|    123|   98|2016-01-07 05:30:...|        1|
        |  2| gao|    123|   95|2016-01-17 05:30:...|        2|
        +---+----+-------+-----+--------------------+---------+
        */
    }
    @Test //查询总和
    public void sumTest(){
        /*SELECT id,name,clazzid,scope,Date,sum(scope) OVER(partition by clazzid ORDER BY scope asc) as rank FROM emp;
         */
        Column over = sum(studentDataset.col("scope")).over(partitionWindow);
        studentDataset.select(
                studentDataset.col("id"),
                studentDataset.col("name"),
                studentDataset.col("clazzid"),
                studentDataset.col("scope"),
                studentDataset.col("Date"),
                over.as("sum(scope)")).show();
        /*
        +---+----+-------+-----+--------------------+----------+
        | id|name|clazzid|scope|                Date|sum(scope)|
        +---+----+-------+-----+--------------------+----------+
        |  4|  ge|    125|   88|2016-02-07 05:30:...|      88.0|
        |  5| guo|    124|   78|2016-02-07 05:30:...|     156.0|
        |  6| tom|    124|   78|2015-01-07 05:30:...|     156.0|
        |  3|  ge|    124|   71|2016-01-27 05:30:...|     227.0|
        |  1| Yin|    123|   98|2016-01-07 05:30:...|      98.0|
        |  2| gao|    123|   95|2016-01-17 05:30:...|     193.0|
        +---+----+-------+-----+--------------------+----------+
        * */
    }

    @Test //lead(字段)下一列的值
    public void leadTest(){
        //SELECT id,name,clazzid,scope,Date,lead(sal) OVER (partition by clazzid ORDER BY scope desc) as next_val FROM emp
        Column over = lead(studentDataset.col("scope"),1,0).over(partitionWindow);
        studentDataset.select(
                studentDataset.col("id"),
                studentDataset.col("name"),
                studentDataset.col("clazzid"),
                studentDataset.col("scope"),
                studentDataset.col("Date"),
                over.as("next_val")).show();
        /*
        +---+----+-------+-----+--------------------+----------+
        | id|name|clazzid|scope|                Date|sum(scope)|
        +---+----+-------+-----+--------------------+----------+
        |  4|  ge|    125|   88|2016-02-07 05:30:...|         0|
        |  5| guo|    124|   78|2016-02-07 05:30:...|        78|
        |  6| tom|    124|   78|2015-01-07 05:30:...|        71|
        |  3|  ge|    124|   71|2016-01-27 05:30:...|         0|
        |  1| Yin|    123|   98|2016-01-07 05:30:...|        95|
        |  2| gao|    123|   95|2016-01-17 05:30:...|         0|
        +---+----+-------+-----+--------------------+----------+
        * */
    }

    @Test //lag(字段)上一列的值
    public void lagTest(){
        //SELECT id,name,clazzid,scope,Date,lag(sal) OVER (partition by clazzid ORDER BY scope desc) as next_val FROM emp
        Column over = lag(studentDataset.col("scope"),1,0).over(partitionWindow);
        studentDataset.select(
                studentDataset.col("id"),
                studentDataset.col("name"),
                studentDataset.col("clazzid"),
                studentDataset.col("scope"),
                studentDataset.col("Date"),
                over.as("prev_val")).show();
        /*
        +---+----+-------+-----+--------------------+--------+
        | id|name|clazzid|scope|                Date|prev_val|
        +---+----+-------+-----+--------------------+--------+
        |  4|  ge|    125|   88|2016-02-07 05:30:...|       0|
        |  5| guo|    124|   78|2016-02-07 05:30:...|       0|
        |  6| tom|    124|   78|2015-01-07 05:30:...|      78|
        |  3|  ge|    124|   71|2016-01-27 05:30:...|      78|
        |  1| Yin|    123|   98|2016-01-07 05:30:...|       0|
        |  2| gao|    123|   95|2016-01-17 05:30:...|      98|
        +---+----+-------+-----+--------------------+--------+
        * */
    }

    @Test //首值
    public void firstVal(){
        //SELECT id,name,clazzid,scope,Date,frist_value(sal) OVER (partition by clazzid ORDER BY scope desc) as firstVal FROM emp
        Column over = first(studentDataset.col("scope")).over(partitionWindow);
        studentDataset.select(
                studentDataset.col("id"),
                studentDataset.col("name"),
                studentDataset.col("clazzid"),
                studentDataset.col("scope"),
                studentDataset.col("Date"),
                over.as("firstVal")).show();
        /*
        +---+----+-------+-----+--------------------+--------+
        | id|name|clazzid|scope|                Date|firstVal|
        +---+----+-------+-----+--------------------+--------+
        |  4|  ge|    125|   88|2016-02-07 05:30:...|      88|
        |  5| guo|    124|   78|2016-02-07 05:30:...|      78|
        |  6| tom|    124|   78|2015-01-07 05:30:...|      78|
        |  3|  ge|    124|   71|2016-01-27 05:30:...|      78|
        |  1| Yin|    123|   98|2016-01-07 05:30:...|      98|
        |  2| gao|    123|   95|2016-01-17 05:30:...|      98|
        +---+----+-------+-----+--------------------+--------+
        * */
    }

    @Test //尾值 spark sql 貌似并不支持？怎么实现？看lastValWindow()
    public void lastVal(){
        //SELECT id,name,clazzid,scope,Date,last_value(sal) OVER (partition by clazzid ORDER BY scope desc) as lastVal FROM emp
        Column over = last(studentDataset.col("scope")).over(partitionWindow);
        studentDataset.select(
                studentDataset.col("id"),
                studentDataset.col("name"),
                studentDataset.col("clazzid"),
                studentDataset.col("scope"),
                studentDataset.col("Date"),
                over.as("lastVal")).show();
        /*
        +---+----+-------+-----+--------------------+-------+
        | id|name|clazzid|scope|                Date|lastVal|
        +---+----+-------+-----+--------------------+-------+
        |  4|  ge|    125|   88|2016-02-07 05:30:...|     88|
        |  5| guo|    124|   78|2016-02-07 05:30:...|     78|
        |  6| tom|    124|   78|2015-01-07 05:30:...|     78|
        |  3|  ge|    124|   71|2016-01-27 05:30:...|     71|
        |  1| Yin|    123|   98|2016-01-07 05:30:...|     98|
        |  2| gao|    123|   95|2016-01-17 05:30:...|     95|
        +---+----+-------+-----+--------------------+-------+
        * */
    }

    @Test
    public void lastValWindow(){
        //SQL
        /**SELECT empno,deptno,sal,last_value(sal) OVER
         *(partition by clazzid ORDER BY scope desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as lastVal FROM emp;
         */
         WindowSpec windowSpec = Window.partitionBy
                (studentDataset.col("clazzid"))
                .orderBy(studentDataset.col("scope").desc())
                .rowsBetween(Window.currentRow(), Window.unboundedFollowing());

        Column over = last(studentDataset.col("scope")).over(windowSpec);
        studentDataset.select(
                studentDataset.col("id"),
                studentDataset.col("name"),
                studentDataset.col("clazzid"),
                studentDataset.col("scope"),
                studentDataset.col("Date"),
                over.as("lastVal")).show();
        /*
        +---+----+-------+-----+--------------------+-------+
        | id|name|clazzid|scope|                Date|lastVal|
        +---+----+-------+-----+--------------------+-------+
        |  4|  ge|    125|   88|2016-02-07 05:30:...|     88|
        |  5| guo|    124|   78|2016-02-07 05:30:...|     71|
        |  6| tom|    124|   78|2015-01-07 05:30:...|     71|
        |  3|  ge|    124|   71|2016-01-27 05:30:...|     71|
        |  1| Yin|    123|   98|2016-01-07 05:30:...|     95|
        |  2| gao|    123|   95|2016-01-17 05:30:...|     95|
        +---+----+-------+-----+--------------------+-------+
        * */
    }

    @Test //按scope排序，将成绩分2组
    public void ntileTest(){
        //SELECT empno,deptno,sal,ntile(2) OVER (ORDER BY sal desc) as ntileVal FROM emp
        WindowSpec partitionWindow = Window
                .orderBy(studentDataset.col("scope").desc());
        Column over = ntile(2).over(partitionWindow);
        studentDataset.select(
                studentDataset.col("id"),
                studentDataset.col("name"),
                studentDataset.col("clazzid"),
                studentDataset.col("scope"),
                studentDataset.col("Date"),
                over.as("ntileVal")).show();
        /*
        +---+----+-------+-----+--------------------+--------+
        | id|name|clazzid|scope|                Date|ntileVal|
        +---+----+-------+-----+--------------------+--------+
        |  1| Yin|    123|   98|2016-01-07 05:30:...|       1|
        |  2| gao|    123|   95|2016-01-17 05:30:...|       1|
        |  4|  ge|    125|   88|2016-02-07 05:30:...|       1|
        |  5| guo|    124|   78|2016-02-07 05:30:...|       2|
        |  6| tom|    124|   78|2015-01-07 05:30:...|       2|
        |  3|  ge|    124|   71|2016-01-27 05:30:...|       2|
        +---+----+-------+-----+--------------------+--------+
        * */
    }

    @Test // cume_dist结果为相对位置/总行数。返回值(0,1]
    public void cumeDist(){
        //SELECT empno,deptno,sal,cume_dist() OVER (PARTITION BY deptno ORDER BY sal desc) as cumeDistVal FROM emp
        Column over = cume_dist().over(partitionWindow);
        studentDataset.select(
                studentDataset.col("id"),
                studentDataset.col("name"),
                studentDataset.col("clazzid"),
                studentDataset.col("scope"),
                studentDataset.col("Date"),
                over.as("cumeDistVal")).show();
        /*
       +---+----+-------+-----+--------------------+------------------+
        | id|name|clazzid|scope|                Date|       cumeDistVal|
        +---+----+-------+-----+--------------------+------------------+
        |  4|  ge|    125|   88|2016-02-07 05:30:...|               1.0|
        |  5| guo|    124|   78|2016-02-07 05:30:...|0.6666666666666666|
        |  6| tom|    124|   78|2015-01-07 05:30:...|0.6666666666666666|
        |  3|  ge|    124|   71|2016-01-27 05:30:...|               1.0|
        |  1| Yin|    123|   98|2016-01-07 05:30:...|               0.5|
        |  2| gao|    123|   95|2016-01-17 05:30:...|               1.0|
        +---+----+-------+-----+--------------------+------------------+
        * */
    }

}
