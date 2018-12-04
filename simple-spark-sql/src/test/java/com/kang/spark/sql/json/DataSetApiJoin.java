package com.kang.spark.sql.json;

import com.kang.spark.sql.dto.Person;
import org.apache.spark.sql.*;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;


/**
 * User:
 * Description:
 * Date: 2018-10-02
 * Time: 17:32
 */
public class DataSetApiJoin extends  BaseTest {

    private  Dataset<Row> studentDataset;

    private Dataset<Row> clazzDataset;

    @Before
    public void initDataSet(){
        List<String> studentStr = Arrays.asList(
                "[{\"id\":\"1\",\"name\":\"Yin\",\"clazzid\":\"123\"}," +
                        "{\"id\":\"2\",\"name\":\"ge\",\"clazzid\":\"124\"}]");
        Dataset<String> studentStrDataset = spark.createDataset(studentStr, Encoders.STRING());
        studentDataset = spark.read().json(studentStrDataset);
        studentDataset.show();

        List<String> clazzStr = Arrays.asList(
                "[{\"clazzid\":\"123\",\"class\":\"123Clazz\"}," +
                        "{\"clazzid\":\"125\",\"class\":\"125Clazz\"}]");
        Dataset<String> clazzStrDataset  = spark.createDataset(clazzStr, Encoders.STRING());
        clazzDataset = spark.read().json(clazzStrDataset);
        clazzDataset.show();

    }

    @Test
    public void innerjoin(){
        studentDataset.join(clazzDataset,"clazzid").show();
        /*
        +-------+---+----+--------+
        |clazzid| id|name|   class|
        +-------+---+----+--------+
        |    123|  1| Yin|123Clazz|
        +-------+---+----+--------+
        * */
        //相同
        studentDataset.join(clazzDataset,studentDataset.col("clazzid")
                .equalTo(clazzDataset.col("clazzid"))).show();
        /*+-------+---+----+--------+-------+
         |clazzid| id|name|   class|clazzid|
         +-------+---+----+--------+-------+
         |    123|  1| Yin|123Clazz|    123|
         +-------+---+----+--------+-------+*/
    }

    @Test
    public void innerjoinAnd(){
        studentDataset.join(
                clazzDataset,studentDataset.col("clazzid").equalTo(clazzDataset.col("clazzid"))
                .and(clazzDataset.col("clazzid").equalTo("123"))).show();
        /*
        +-------+---+----+--------+-------+
        |clazzid| id|name|   class|clazzid|
        +-------+---+----+--------+-------+
        |    123|  1| Yin|123Clazz|    123|
        +-------+---+----+--------+-------+
        * */

    }

    @Test
    public void joinWith(){
        Dataset<Tuple2<Row, Row>> dataset = studentDataset.joinWith(clazzDataset,
                studentDataset.col("clazzid")
                        .equalTo(clazzDataset.col("clazzid")));
        dataset.show(); //schema会不一样，注意和上面的对比一下
        /*
        +-------------+---------------+
        |           _1|             _2|
        +-------------+---------------+
        |[123, 1, Yin]|[123Clazz, 123]|
        +-------------+---------------+
        */
    }

    @Test //全查
    public void outJoin(){
        Dataset<Tuple2<Row, Row>> dataset = studentDataset.joinWith(clazzDataset,
                studentDataset.col("clazzid")
                        .equalTo(clazzDataset.col("clazzid")),"outer");
        dataset.show();
        /*
        +-------------+---------------+
        |           _1|             _2|
        +-------------+---------------+
        |         null|[125Clazz, 125]|
        | [124, 2, ge]|           null|
        |[123, 1, Yin]|[123Clazz, 123]|
        +-------------+---------------+
        * */
    }

    @Test //左连接
    public void leftJoin(){
        Dataset<Tuple2<Row, Row>> dataset = studentDataset.joinWith(clazzDataset,
                studentDataset.col("clazzid")
                        .equalTo(clazzDataset.col("clazzid")),"left_outer");
        dataset.show();
        /*
        +-------------+---------------+
        |           _1|             _2|
        +-------------+---------------+
        | [124, 2, ge]|           null|
        |[123, 1, Yin]|[123Clazz, 123]|
        +-------------+---------------+
        * */
    }

    @Test //右连接
    public void rightJoin(){
        Dataset<Tuple2<Row, Row>> dataset = studentDataset.joinWith(clazzDataset,
                studentDataset.col("clazzid")
                        .equalTo(clazzDataset.col("clazzid")),"right_outer");
        dataset.show();
        /*
        +-------------+---------------+
        |           _1|             _2|
        +-------------+---------------+
        |         null|[125Clazz, 125]|
        |[123, 1, Yin]|[123Clazz, 123]|
        +-------------+---------------+
        * */
    }


    @Test //左半连接,相当sql的in
    public void leftsemi(){
        Dataset<Row> dataset = studentDataset.join(clazzDataset,
                studentDataset.col("clazzid")
                        .equalTo(clazzDataset.col("clazzid")),"LeftSemi");
        dataset.show();
        /*
        +-------+---+----+
        |clazzid| id|name|
        +-------+---+----+
        |    123|  1| Yin|
        +-------+---+----+
        * */
    }

    @Test //笛卡尔连接
    public void crossJoin(){
        studentDataset.crossJoin(clazzDataset).show();
    }

}
