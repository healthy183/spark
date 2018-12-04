package com.kang.spark.sql.json;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
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
public class DataSetAggregateFunction extends BaseTest {

    private Dataset<Row> studentDataset;

    @Before
    public void initDataSet(){
        List<String> studentStr = Arrays.asList(
                "[" +
                        "{\"id\":\"1\",\"name\":\"Yin\",\"clazzid\":\"123\",\"scope\":\"98\"}," +
                        "{\"id\":\"2\",\"name\":\"gao\",\"clazzid\":\"123\",\"scope\":\"95\"}," +
                        "{\"id\":\"3\",\"name\":\"ge\", \"clazzid\":\"124\",\"scope\":\"71\"}," +
                        "{\"id\":\"4\",\"name\":\"ge\", \"clazzid\":\"125\",\"scope\":\"88\"}," +
                        "{\"id\":\"5\",\"name\":\"tom\",\"clazzid\":\"124\",\"scope\":\"78\"} " +
                        "]");
        Dataset<String> studentStrDataset = spark.createDataset(studentStr, Encoders.STRING());
        studentDataset = spark.read().json(studentStrDataset);
        studentDataset.show();
    }

    @Test //返回一个组中不同项目的近似去重数的总和
    public void sumDistinctTest() {
        studentDataset.groupBy("name")
                .agg(sumDistinct("scope")).show();
        /*
        +----+-------------------+
        |name|sum(DISTINCT scope)|
        +----+-------------------+
        |  ge|              159.0|
        | gao|               95.0|
        | Yin|               98.0|
        | tom|               78.0|
        +----+-------------------+
        * */
    }

    @Test //返回一个组中不同项目的近似去重数
    public void approxCountDistinctTest(){
        studentDataset.groupBy("name")
                .agg(approxCountDistinct("scope")).show();
        /*
        +----+----------------------------+
        |name|approx_count_distinct(scope)|
        +----+----------------------------+
        |  ge|                           2|
        | gao|                           1|
        | Yin|                           1|
        | tom|                           1|
        +----+----------------------------+
        * */
    }

    @Test //name分组，列出所有scope
    public void collectList(){
        studentDataset.groupBy("name")
                .agg(collect_list("scope")).show();
        /*
        +----+-------------------+
        |name|collect_list(scope)|
        +----+-------------------+
        |  ge|           [71, 88]|
        | gao|               [95]|
        | Yin|               [98]|
        | tom|               [78]|
        +----+-------------------+
        * */
    }

    @Test //name分组，列出所有去重后scope
    public void collectSet(){
        studentDataset.groupBy("name")
                .agg(collect_set("scope")).show();
    }


    @Test //返回两列的相关系数？不懂
    public void corrTest(){
        studentDataset.groupBy("name")
                .agg(corr("scope","clazzid")).show();
        /*
        +----+--------------------+
        |name|corr(scope, clazzid)|
        +----+--------------------+
        |  ge|                 1.0|
        | gao|                 NaN|
        | Yin|                 NaN|
        | tom|                 NaN|
        +----+--------------------+
        */
    }

    @Test //countDistinct 'name'分组后统计'clazzid'不重复元素个数
    public void countDistinctTest(){
        studentDataset.groupBy("name")
                .agg(countDistinct("clazzid"),current_date()).show();
        /*
        +----+-----------------------+
        |name|count(DISTINCT clazzid)|
        +----+-----------------------+
        |  ge|                      2|
        | gao|                      1|
        | Yin|                      1|
        | tom|                      1|
        +----+-----------------------+
        * */
    }

    @Test //kurtosis返回此组scope峰值
    public void kurtosisTest(){
        studentDataset.groupBy("name")
                .agg(kurtosis("scope"),current_date()).show();
        /*
        +----+---------------+--------------+
        |name|kurtosis(scope)|current_date()|
        +----+---------------+--------------+
        |  ge|           -2.0|    2018-10-04|
        | gao|            NaN|    2018-10-04|
        | Yin|            NaN|    2018-10-04|
        | tom|            NaN|    2018-10-04|
        +----+---------------+--------------+
        * */
    }

    @Test //返回此组scope偏斜值？不懂
    public void skewnessTest(){
        studentDataset.groupBy("name")
                .agg(skewness("scope")).show();
        /*
        +----+---------------+
        |name|skewness(scope)|
        +----+---------------+
        |  ge|            0.0|
        | gao|            NaN|
        | Yin|            NaN|
        | tom|            NaN|
        +----+---------------+
        * */
    }

    @Test //分组后scope总体标准差  和stddev_samp一样？
    public void stddevPop(){
        studentDataset.groupBy("name")
                .agg(stddev_pop("scope")).show();
        /*
        +----+-----------------+
        |name|stddev_pop(scope)|
        +----+-----------------+
        |  ge|              8.5|
        | gao|              0.0|
        | Yin|              0.0|
        | tom|              0.0|
        +----+-----------------+
        * */
    }



    @Test //分组后sope的方差
    public void varPop(){
        studentDataset.groupBy("name")
                .agg(var_pop("scope")).show();
        /*
        +----+--------------+
        |name|var_pop(scope)|
        +----+--------------+
        |  ge|         72.25|
        | gao|           0.0|
        | Yin|           0.0|
        | tom|           0.0|
        +----+--------------+
        * */
    }

    @Test //分组后sope的无偏方差
    public void varSamp(){
        studentDataset.groupBy("name")
                .agg(var_samp("scope")).show();
        /*
        +----+---------------+
        |name|var_samp(scope)|
        +----+---------------+
        |  ge|          144.5|
        | gao|            NaN|
        | Yin|            NaN|
        | tom|            NaN|
        +----+---------------+
        * */
    }

}
