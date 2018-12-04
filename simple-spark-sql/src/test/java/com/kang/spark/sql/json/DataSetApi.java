package com.kang.spark.sql.json;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * User:
 * Description:
 * Date: 2018-10-01
 * Time: 17:48
 */
public class DataSetApi extends  BaseTest  {
    private static final String  HDFS_IP ="192.168.59.130";

    private static  final String JSON_PATH =
            "hdfs://"+HDFS_IP+":9000/jsonDemoDir/simpleJson.json";
    private Dataset<Row> df;

    @Before
    public void initDataSet(){
      df = spark.read().option("multiline","true")
              .json(JSON_PATH).cache();
    }

    @Test
    public void columns(){
        df.printSchema(); //打印字段
        for(String str : df.columns()) {
            System.out.println(str);
        }
    }

    @Test
    public void withColumn(){
        //添加字段
        df.withColumn("ageDouble",col("age").plus(1)).show();
        //更改字段名
        df.withColumnRenamed("age","ageRename").show();
    }

    @Test
    public void show(){
        df.show(); //打印所有记录
         /* +---+----+----+
        |age|  id|name|
        +---+----+----+
        | 19| 001| 昂昂溪|
        | 19| 002|null|
        | 19|null|  敖包|
        | 19|null|null|
        +---+----+----+
        * */
    }

    @Test
    public void showAndDistinct(){
        df.select("name").show(); //只查name
        df.select("name").distinct().show(); //只查name并去从
        //查姓名，年龄(并加1)
        df.select(col("name"), col("age").plus(1)).show();
    }


    @Test
    public void take(){
        //take() take top 2 records
        Row[] datasets  = (Row[])df.take(2);
        //System.out.println(datasets.toString());
        for(Row row : datasets){
            System.out.println((String) row.getAs("name"));
        }
    }

    @Test
    public void filter(){
        //年龄大约21岁
        df.filter(col("age").gt(21)).show();
        df.filter(col("age").lt(21)).show(); //小于
        df.filter(col("name").equalTo("敖包")).show();
        df.where("age > 18 and name = '敖包' ").show();
        df.filter(col("age").gt(17).and(col("name").equalTo("敖包"))).show();
        df.filter(col("age").gt(17)).sort(col("name").asc()).sort(col("id").asc()).show();
        df.filter(col("age").gt(17)).sort(col("name").asc()).sort(col("id").asc()).show(1);
    }

    @Test
    public void groupBy(){
        //姓名总数
        df.groupBy("name").count().show();
        //分组然后having过滤
        df.groupBy("name").count().filter(col("count").gt(1)).show();
        //分组函数并重命名字段
        df.groupBy("name").agg(count("name"), max("age"), avg("age"))
                .toDF("name","countOfname","maxOfAge","avgOfAge").show();
    }

    @Test
    public void createOrReplaceTempView(){
        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();
    }

    @Test
    public void collect(){
        Row[] collect = (Row[]) df.collect();
        for(Row row : collect){
            System.out.println(row.get(0));
        }
    }

    @Test
    public void collectAsList(){
        List<Row> rows = df.collectAsList(); //转成list
        for(Row row : rows){
            System.out.println((String) row.getAs("name"));
        }
    }

    @Test
    public void createGlobalTempView(){
        try {
            df.createGlobalTempView("gt_people");
            spark.sql("SELECT * FROM global_temp.gt_people").show();
            spark.newSession().sql("SELECT * FROM global_temp.gt_people").show();
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }


    @Test //即sql的unionall
    public void union(){
        String JSON_PATH_UNION =
                "hdfs://"+HDFS_IP+":9000/jsonDemoDir/simpleJsonUnion.json";
        Dataset<Row>  unionDS = spark.read().option("multiline","true")
                .json(JSON_PATH_UNION).cache();
        df.union(unionDS).show();
    }

    @Test  //df中不在exceptDS的对象(差集)
    public void except(){
        String JSON_PATH_EXCEPT =
                "hdfs://"+HDFS_IP+":9000/jsonDemoDir/simpleJsonUnion.json";
        Dataset<Row>  exceptDS = spark.read().option("multiline","true")
                .json(JSON_PATH_EXCEPT).cache();
        df.except(exceptDS).show();
    }

    @Test //交集
    public void intersect(){
        String JSON_PATH_INTERSECT =
                "hdfs://"+HDFS_IP+":9000/jsonDemoDir/simpleJsonUnion.json";
        Dataset<Row>  intersectDS = spark.read().option("multiline","true")
                .json(JSON_PATH_INTERSECT).cache();
        df.intersect(intersectDS).show();
    }

    @Test
    public void save(){
        //无操作权限
        //Permission denied: user=healthy, access=WRITE, inode="/jsonDemoDir":root:supergroup:drwxr-xr-x
        df.write().json("hdfs://"+HDFS_IP+":9000/jsonDemoDir/save.json");
    }

    @Test //某字段统计详情
    public void describe(){
        String JSON_PATH_EXCEPT =
                "hdfs://"+HDFS_IP+":9000/jsonDemoDir/simpleJsonUnion.json";
        Dataset<Row>  exceptDS = spark.read().option("multiline","true")
                .json(JSON_PATH_EXCEPT).cache();
        exceptDS.describe("age").show();
        /** +-------+------------------+
        |summary|               age|
        +-------+------------------+
        |  count|                 7|
        |   mean|18.857142857142858|
        | stddev|3.9339789623472163|
        |    min|                11|
        |    max|                24|
        +-------+------------------+
        * */
    }

    @Test
    public void dtypes(){
        Tuple2<String, String>[] dtypes = df.dtypes();
        for(Tuple2 tuple2 : dtypes){
            System.out.println(tuple2.toString());
        }
        /*
        (age,LongType)
        (id,StringType)
        (name,StringType)
        * */
    }

    @Test
    public void explain(){
        df.explain(); //逻辑计划 执行计划 物理计划
    }

    @Test
    public void saveOfTable(){
        //能创建表，但是没有数据，为什么？
        df.write().bucketBy(42, "age").saveAsTable("simpleJson_table");
        Dataset<Row> namesDF = spark.sql("SELECT * FROM simpleJson_table");
        namesDF.show();
    }

    @Test
    public void persist(){
        df.persist(); //持久化
        df.unpersist(); //取消持久化
    }

    @Test //不重复的随机抽样
    public void sample(){
        df.sample(false,1).show();
    }

    //ClassCastException: cannot assign instance of java.lang.invoke.SerializedLambda
    @Test
    public void foreach(){
        List<String> clazzStr = Arrays.asList(
                "[{\"clazzid\":\"123\",\"class\":\"123Clazz\"}," +
                        "{\"clazzid\":\"125\",\"class\":\"125Clazz\"}]");
        Dataset<String> clazzStrDataset  = spark.createDataset(clazzStr, Encoders.STRING());
        Dataset<Row>  clazzDataset = spark.read().option("multiline","true").json(clazzStrDataset);
        clazzDataset.foreach(row -> {
            System.out.println(row.get(1));
        });
    }

    @Test  //?
    public void map(){
        //df.map()
    }

    @Test //?
    public void repartition(){
        df.repartition(2).show();
    }

    @Test //执行计划
    public void queryExecution(){
        df.queryExecution();
    }

    @Test //将字段key1空值改成xxx
    public void fillTest(){
        //df.na().fill("xxx",df.col("key1")).show();
    }
}

