package com.kang.spark.sql.parquet;

import com.kang.spark.sql.dto.Cube;
import com.kang.spark.sql.dto.Square;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * User:
 * Description:
 * Date: 2018-09-24
 * Time: 14:30
 */
public class SaveParquet {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.some.config.option", "some-value");
        sparkConf.set("spark.driver.host","192.168.59.3");
        //sparkConf.set("spark.sql.files.ignoreCorruptFiles","true");

        SparkSession spark = SparkSession
                .builder()
                .appName("jsonDemo")
                .master("spark://192.168.59.130:7077")
                .config(sparkConf)
                .getOrCreate();

        List<Square> squares = new ArrayList<>();
        for (int value = 1; value <= 5; value++) {
            Square square = new Square();
            square.setValue(value);
            square.setSquare(value * value);
            squares.add(square);
        }

// Create a simple DataFrame, store into a partition directory
        Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
        squaresDF.write().parquet("data/test_table/key=1");

        List<Cube> cubes = new ArrayList<>();
        for (int value = 6; value <= 10; value++) {
            Cube cube = new Cube();
            cube.setValue(value);
            cube.setCube(value * value * value);
            cubes.add(cube);
        }

// Create another DataFrame in a new partition directory,
// adding a new column and dropping an existing column
        Dataset<Row> cubesDF = spark.createDataFrame(cubes, Cube.class);
        cubesDF.write().parquet("data/test_table/key=2");

// Read the partitioned table
        Dataset<Row> mergedDF = spark.read()
                .option("mergeSchema", true)
                .format("parquet")
                .parquet("data/test_table");
        //mergedDF.printSchema();
        //exception
        //Unable to infer schema for Parquet. It must be specified manually.;
        mergedDF.show();

// The final schema consists of all 3 columns in the Parquet files together
// with the partitioning column appeared in the partition directory paths
// root
//  |-- value: int (nullable = true)
//  |-- square: int (nullable = true)
//  |-- cube: int (nullable = true)
//  |-- key: int (nullable = true)


    }
}
