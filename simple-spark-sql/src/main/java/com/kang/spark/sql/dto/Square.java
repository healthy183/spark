package com.kang.spark.sql.dto;

/**
 * User:
 * Description:
 * Date: 2018-09-24
 * Time: 14:30
 */
public class Square implements  java.io.Serializable {

    private int value;
    private int square;

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public int getSquare() {
        return square;
    }

    public void setSquare(int square) {
        this.square = square;
    }
}
