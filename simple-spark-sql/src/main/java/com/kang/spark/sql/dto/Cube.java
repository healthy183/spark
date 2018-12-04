package com.kang.spark.sql.dto;

/**
 * User:
 * Description:
 * Date: 2018-09-24
 * Time: 14:31
 */
public class Cube implements java.io.Serializable {

    private int value;
    private int cube;

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public int getCube() {
        return cube;
    }

    public void setCube(int cube) {
        this.cube = cube;
    }
}
