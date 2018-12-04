package com.kang.spark.sql.dto;

import java.io.Serializable;

/**
 * User:
 * Description:
 * Date: 2018-09-23
 * Time: 18:21
 */
public class Average implements Serializable {

    private long sum;
    private long count;

    public Average(){};

    public Average(long sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
