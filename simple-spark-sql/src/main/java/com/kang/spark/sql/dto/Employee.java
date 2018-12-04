package com.kang.spark.sql.dto;

import java.io.Serializable;

/**
 * User:
 * Description:
 * Date: 2018-09-23
 * Time: 18:20
 */
public class Employee  implements Serializable {

    private String name;
    private long salary;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getSalary() {
        return salary;
    }

    public void setSalary(long salary) {
        this.salary = salary;
    }
}
