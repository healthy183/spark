package com.kang.spark.sql.dto;

import java.io.Serializable;

/**
 * User:
 * Description:
 * Date: 2018-09-03
 * Time: 1:56
 */
public class Person implements Serializable {

    private static final long serialVersionUID = -5871880941802444567L;
    private String name;
    private int age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
