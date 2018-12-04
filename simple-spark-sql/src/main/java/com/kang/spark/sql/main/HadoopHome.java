package com.kang.spark.sql.main;

import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;

import java.util.Properties;

/**
 * User:
 * Description:
 * Date: 2018-09-02
 * Time: 15:00
 */
public class HadoopHome {

    public static void main(String[] args) {
        //配置完需要重启idea
        String hadoop_home = System.getenv("HADOOP_HOME");
        System.out.println(hadoop_home);
    }
}
