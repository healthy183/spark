package com.kang.spark.sql.tools;

import java.io.File;

/**
 * User:
 * Description:
 * Date: 2018-09-02
 * Time: 22:00
 */
public class FileExits {

    private static  final String JSON_PATH
            = "D:\\idea_workspace\\spark\\simple-spark-sql\\json\\simpleJson.json";

    public static void main(String[] args) {
        File file = new File(JSON_PATH);
        System.out.println(file.exists()+"");
    }
}
