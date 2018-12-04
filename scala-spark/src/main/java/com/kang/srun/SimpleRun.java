package com.kang.srun;

/**
 * User:
 * Description:
 * Date: 2018-11-10
 * Time: 23:51
 */
public class SimpleRun {

    ////scala call java
    public static void main(String[] args) {
        ScalaClassRun scalaClassRun = new ScalaClassRun();
        scalaClassRun.runSimple();
        ScalaObjectRun.objectRun();
    }

    public void simpleRun(){
        System.out.println("simpleRun");
    }


}
