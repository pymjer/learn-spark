package com.hawkins.spark.holder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * @author: jiang.huang
 * @create Date: 2016/12/7 Time: 13:54
 */
public class Reading {
    static final File f=new File(System.getProperty("java.io.tmpdir"), "x.ser");
    public static void main(String... arg) throws IOException, ClassNotFoundException {
        try(FileInputStream is=new FileInputStream(f);
            ObjectInputStream ois=new ObjectInputStream(is)) {
            Holder h=(Holder)ois.readObject();
            System.out.println(h.r);
            h.r.run();
        }
        System.out.println("read from "+f);
    }
}
