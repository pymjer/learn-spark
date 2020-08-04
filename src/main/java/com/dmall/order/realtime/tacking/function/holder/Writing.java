package com.dmall.order.realtime.tacking.function.holder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * @author: jiang.huang
 * @create Date: 2016/12/7 Time: 13:54
 */
public class Writing {
    static final File f = new File(System.getProperty("java.io.tmpdir"), "x.ser");

    public static void main(String... arg) throws IOException {
        try (FileOutputStream os = new FileOutputStream(f);
             ObjectOutputStream oos = new ObjectOutputStream(os)) {
            oos.writeObject(Defining.get());
        }
        System.out.println("written to " + f);
    }
}
