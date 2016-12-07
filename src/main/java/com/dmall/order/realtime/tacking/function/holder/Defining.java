package com.dmall.order.realtime.tacking.function.holder;

import java.io.Serializable;

/**
 * @author: jiang.huang
 * @create Date: 2016/12/7 Time: 13:53
 */
public class Defining {
    public static Holder get() {
        final Holder holder = new Holder();
        holder.r=(Runnable&Serializable)()->{};
        return holder;
    }
}
