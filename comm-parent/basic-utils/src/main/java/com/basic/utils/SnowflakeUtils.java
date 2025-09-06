package com.basic.utils;

import com.basic.commons.id.SnowflakeIdWorker;

public class SnowflakeUtils {

    private static SnowflakeIdWorker snowflakeIdWorker;

    static {

        snowflakeIdWorker=new SnowflakeIdWorker();
    }

    public static long getNextId(){
        return snowflakeIdWorker.nextId();
    }

    public static String getNextIdStr(){
        return String.valueOf(snowflakeIdWorker.nextId());
    }


    //==============================Test=============================================
    /** 测试 */
    public static void main(String[] args) {
        long startTime=System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            long id = SnowflakeUtils.getNextId();
            //System.out.println(Long.toBinaryString(id));
            System.out.println(id);
        }
        System.out.println("总用时===》 "+(System.currentTimeMillis()-startTime)/1000);
    }
}
