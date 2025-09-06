package com.basic.delayjob.model;


import java.util.Arrays;

/**
 * @Description: 延时任务 类型
 * @author chat 
 * @version V1.0
 */
public enum DelayJobType {


    /**
     * 发布
     */
    PUBLISH(1,"发布"),
    /**
     * 取消
     */
    CANCEL(2,"取消"),
    /**
     * 未知
     */
    UNKNOWN(0, "未知");

    ;
    private DelayJobType(int type, String desc){
        this.type= (byte) type;
        this.desc=desc;
    }

    /**
     * 类型
     */
    private  byte type;
    /**
     * 描述
     */
    private  String desc;

    public static String getAddTypeDesc(int type){
        return Arrays.asList(DelayJobType.values()).stream()
                .filter(value -> value.getType()== type)
                .findFirst().orElse(DelayJobType.UNKNOWN).getDesc();
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
