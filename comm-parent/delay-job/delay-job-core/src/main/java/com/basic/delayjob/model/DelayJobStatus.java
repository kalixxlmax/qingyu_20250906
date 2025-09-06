package com.basic.delayjob.model;


import java.util.Arrays;

/**
 * @Description: 延时任务 状态
 * @author chat 
 * @version V1.0
 */
public enum DelayJobStatus {
    /**
     * 任务状态
     * 0 初始化
     * 1 待发布,等待延时任务
     * 2 已发布,等待执行
     * 3 业务执行中,时间已到
     * 4 执行完成,已完结
     *
     * -1 已取消
     */

    INIT(0,"初始化"),


    WAIT_PUBLISH(1,"待发布"),

    WAITEXEC(2,"待执行"),

    EXECING(3,"执行中"),

    /**
     * 取消
     */
    CANCEL(-1,"已取消"),


    UNKNOWN(-2, "未知");



    ;
    private DelayJobStatus(int type, String desc){
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
        return Arrays.asList(DelayJobStatus.values()).stream()
                .filter(value -> value.getType()== type)
                .findFirst().orElse(DelayJobStatus.UNKNOWN).getDesc();
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
