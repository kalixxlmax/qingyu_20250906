package com.basic.delayjob.model;

import com.alibaba.fastjson.JSON;
import lombok.Data;

/**
 * 延时任务
 */
@Data
public class DelayJob {


    public DelayJob() {
    }

    public DelayJob( String topic,String jobId,byte type) {
        this.topic = topic;
        this.jobId = jobId;
        this.type=type;

    }
    public DelayJob(String topic,String jobId, long execTime, long ttlTime, String message) {
        this.topic = topic;
        this.jobId = jobId;
        this.execTime = execTime;
        this.ttlTime = ttlTime;
        this.message = message;
    }
    public DelayJob(String topic,String jobId, long execTime, long ttlTime, String message, byte type) {
        this.topic = topic;
        this.jobId = jobId;
        this.execTime = execTime;
        this.ttlTime = ttlTime;
        this.message = message;
        this.type = type;
    }


    /**
     * 任务主体（具体业务类型）
     */
    protected String topic;

    /**
     * 延迟任务的唯一标识，用于检索任务
     */
    protected String jobId;

    /**
     * 任务执行时间(时间戳:精确到秒)
     */
    protected long execTime;

    /**
     * 任务的执行超时时间
     */
    protected long ttlTime;

    /**
     * 重试次数
     */
    protected int retryTimes = 0;

    /**
     * 消费失败，重新消费间隔(单位秒)
     * 默认0L, 消费失败不重新消费
     */
    protected long retryDelay = 0L;

    /**
     * 任务具体的消息内容，用于处理具体业务逻辑用
     */
    protected String message;


    /**
     * 1 发布任务
     *
     * 2.取消任务
     */
    protected byte type=1;

    /**
     * 任务状态
     */
    protected byte status;



    @Override
    public String toString(){
        return JSON.toJSONString(this);
    }
}
