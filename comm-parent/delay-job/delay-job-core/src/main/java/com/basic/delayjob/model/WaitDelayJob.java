package com.basic.delayjob.model;

import lombok.Getter;
import lombok.Setter;

/**
 * 等待延时的任务
 *
 * 大于2天延时任务 推荐先放入等待延时的任务中
 * @param <T>
 */
@Setter
@Getter
public class WaitDelayJob extends DelayJob {

    /**
     * 等待结束时间
     *
     * 延时任务 3天后执行
     *
     * 等待结束时间为 两天后
     *
     * 定时任务半天检查一次 ,放入延时任务队列
     *
     */
    private long waitEndTime;



    public WaitDelayJob() {

    }



    public WaitDelayJob( String topic,String jobId,byte type) {
        this.topic = topic;
        this.jobId = jobId;
        this.type=type;

    }

    public WaitDelayJob(String topic,String jobId, long execTime, long ttlTime, String message, byte type, long waitEndTime) {
        super(topic,jobId,execTime, ttlTime, message, type);
        this.waitEndTime = waitEndTime;
    }

    public WaitDelayJob(DelayJob delayJob) {
        super(delayJob.getTopic(),delayJob.getJobId(),delayJob.getExecTime()
                , delayJob.getTtlTime(), delayJob.getMessage(), delayJob.getType());
        this.waitEndTime=delayJob.getExecTime()-7200;

    }
}
