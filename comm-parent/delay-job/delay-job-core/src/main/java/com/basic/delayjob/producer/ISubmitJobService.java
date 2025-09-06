package com.basic.delayjob.producer;

import com.basic.delayjob.model.DelayJob;


/**
 * 提交任务接口
 */
public interface ISubmitJobService {

    /**
     * 提交延时任务
     * @param job
     * @param execTime
     */
     boolean submitJob(DelayJob job, Long execTime);

     boolean submitJob(DelayJob job);


    /**
     * 取消延时任务
     * @param jobId
     */
     boolean cancelDelayJob(String topic, String jobId);


}
