package com.basic.delayjob.producer;

import com.basic.delayjob.model.DelayJob;
import com.basic.delayjob.model.WaitDelayJob;


/**
 * 提交任务接口
 */
public interface IWaitDelayJobService {

    /**
     * 发布等待任务
     * @param job
     */
    public boolean publishWaitJob(WaitDelayJob job);


    /**
     * 取消任务
     * @param jobId
     */
    public boolean cancelWaitJob(String topic, String jobId);


}
