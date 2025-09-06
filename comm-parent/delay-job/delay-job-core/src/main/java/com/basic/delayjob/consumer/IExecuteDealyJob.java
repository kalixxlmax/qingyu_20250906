package com.basic.delayjob.consumer;

import com.basic.delayjob.model.DelayJob;

/**
 * 执行消费任务接口
 */
public interface IExecuteDealyJob {

    public void execute(DelayJob job);
}
