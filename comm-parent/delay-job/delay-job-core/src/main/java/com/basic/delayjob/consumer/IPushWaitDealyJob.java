package com.basic.delayjob.consumer;

import com.basic.delayjob.model.WaitDelayJob;

/**
 * 执行推送 等待任务
 */
@Deprecated
public interface IPushWaitDealyJob {

    public void execute(WaitDelayJob job);
}
