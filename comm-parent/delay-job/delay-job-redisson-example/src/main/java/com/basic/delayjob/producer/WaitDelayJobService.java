package com.basic.delayjob.producer;

import com.basic.delayjob.model.WaitDelayJob;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import com.basic.delayjob.AbstractWaitDelayJobService;
//@Slf4j
//@Service
public class WaitDelayJobService extends AbstractWaitDelayJobService {

   /* @Lazy
    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @Override
    public void execute(WaitDelayJob job) {
        log.info("执行延时任务 通知业务层执行任务 topic {}  jobId {} message {}",
                job.getTopic(),job.getJobId(),job.getMessage());

        try {
            SendResult sendResult = rocketMQTemplate.syncSend(job.getTopic(), job.getMessage());
            if(SendStatus.SEND_OK!=sendResult.getSendStatus()){
                log.info("执行延时任务发布失败  -->  sendStatus {}",sendResult.getSendStatus() );
            }
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }*/
}
