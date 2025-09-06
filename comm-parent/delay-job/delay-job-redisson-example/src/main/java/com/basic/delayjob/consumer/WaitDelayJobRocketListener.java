package com.basic.delayjob.consumer;

import com.basic.delayjob.constant.DelayJobConstant;
import com.basic.delayjob.model.DelayJobType;
import com.basic.delayjob.model.WaitDelayJob;
import com.basic.delayjob.producer.IWaitDelayJobService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@RocketMQMessageListener(topic = DelayJobConstant.PUBLISH_WAITDELAYJOB_TOPIC,
        consumerGroup = "publish-waitdelayjob-consumer",
        messageModel = MessageModel.CLUSTERING
)
@Service
public class WaitDelayJobRocketListener implements RocketMQListener<WaitDelayJob> {

    @Autowired
    private IWaitDelayJobService waitDelayJobService;

    @Override
    public void onMessage(WaitDelayJob msg) {
        log.info("收到 {} 待处理延时任务 --> topic {} jobId {} execTime {} ",
                msg.getType(),msg.getTopic(),msg.getJobId(),msg.getExecTime());

        try {
            if(msg.getType() == DelayJobType.PUBLISH.getType()){
                waitDelayJobService.publishWaitJob(msg);
            }else if(msg.getType() ==DelayJobType.CANCEL.getType()){
                waitDelayJobService.cancelWaitJob(msg.getTopic(),msg.getJobId());
            }
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }


    }
    
}
