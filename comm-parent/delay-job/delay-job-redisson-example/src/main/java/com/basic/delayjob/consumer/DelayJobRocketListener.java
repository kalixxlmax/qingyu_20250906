package com.basic.delayjob.consumer;

import com.basic.delayjob.constant.DelayJobConstant;
import com.basic.delayjob.model.DelayJob;
import com.basic.delayjob.model.DelayJobType;
import com.basic.delayjob.producer.ISubmitJobService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@RocketMQMessageListener(topic = DelayJobConstant.PUBLISH_DELAYJOB_TOPIC,
        consumerGroup = "publish-delayjob-consumer",
        messageModel = MessageModel.CLUSTERING
)
@Service
public class DelayJobRocketListener implements RocketMQListener<DelayJob> {

    @Autowired
    private ISubmitJobService submitJobService;

    @Override
    public void onMessage(DelayJob msg) {
        log.info("收到 {} 延时任务 --> topic {} jobId {} execTime {} ",
                msg.getType(),msg.getTopic(),msg.getJobId(),msg.getExecTime());

        try {
            if(msg.getType() ==DelayJobType.PUBLISH.getType()){
                submitJobService.submitJob(msg,msg.getExecTime());
            }else if(msg.getType() ==DelayJobType.CANCEL.getType()){
                submitJobService.cancelDelayJob(msg.getTopic(),msg.getJobId());
            }
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }


    }
}
