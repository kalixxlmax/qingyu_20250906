package com.basic.delayjob;

import com.basic.delayjob.constant.DelayJobConstant;
import com.basic.delayjob.consumer.IExecuteDealyJob;
import com.basic.delayjob.model.DelayJob;
import com.basic.delayjob.model.DelayJobType;
import com.basic.delayjob.model.WaitDelayJob;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class PushExecDelayJobService implements IExecuteDealyJob{



    @Lazy
    @Autowired
    private RocketMQTemplate rocketMQTemplate;


    @Override
    public void execute(DelayJob job) {
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

    }



    public void publishDelayJob(DelayJob job){
        log.info("发布延时任务 topic {}  jobId {} message {}",
                job.getTopic(),job.getJobId(),job.getMessage());

        try {
            SendResult sendResult = rocketMQTemplate.syncSend(
                    DelayJobConstant.PUBLISH_DELAYJOB_TOPIC, job.toString());
            if(SendStatus.SEND_OK!=sendResult.getSendStatus()){
                log.info("发布失败  -->  sendStatus {}",sendResult.getSendStatus() );
            }
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }


    }

    public void cancelDelayJob(String topic,String jobId){
        log.info("取消延时任务 topic {}  jobId {} ",
                topic,jobId);

        try {
            DelayJob delayJob=new DelayJob(topic,jobId,DelayJobType.CANCEL.getType());
            SendResult sendResult = rocketMQTemplate.syncSend(
                    DelayJobConstant.PUBLISH_DELAYJOB_TOPIC,delayJob.toString());
            if(SendStatus.SEND_OK!=sendResult.getSendStatus()){
                log.info("发布失败  -->  sendStatus {}",sendResult.getSendStatus() );
            }
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }

    public void publishWaitDelayJob(WaitDelayJob job){
        log.info("发布待处理延时任务 topic {}  jobId {} message {}",
                job.getTopic(),job.getJobId(),job.getMessage());

        try {
            SendResult sendResult = rocketMQTemplate.syncSend(
                    DelayJobConstant.PUBLISH_WAITDELAYJOB_TOPIC, job.toString());
            if(SendStatus.SEND_OK!=sendResult.getSendStatus()){
                log.info("发布失败  -->  sendStatus {}",sendResult.getSendStatus() );
            }
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }

    public void cancelWaitDelayJob(String topic,String jobId){
        log.info("取消待处理延时任务 topic {}  jobId {} ",
                topic,jobId);

        try {
            WaitDelayJob delayJob=new WaitDelayJob(topic,jobId, DelayJobType.CANCEL.getType());
            SendResult sendResult = rocketMQTemplate.syncSend(
                    DelayJobConstant.PUBLISH_WAITDELAYJOB_TOPIC,delayJob.toString());
            if(SendStatus.SEND_OK!=sendResult.getSendStatus()){
                log.info("发布失败  -->  sendStatus {}",sendResult.getSendStatus() );
            }
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }
}
