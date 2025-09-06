package com.basic.delayjob;

import com.basic.delayjob.model.WaitDelayJob;
import com.basic.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class WaitDelayJobService extends AbstractWaitDelayJobService {




    @Lazy
    @Autowired
    private AbstractSubmitDelayJobService delayJobService;

    @Override
    public boolean publishWaitJob(WaitDelayJob delayJob) {

        if(delayJob.getExecTime() - DateUtil.currentTimeSeconds() < 7200) {
            return delayJobService.submitJob(delayJob,delayJob.getExecTime());
            //submitJobService.submitJob(delayJob,delayJob.getExecTime());
        }else {
           return super.publishWaitJob(delayJob);
        }

    }

    @Override
    public boolean cancelWaitJob(String topic, String jobId) {
        boolean result = super.cancelWaitJob(topic, jobId);
        if(!result){
            return delayJobService.cancelDelayJob(topic,jobId);
        }else {
            return result;
        }
    }

    //@Override
    /*public List<WaitDelayJobDO> getWaitJobList(long waitEndTime) {
        return super.getWaitJobList(waitEndTime);
    }*/

   /*

     @Lazy
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
