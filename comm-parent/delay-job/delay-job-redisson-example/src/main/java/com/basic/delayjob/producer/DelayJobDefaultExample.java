package com.basic.delayjob.producer;

import com.basic.delayjob.AbstractSubmitDelayJobService;
import com.basic.delayjob.AbstractWaitDelayJobService;
import com.basic.delayjob.constant.DelayJobConstant;
import com.basic.delayjob.consumer.DelayJobRocketListener;
import com.basic.delayjob.model.DelayJob;
import com.basic.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * DelayJobDefaultExample <br>
 *
 * @author: lidaye <br>
 * @date: 2021/7/7  <br>
 */
@Slf4j
@Component
public class DelayJobDefaultExample {

   public final static String EXAMPLE_TOPIC="example_topic";

   private String jobId="example_topic_jobId_"+ DateUtil.getYMDString()+"-";


   @Autowired
   private AbstractSubmitDelayJobService delayJobService;


    @Autowired
    private AbstractWaitDelayJobService waitDelayJobService;


    /**
     * 发布延时任务
     */
    public void publishDelayJob(){
       DelayJob delayJob0 =createExampleDelayJob("0",20);
       delayJobService.submitJob(delayJob0);


        DelayJob delayJob1 =createExampleDelayJob("1",30);
        delayJobService.submitJob(delayJob1);

        DelayJob delayJob2 =createExampleDelayJob("2",40);
        delayJobService.submitJob(delayJob2);


        DelayJob delayJob3 =createExampleDelayJob("3",50);
        delayJobService.submitJob(delayJob3);

        DelayJob delayJob4 =createExampleDelayJob("4",60);
        delayJobService.submitJob(delayJob4);


        DelayJob delayJob10=createExampleDelayJob("10",7200*2);
        delayJobService.submitJob(delayJob10);


        delayJobService.cancelDelayJob(EXAMPLE_TOPIC,delayJob2.getJobId());



   }
   private DelayJob createExampleDelayJob(String exampleJobId,long delayTime){
       DelayJob delayJob = new DelayJob();
       delayJob.setTopic(EXAMPLE_TOPIC);
       delayJob.setJobId(jobId+exampleJobId);
       /**
        * 三分钟后执行
        */
       //delayJob.setExecTime(DateUtil.currentTimeSeconds()+60*3);

       delayJob.setExecTime(DateUtil.currentTimeSeconds()+delayTime);
       delayJob.setTtlTime(delayJob.getExecTime());
       delayJob.setMessage("example_topic_message_"+exampleJobId);

       return delayJob;
   }

    /**
     * 取消延时任务
     */
    public void cancelDelayJob(){
        delayJobService.cancelDelayJob(EXAMPLE_TOPIC,jobId);
    }


    @RocketMQMessageListener(topic = EXAMPLE_TOPIC,
            consumerGroup = "publish-delayjob-example",
            messageModel = MessageModel.CLUSTERING
   )
    @Component
   static class DelayJobExecExampleListener implements RocketMQListener<String>{

       @Override
       public void onMessage(String message) {
           log.info("业务层执行延时任务 exec TOPIC {} {} ",EXAMPLE_TOPIC,message);
       }
   }
}
