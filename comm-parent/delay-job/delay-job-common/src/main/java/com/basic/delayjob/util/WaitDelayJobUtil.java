package com.basic.delayjob.util;

import com.alibaba.fastjson.JSONObject;
import com.basic.delayjob.constant.DelayJobConstant;
import com.basic.delayjob.model.DelayJob;
import com.basic.delayjob.model.WaitDelayJob;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;

/**
 * 发布等待任务工具类，任务统一发送到 PUBLISH_WAITDELAYJOB_TOPIC 分发
 */
@Slf4j
public class WaitDelayJobUtil {

    /**
     * 创建延时任务
     *
     * @param topic       要发布到的主题
     * @param jobId       任务id
     * @param execTime    任务执行时间
     * @param ttlTime     任务的执行超时时间
     * @param waitEndTime 任务等待时间
     * @param jobContent  任务执行使用的实体
     * @return 消息发送成功返回 true
     */
    public static boolean createJob(RocketMQTemplate rocketMQTemplate, String topic, String jobId, long execTime, long ttlTime, long waitEndTime, String jobContent) {
        if (rocketMQTemplate == null) {
            throw new NullPointerException("rocketMQTemplate is null,Please check your configuration");
        } else {
            // 发布到等待延时任务
            WaitDelayJob waitDelayJob = new WaitDelayJob(topic, jobId, execTime, ttlTime, jobContent, (byte) 1, waitEndTime);
            SendResult sendResult = rocketMQTemplate.syncSend(DelayJobConstant.PUBLISH_WAITDELAYJOB_TOPIC, JSONObject.toJSONString(waitDelayJob));
            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                log.info("create Job to topic {} success,jobContent is {}", topic, jobContent);
                return true;
            }
            log.error("create Job to topic {} error,jobContent is {}", topic, jobContent);
        }
        return false;
    }

    public static boolean createJob(RocketMQTemplate rocketMQTemplate, String topic, String jobId, long execTime, long ttlTime, long waitEndTime, JSONObject jobContent) {
        return createJob(rocketMQTemplate, topic, jobId, execTime, ttlTime, waitEndTime, jobContent.toJSONString());
    }

    public static boolean createJob(RocketMQTemplate rocketMQTemplate, String topic, String jobId, long execTime, long ttlTime, String jobContent) {
        return createJob(rocketMQTemplate, topic, jobId, execTime, ttlTime, 0, jobContent);
    }

    public static boolean createJob(RocketMQTemplate rocketMQTemplate, String topic, String jobId, long execTime, long ttlTime, JSONObject jobContent) {
        return createJob(rocketMQTemplate, topic, jobId, execTime, ttlTime, 0, jobContent.toJSONString());
    }

    public static boolean createJob(RocketMQTemplate rocketMQTemplate, DelayJob delayJob) {
        return createJob(rocketMQTemplate, delayJob.getTopic(), delayJob.getJobId(), delayJob.getExecTime(), delayJob.getTtlTime(), 0, delayJob.getMessage());
    }

    public static boolean createJob(RocketMQTemplate rocketMQTemplate, WaitDelayJob delayJob) {
        return createJob(rocketMQTemplate, delayJob.getTopic(), delayJob.getJobId(), delayJob.getExecTime(), delayJob.getTtlTime(), delayJob.getWaitEndTime(), delayJob.getMessage());
    }

    /**
     * 取消等待任务
     *
     * @param topic 要取消的任务主题
     * @param jobId 要取消的任务id
     * @return 消息发送成功返回 true
     */
    public static boolean cancelJob(RocketMQTemplate rocketMQTemplate, String topic, String jobId) {
        if (rocketMQTemplate == null) {
            throw new NullPointerException("rocketMQTemplate is null,Please check your configuration");
        } else {
            WaitDelayJob waitDelayJob = new WaitDelayJob(topic, jobId, (byte) 2);
            return rocketMQTemplate.syncSend(DelayJobConstant.PUBLISH_WAITDELAYJOB_TOPIC, JSONObject.toJSONString(waitDelayJob)).getSendStatus() == SendStatus.SEND_OK;
        }
    }
}
