package com.basic.delayjob.repository;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
@Data
@Document(value = "delayjob")
public class DelayJobDO  {


    /**
     * 延迟任务的唯一标识，用于检索任务
     */
    @Id
    private String jobId;

    /**
     * 任务主体（具体业务类型）
     */
    @Indexed
    private String topic;

    /**
     * 任务执行时间(时间戳:精确到秒)
     */
    private long execTime;

    /**
     * 任务的执行超时时间
     */
    private long ttlTime;

    /**
     * 重试次数
     */
    private int retryTimes = 0;

    /**
     * 消费失败，重新消费间隔(单位秒)
     * 默认0L, 消费失败不重新消费
     */
    private long retryDelay = 0L;

    /**
     * 任务具体的消息内容，用于处理具体业务逻辑用
     */
    private String message;

    /**
     * 任务状态
     */
    protected byte status;


    @Override
    public String toString(){
        return JSON.toJSONString(this);
    }


}
