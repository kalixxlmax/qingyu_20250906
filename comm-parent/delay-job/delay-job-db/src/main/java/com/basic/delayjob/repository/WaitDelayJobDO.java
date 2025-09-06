package com.basic.delayjob.repository;

import com.basic.delayjob.model.DelayJob;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * 等待延时的任务
 *
 * 大于2天延时任务 推荐先放入等待延时的任务中
 * @param <T>
 */
@Setter
@Getter
@Document(value = "wait_delayjob")
public class WaitDelayJobDO extends DelayJob {

    /**
     * 等待结束时间
     *
     * 延时任务 3天后执行
     *
     * 等待结束时间为 两天后
     *
     * 定时任务半天检查一次 ,放入延时任务队列
     *
     */
    private long waitEndTime;

}
