package com.basic.delayjob;

import com.basic.delayjob.consumer.AbstractWaitDelayBucketHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * WaitDelayJobTask <br>
 *
 * @author: lidaye <br>
 * @date: 2021/7/7  <br>
 */
@Slf4j
@Component
@EnableScheduling
public class WaitDelayJobTask {
    @Autowired
    private AbstractWaitDelayBucketHandler abstractWaitDelayBucketHandler;

    @Scheduled(cron = "0 0 0/2 * * ?")
    public void executeHalfAnHourTask(){
        log.info("走一波定时任务");
        abstractWaitDelayBucketHandler.pullWaitDelayJob();
    };
}
