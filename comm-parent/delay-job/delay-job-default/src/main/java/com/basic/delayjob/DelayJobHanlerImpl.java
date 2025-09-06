package com.basic.delayjob;

import com.basic.delayjob.consumer.AbstractDelayBucketHandler;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Service;

/**
 * DelayJobHanlerImpl <br>
 *
 * @author: lidaye <br>
 * @date: 2021/7/7  <br>
 */
@Service
public class DelayJobHanlerImpl extends AbstractDelayBucketHandler implements SmartLifecycle {

    @Autowired
    private volatile RedissonClient redissonClient;

    @Override
    public RedissonClient getRedissonClient() {
        return redissonClient;
    }

    private Thread delayThread;



    @Override
    public void start() {
        delayThread = new Thread(this);
        delayThread.start();
    }

    @Override
    public void stop() {
        if (delayThread!=null){
            delayThread.interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return null!=delayThread;
    }


}
