package com.basic.im.repository;

import com.basic.redisson.AbstractRedisson;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;

@Service
public class TaskRepository extends AbstractRedisson {

    private final RedissonClient redissonClient;

    public TaskRepository(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    @Override
    public RedissonClient getRedissonClient() {
        return redissonClient;
    }


}
