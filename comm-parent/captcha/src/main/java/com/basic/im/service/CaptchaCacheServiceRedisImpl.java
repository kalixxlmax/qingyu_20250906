package com.basic.im.service;

import com.anji.captcha.service.CaptchaCacheService;
import com.google.auto.service.AutoService;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.TimeUnit;

@AutoService(CaptchaCacheService.class)
public class CaptchaCacheServiceRedisImpl implements CaptchaCacheService {
    @Override
    public String type() {
        return "redis";
    }

    @Autowired
    private RedissonClient redissonClient;

    @Override
    public void set(String key, String value, long expiresInSeconds) {
        RBucket<String> bucket = redissonClient.getBucket(key);
        bucket.set(value);
        bucket.expire(expiresInSeconds,TimeUnit.SECONDS);
    }

    @Override
    public boolean exists(String key) {
        RBucket<String> bucket = redissonClient.getBucket(key);
        return bucket.isExists();
    }

    @Override
    public void delete(String key) {
        redissonClient.getBucket(key).delete();
    }

    @Override
    public String get(String key) {
        return (String) redissonClient.getBucket(key).get();
    }
}
