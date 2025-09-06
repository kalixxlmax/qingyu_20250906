package com.basic.im.sms.repository;

import cn.hutool.core.util.ObjectUtil;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Repository;

import java.util.concurrent.TimeUnit;

@Repository
public class SmsRedisRepository {

    private final RedissonClient redissonClient;

    public SmsRedisRepository(RedissonClient redissonClient) { this.redissonClient = redissonClient; }

    /**
     * 发送验证码手机拉黑
     */
    private final static String SMS_SEND_BLOCK ="sms:send:block:phone:%s";

    /**
     * 手机号码是否通过了检测
     */
    private final static String SMS_SEND_IS_CHECK="sms:send:is:check:phone:%s";

    /**
     * 拉黑手机号码
     * @param phone  手机号码
     * @param time   拉黑时间
     */
    public void addBlock(String phone,long time){
        String key = String.format(SMS_SEND_BLOCK,phone);
        RBucket<Object> bucket = this.redissonClient.getBucket(key);
        bucket.set(phone);
        bucket.expire(time,TimeUnit.SECONDS);
    }

    /**
     * 查询手机号码是否被拉黑
     */
    public boolean isBlack(String phone){
        String key = String.format(SMS_SEND_BLOCK,phone);
        return this.redissonClient.getBucket(key).isExists();
    }

    /**
     * 设置免验证码检测标志
     */
    public void setCheck(String phone){
        String key=String.format(SMS_SEND_IS_CHECK,phone);
        RBucket<Boolean> bucket = this.redissonClient.getBucket(key);
        bucket.set(true);
        bucket.expire(60,TimeUnit.SECONDS);
    }

    /**
     * 取得检测标志并且删除
     */
    public boolean isCheck(String phone){
        String key=String.format(SMS_SEND_IS_CHECK,phone);
        RBucket<Boolean> bucket = this.redissonClient.getBucket(key);
        Boolean flag = bucket.get();
        if (ObjectUtil.isNull(flag)){
            return false;
        }
        bucket.delete();
        return flag;
    }

}
