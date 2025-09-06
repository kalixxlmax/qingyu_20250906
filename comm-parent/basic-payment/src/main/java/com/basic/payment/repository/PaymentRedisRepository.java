package com.basic.payment.repository;

import com.basic.redisson.AbstractRedisson;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Set;

@Repository
public class PaymentRedisRepository extends AbstractRedisson {

    final  String PAY_CHANNEL_REDISKEY="pay:channelIds";

    @Autowired
    private RedissonClient redissonClient;

    @Override
    public RedissonClient getRedissonClient() {
        return redissonClient;
    }

    public Set<String> queryPaymentChannel() {
        RSet<String> rSet = getSet(PAY_CHANNEL_REDISKEY);
        return rSet.readAll();
    }

    public void deletePaymentChannel() {
       deleteBucket(PAY_CHANNEL_REDISKEY);
    }
    public void savePaymentChannel(Set<String> list) {
        RSet<String> rSet = getSet(PAY_CHANNEL_REDISKEY);
        rSet.clear();
        rSet.addAll(list);

    }

    public boolean paymentChannelIsEnable(String channelId) {

        RSet<String> rSet = getSet(PAY_CHANNEL_REDISKEY);
        return rSet.contains(channelId);
    }




}
