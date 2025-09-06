package com.basic.im.live.service.impl;

import com.basic.redisson.AbstractRedisson;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

@Service
public class LiveRoomRedisRepository extends AbstractRedisson {
    @Autowired(required=false)
    private RedissonClient redissonClient;

    @Override
    public RedissonClient getRedissonClient() {
        return redissonClient;
    }


    /**
     * 群组开启直播间锁，避免同时开启多个直播间
     */
    public static final String LIVE_START="live:start:roomId:%s";

    /**
     * 群组是否开启成功
     */
    public static final String LIVE_START_FLAG="live:start:flag:roomId:%s";


    /**
     * 取得开启直播间 key
     * @param liveRoomId 用户群组Id
     */
    public String getLiveStart(String liveRoomId){
        return String.format(LIVE_START,liveRoomId);
    }

    /**
     * 设置直播间是否创建成功标志
     */
    public void setLiveStartFlag(String liveRoomId,Long status){
        RAtomicLong atomicLong = redissonClient.getAtomicLong(String.format(LIVE_START_FLAG, liveRoomId));
        atomicLong.set(status);
    }

    /**
     * 取得直播间是否创建成功标志
     */
    public boolean getLiveStartFlag(String liveRoomId){
        RAtomicLong atomicLong = redissonClient.getAtomicLong(String.format(LIVE_START_FLAG, liveRoomId));
        if (atomicLong!=null){
            return atomicLong.get() == 1;
        }
        return false;
    }
}
