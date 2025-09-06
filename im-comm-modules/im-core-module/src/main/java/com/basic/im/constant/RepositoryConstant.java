package com.basic.im.constant;

import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * redis常量、通用方法
 */
public class RepositoryConstant {
    /**
     * 公众号发红包榜单
     */
    public static final Map<Integer,String> OFFICIAL_PACKET_KEY_MAP=new HashMap<>(4);

    /**
     * 加好友榜单 key
     */
    public static final Map<Integer,String> ADD_PAY_KEY_MAP=new HashMap<>(4);
    /**
     * 付费加关注榜单 key
     */
    public static final Map<Integer,String> ATTENTION_PAY_KEY_MAP=new HashMap<>(4);

    /**
     * 存储榜单数据过期时间
     */
    public static final String RANKING_EXPIRE_MAP_KEY ="ranking:expire:map:key";
    public static RMap<String,Long> expireMap;

    //  初始化数据
    static {
        OFFICIAL_PACKET_KEY_MAP.put(0,"official:packet:friends:today:%s:%s"); // 公众号发红包 日榜
        OFFICIAL_PACKET_KEY_MAP.put(1,"official:packet:month:%s:%s");         // 公众号发红包 月榜
        OFFICIAL_PACKET_KEY_MAP.put(2,"official:packet:all:%s:%s");           // 公众号发红包 总榜

        ADD_PAY_KEY_MAP.put(0,"pay:friends:today:%s:%s");            // 用户被付费加好友 日榜
        ADD_PAY_KEY_MAP.put(1,"pay:friends:month:%s:%s");            // 用户被付费加好友 月榜
        ADD_PAY_KEY_MAP.put(2,"pay:friends:all:%s:%s");              // 用户被付费加好友 总榜


        ATTENTION_PAY_KEY_MAP.put(0,"pay:attention:today:%s:%s");    // 公众号被付费加关注 日榜
        ATTENTION_PAY_KEY_MAP.put(1,"pay:attention:month:%s:%s");    // 公众号被付费加关注 月榜
        ATTENTION_PAY_KEY_MAP.put(2,"pay:attention:all:%s:%s");    // 公众号被付费加关注 总榜
    }


    /**
     * 设置 指定 key 对应内容的存储时间
     */
    public static Long setRankingExpire(RedissonClient redissonClient, String key){
        return setRankingExpire(redissonClient,key,new Date().getTime());
    }

    public static Long setRankingExpire(RedissonClient redissonClient, String key, Long time){
        expireMap=redissonClient.getMap(RepositoryConstant.RANKING_EXPIRE_MAP_KEY);
        expireMap.put(key,time);
        return time;
    }

    /**
     * 获取对应 key 的对应内容过期时间
     */
    public static Long getRankingExpire(RedissonClient redissonClient, String key){
        // 先从本地获取
        if (expireMap!=null && expireMap.get(key)!=null){
            return expireMap.get(key);
        }
        expireMap=redissonClient.getMap(RepositoryConstant.RANKING_EXPIRE_MAP_KEY);
        return null == expireMap.get(key) ? 0l : expireMap.get(key);
    }
}
