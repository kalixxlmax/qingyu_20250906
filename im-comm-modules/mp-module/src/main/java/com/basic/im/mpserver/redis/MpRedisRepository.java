package com.basic.im.mpserver.redis;

import com.basic.redisson.AbstractRedisson;
import com.basic.utils.StringUtil;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2020/4/26
 **/
@Component
public class MpRedisRepository extends AbstractRedisson {

    @Autowired
    private RedissonClient redissonClient;

    @Override
    public RedissonClient getRedissonClient() {
        return redissonClient;
    }


    private static final String MP_CONFIG_KEY="mp:config:%s";

    /**
     * @Description 设置全局配置
     * @Date 10:03 2020/4/26
     **/
    public void setMpConfig(String config){
        if(!StringUtil.isEmpty(config)){
            RBucket<String> bucket = redissonClient.getBucket(MP_CONFIG_KEY);
            bucket.set(config);
        }
    }


    /**
     * @Description 获取全局配置
     * @Date 10:04 2020/4/26
     **/
    public String getMpConfig(){
        RBucket<String> bucket = redissonClient.getBucket(MP_CONFIG_KEY);
        return bucket.get();
    }
}
