package com.basic.im.repository;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.entity.*;
import com.basic.redisson.AbstractRedisson;
import com.basic.utils.StringUtil;
import org.redisson.api.RBucket;
import org.redisson.api.RKeys;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Repository
public class CoreRedisRepository extends AbstractRedisson {

    @Autowired
    private RedissonClient redissonClient;


    @Override
    public RedissonClient getRedissonClient() {
        return redissonClient;
    }


    public static final String GET_CONFIG = "app:config";
    public static final String GET_CLIENTCONFIG = "clientConfig";
    public static final String GET_PAYCONFIG = "payConfig";
    public static final String GET_SMSCONFIG = "smsconfig";

    // 多包名下的安卓推送
    public static final String GET_ANDROID_PUSHCONFIG = "impush:pushconfig:android:%s";

    // 多包名下的IOS推送
    public static final String GET_IOS_PUSHCONFIG = "impush:pushconfig:ios:%s";

    // 安卓的华为推送初始化access_token
    public static final String HW_PUSH_ACCESSTOKEN = "impush:hw:%s";

     // 安卓的VIVO推送authToken
    public static final String VIVO_PUSHTOKEN = "impush:vivo:%s";

    // 推送配置修改标识
    public static final String PUSH_UPDATE_FLAG = "impush:update:flag";

    public void setConfig(Config config) {
        setBucket(GET_CONFIG, config.toString());
    }


    public  Config getConfig() {
        RBucket<String> config=redissonClient.getBucket(GET_CONFIG);
        if (config!=null && StrUtil.isNotBlank(config.get())){
            Config getConfig = JSONObject.parseObject(config.get(), Config.class);
            try {
                JSONObject jsonObject = JSONObject.parseObject(config.get());
                JSONArray jsonArray = jsonObject.getJSONObject("systemApiConfig").getJSONArray("requestApiList");
                List<String> stringList = JSONObject.parseArray(jsonArray.toJSONString(), String.class);
                getConfig.getSystemApiConfig().setRequestApiList(stringList);
            }catch (Exception e){
            }
            return getConfig;
        }
        return null;
    }
    public  ClientConfig getClientConfig() {
        RBucket<String> config=redissonClient.getBucket(GET_CLIENTCONFIG);
        if (config!=null && StrUtil.isNotBlank(config.get())){
            return JSONObject.parseObject(config.get(), ClientConfig.class);
        }
        return null;
    }

    public  void setClientConfig(ClientConfig clientConfig) {
         setBucket(GET_CLIENTCONFIG,JSONObject.toJSONString(clientConfig));
    }

    public void setPayConfig(PayConfig payConfig){
        setBucket(GET_PAYCONFIG,JSONObject.toJSONString(payConfig));
    }

    public PayConfig getPayConfig(){
        RBucket<String> config = redissonClient.getBucket(GET_PAYCONFIG);
        if (StrUtil.isNotBlank(config.get())){
            return JSONObject.parseObject(config.get(),PayConfig.class);
        }
        return null;
    }

    public void setSmsConfig(SmsConfig smsConfig){
        setBucket(GET_SMSCONFIG,JSON.toJSONString(smsConfig));
    }

    public SmsConfig getSmsConfig(){
        String config = getBucket(String.class,GET_SMSCONFIG);
        return StringUtil.isEmpty(config) ? null : JSON.parseObject(config,SmsConfig.class);
    }

    /**
     * 去掉redis定义有效期的随机数
     * 用于严格保证数据有效期限 比如：华为推送的token有效期
     */
    public void setBucketImpl(String key, Object obj, long time, TimeUnit unit){
        RBucket<Object> bucket = getRedissonClient().getBucket(key);
        bucket.set(obj,time,unit);
    }

    /**
     * 保存华为推送初始化的access_token
     * expireIn 单位秒
     */
    public void saveHWPushToken(String access_token,String packageName) {
        String key = String.format(HW_PUSH_ACCESSTOKEN, packageName);
        setBucketImpl(key, access_token, KConstants.Expire.HOUR, TimeUnit.SECONDS);
    }

    /**
     * 获取华为推送access_token
     */
    public Map<String, String> getHWPushToken(String packageName) {
        String key = String.format(HW_PUSH_ACCESSTOKEN, packageName);
        RBucket<String> value = redissonClient.getBucket(key);
        if (StringUtil.isEmpty(value.get())) {
            return Maps.newConcurrentMap();
        }
        Map tokenMaps = Maps.newConcurrentMap();
        tokenMaps.put("access_token", value.get());
        // 注 :remainTimeToLive同pttl命令单位毫秒，ttl命令单位秒
        tokenMaps.put("expireIn", value.remainTimeToLive());
        return tokenMaps;
    }

    /**
     * 保存vivo推送token
     */
    public void saveVivoToken(String access_token,String packageName) {
        String key = String.format(VIVO_PUSHTOKEN, packageName);
        setBucketImpl(key, access_token, KConstants.Expire.HOUR, TimeUnit.SECONDS);
    }

    public String getVivoPushToken(String packageName) {
        String key = String.format(VIVO_PUSHTOKEN, packageName);
        RBucket<String> value = redissonClient.getBucket(key);
        return value.get();
    }

    /**
     * 更新推送配置，初始化推送信息
     * @param type 0:未修改，1：已修改
     * @return
     */
    public void updatePushFlag(int type) {
        setBucket(PUSH_UPDATE_FLAG, type);
    }

    public Integer getPushFlag() {
        RBucket<Integer> bucket = redissonClient.getBucket(PUSH_UPDATE_FLAG);
        if(null == bucket || null == bucket.get()){
            return 0;
        }
        return bucket.get();
    }

    /**
     * 多包名安卓推送
     * @param
     * @return
     */
    public void setAndroidPushConfig(String packName, PushConfig.AndroidPush androidPush){
        String key = String.format(GET_ANDROID_PUSHCONFIG,packName);
        setBucket(key,JSONObject.toJSONString(androidPush));
    }

    /**
     * 多包名ios推送
     * @param
     * @return
     */
    public void setIOSPushConfig(String packName, PushConfig.IOSPush iosPush) {
        String key = String.format(GET_IOS_PUSHCONFIG, packName);
        setBucket(key, JSONObject.toJSONString(iosPush));
    }

    /**
     * 根据包名获取对应安卓推送配置
     * @param
     * @return
     */
    public PushConfig.AndroidPush getPushAndroidConfig(String packName){
        String key = String.format(GET_ANDROID_PUSHCONFIG,packName);
        String config = getBucket(String.class,key);
        return StringUtil.isEmpty(config) ? null : JSONObject.parseObject(config,PushConfig.AndroidPush.class);
    }

    /**
     * 根据包名获取对应IOS推送配置
     * @param
     * @return
     */
    public PushConfig.IOSPush getPushIosConfig(String packName){
        String key = String.format(GET_IOS_PUSHCONFIG,packName);
        String config = getBucket(String.class,key);
        return StringUtil.isEmpty(config) ? null : JSONObject.parseObject(config,PushConfig.IOSPush.class);
    }

    /**
     *  清除推送配置
     **/
    public void cleanPushConfig() {
        RKeys keys = redissonClient.getKeys();
        keys.deleteByPattern(String.format(GET_ANDROID_PUSHCONFIG, "*"));
        keys.deleteByPattern(String.format(GET_IOS_PUSHCONFIG, "*"));
    }
    public static String SIGN_POLICY_AWARD_KEY = "sign_policy_award";
    public Integer getSignPolicyAward(Long day) {
        return (Integer) redissonClient.getMap(SIGN_POLICY_AWARD_KEY).get(day);
    }

}
