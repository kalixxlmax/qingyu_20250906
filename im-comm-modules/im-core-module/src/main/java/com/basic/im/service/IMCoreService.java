package com.basic.im.service;

import com.basic.im.comm.utils.StringUtil;
import com.basic.im.config.AppConfig;
import com.basic.im.entity.*;
import com.basic.im.repository.CoreRedisRepository;
import com.basic.im.repository.IMCoreRepository;
import com.basic.im.utils.SKBeanUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class IMCoreService {

    @Autowired
    private IMCoreRepository imCoreRepository;


    @Autowired
    private CoreRedisRepository coreRedisRepository;


    @Autowired
    private AppConfig appConfig;


    public Config getConfig() {
        Config config = null;
        try {
            config= coreRedisRepository.getConfig();
            if(null==config){
                config = imCoreRepository.getConfig();
                if(null==config) {
                    config=new Config();
                }
                coreRedisRepository.setConfig(config);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            config = imCoreRepository.getConfig();
        }

        return config;
    }
    public void setConfig(Config dest){
        imCoreRepository.setConfig(dest);
        coreRedisRepository.setConfig(dest);
    }

    public void setClientConfig(ClientConfig clientconfig){
        imCoreRepository.setClientConfig(clientconfig);
        coreRedisRepository.setClientConfig(clientconfig);
    }

    public ClientConfig getClientConfig() {
        ClientConfig clientconfig = null;
        try {
            clientconfig=coreRedisRepository.getClientConfig();
            if(null==clientconfig){
                clientconfig = imCoreRepository.getClientConfig();
                if(null==clientconfig) {
                    clientconfig=new ClientConfig();
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            clientconfig = imCoreRepository.getClientConfig();
        }

        return clientconfig;
    }

    public void setPayConfig(PayConfig payConfig){
        imCoreRepository.setPayConfig(payConfig);
        coreRedisRepository.setPayConfig(payConfig);
//        ConstantUtil.setPayConfig(payConfig);
    }

    public PayConfig getPayConfig(){
        PayConfig payConfig = null;
        try {
            payConfig = coreRedisRepository.getPayConfig();
            if(null == payConfig){
                payConfig = imCoreRepository.getPayConfig();
                if(null == payConfig){
                    payConfig = new PayConfig();
                }
            }
        } catch (Exception e){
            log.error(e.getMessage());
            payConfig = imCoreRepository.getPayConfig();
        }
        return payConfig;
    }

    /**
     * 判断后台管理是否添加推送配置
     * @param
     * @return
     */
    public boolean isInitPushConfig(){
        List<PushConfig> pushConfigs = imCoreRepository.initPushConfig();
        return pushConfigs.isEmpty();
    }

    public List<PushConfig> getParentPushConfig(){
        return imCoreRepository.initPushConfig();
    }

    public void savePushConfig(PushConfig pushConfig){
         imCoreRepository.initDBPushConfig(pushConfig);
    }

    /**
     * 多包名下初始化推送配置到redis
     * @param
     * @return
     */
    public boolean initPushConfig(){
        List<PushConfig> pushConfigs = imCoreRepository.initPushConfig();
        if(null == pushConfigs || pushConfigs.isEmpty()){
            return false;
        }
        for (PushConfig pushConfig : pushConfigs) {
            if(null!=pushConfig.getAndroidPush()) {
                // 根据包名为key区分安卓，iOS个人版，ios企业版 存入redis
                setInitAndroidPushConfig(pushConfig.getPackageName(), pushConfig.getAndroidPush());
            }else {
                log.error("请配置Android 第三方推送配置！======》");
                return false;
            }

            if(null!=pushConfig.getIosPush()) {
                if (StringUtil.isEmpty(pushConfig.getIosPush().getAppStoreAppId())) {
                    pushConfig.getIosPush().setAppStoreAppId(pushConfig.getAppStoreAppId());
                }
                if (StringUtil.isEmpty(pushConfig.getIosPush().getBetaAppId())) {
                    pushConfig.getIosPush().setBetaAppId(pushConfig.getBetaAppId());
                }

                if (null!=pushConfig.getAndroidPush() && !StringUtil.isEmpty(pushConfig.getAndroidPush().getJPushAppKey())){
                    pushConfig.getIosPush().setJPushAppKey(pushConfig.getAndroidPush().getJPushAppKey());
                }

                if (null!=pushConfig.getAndroidPush() && !StringUtil.isEmpty(pushConfig.getAndroidPush().getJPushMasterSecret())){
                    pushConfig.getIosPush().setJPushMasterSecret(pushConfig.getAndroidPush().getJPushMasterSecret());
                }

                setInitIOSPushConfig(pushConfig.getAppStoreAppId(),pushConfig.getIosPush());

                setInitIOSPushConfig(pushConfig.getBetaAppId(),pushConfig.getIosPush());
            }else {
                log.error("请配置IOS APNS 第三方推送配置！======》");
            }



        }
        return true;

    }
    /**
     * 初始化多包名安卓推送到redis
     * @param
     * @return
     */
    public void setInitAndroidPushConfig(String packName,PushConfig.AndroidPush androidPush){
        coreRedisRepository.setAndroidPushConfig(packName,androidPush);
    }

    /**
     * 初始化多包名IOS推送到redis
     * @param
     * @return
     */
    public void setInitIOSPushConfig(String packName,PushConfig.IOSPush iosPush){
        coreRedisRepository.setIOSPushConfig(packName,iosPush);
    }

    /**
     * 根据包名获取对应推送配置
     * @param
     * @return
     */
    public PushConfig.AndroidPush getAndroidPushConfigHandler(String packName) {
        return coreRedisRepository.getPushAndroidConfig(packName);
    }

    public PushConfig.IOSPush getIosPushConfigHandler(String packName){
        return coreRedisRepository.getPushIosConfig(packName);
    }

    /**
     * 清除所有推送配置
     */
    public void cleanPushConfig(){
        coreRedisRepository.cleanPushConfig();
    }

    /**
     * 更新推送配置，初始化推送信息
     * @param type 0:未修改，1：已修改
     * @return
     */
    public void updatePushFlag(int type){
        coreRedisRepository.updatePushFlag(type);
    }

    public Integer getPushFlag() {
        return coreRedisRepository.getPushFlag();
    }


    public void setSmsConfig(SmsConfig smsConfig){
        imCoreRepository.setSmsConfig(smsConfig);
        coreRedisRepository.setSmsConfig(smsConfig);
    }

    public SmsConfig getSmsConfig(){
        SmsConfig smsConfig = null;
        try {
            smsConfig = coreRedisRepository.getSmsConfig();
            if(null == smsConfig){
                smsConfig = imCoreRepository.getSmsConfig();
                if(null == smsConfig){
                    smsConfig = new SmsConfig();
                }else{
                    setSmsConfig(smsConfig);
                }
            }
        } catch (Exception e){
            log.error(e.getMessage());
            smsConfig = imCoreRepository.getSmsConfig();
        }
        return smsConfig;
    }

    public AppConfig getAppConfig() {
        return appConfig;
    }


}
