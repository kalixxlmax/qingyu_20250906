package com.basic.im.push.service.hwpush.examples;

import com.google.common.collect.Maps;
import com.basic.im.entity.PushConfig;
import com.basic.im.push.service.PushServiceUtils;
import com.basic.im.push.service.hwpush.messaging.HuaweiApp;
import com.basic.im.push.service.hwpush.util.InitAppUtils;

import java.util.Map;

/**
 * 华为透传推送
 * @version V1.0
 *  
 * @date 2020/4/13 17:20
 */
public abstract class SendHWMessage extends PushServiceUtils {


    public static Map<String, HuaweiApp> huaweiAppMap;// 根据包名获取华为推送实例

    public static HuaweiApp getHuaweiApp(String packageName){
        HuaweiApp huaweiApp;
        PushConfig.AndroidPush pushConfig = getPushConfig(packageName);
        if(null==pushConfig){
            return null;
        }
        if(null != huaweiAppMap){
            huaweiApp = huaweiAppMap.get(packageName);
            if(null != huaweiApp){
                return huaweiApp;
            }else{
                huaweiApp = HuaweiApp.getInstance(pushConfig.getHwAppId());
                if(null==huaweiApp) {
                huaweiApp = InitAppUtils.initializeApp(pushConfig.getHwAppId(),pushConfig.getHwAppSecret(),pushConfig.getHwTokenUrl(),pushConfig.getHwApiUrl(),packageName);
                huaweiAppMap.put(packageName,huaweiApp);
                }
                return huaweiApp;
            }
        }else{
            huaweiApp = HuaweiApp.getInstance(pushConfig.getHwAppId());
            if(null==huaweiApp) {
            huaweiApp = InitAppUtils.initializeApp(pushConfig.getHwAppId(),pushConfig.getHwAppSecret(),pushConfig.getHwTokenUrl(),pushConfig.getHwApiUrl(),packageName);
            huaweiAppMap = Maps.newConcurrentMap();
            huaweiAppMap.put(packageName,huaweiApp);
            }
            return huaweiApp;
        }
    }

    protected static HuaweiApp getAPPInfo(String packageName, boolean initFlag){
        if (initFlag) {
            huaweiAppMap = Maps.newConcurrentMap();
        }
        HuaweiApp huaweiApp;
        PushConfig.AndroidPush pushConfig = getPushConfig(packageName);
        if(null == getHuaweiApp(packageName)){
            huaweiApp = HuaweiApp.getInstance(pushConfig.getHwAppId());
            if(null==huaweiApp) {
                huaweiApp = InitAppUtils.initializeApp(pushConfig.getHwAppId(), pushConfig.getHwAppSecret(), pushConfig.getHwTokenUrl(), pushConfig.getHwApiUrl(), packageName);
            }
        }else{
                huaweiApp = getHuaweiApp(packageName);
        }
        return huaweiApp;
    }
}
