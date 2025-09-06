package com.basic.im.admin.entity;

import com.basic.im.entity.PushConfig;
import lombok.Data;

/**
 * @ClassName PushConfigModelVO
 * @Author xie yuan yuang
 * @date 2020.08.03 16:48
 * @Description
 */
@Data
public class PushConfigModelVO {

    private int id;

    private String packageName;// 安卓包名

    private String appStoreAppId;// appStore 版本App包名 (IOS个人版)

    private String betaAppId;// 企业版 app 包名(IOS企业版)


    // 小米
    private String xmAppSecret;

    // 华为
    private String hwAppSecret;
    private String hwAppId;
    private String hwTokenUrl;
    private String hwApiUrl;
    private String hwIconUrl;
    // 是否使用单独部署华为推送   1是    0：否
    private byte isOpen;

    // 百度
   /* private String bdAppStoreAppId;
    private String bdAppStoreAppKey;
    private String bdAppStoreSecretKey;
    private String[] bdAppKey;
    private String bdRestUrl;
    private String[] bdSecretKey;*/

    // 极光
    private String jPushAppKey;
    private String jPushMasterSecret;

    // google FCM
    private String fcmDataBaseUrl;
    private String fcmKeyJson;

    // 魅族
    private String mzAppSecret;
    private long mzAppId;

    // VIVO
    private int vivoAppId;
    private String vivoAppKey;
    private String vivoAppSecret;

    // OPPO
    private String oppoAppKey;
    private String oppoMasterSecret;




    // 企业版 测试版 apns 推送证书
    private String betaApnsPk;

   /* // appStore 版本 App 包名
    private String appStoreAppId;*/

    // appStore apns 推送证书
    private String appStoreApnsPk;

    // voip 证书
    private String voipPk;

    // 证书 密码
    private String pkPassword;

    // 沙箱模式
    private byte isApnsSandbox = 0;

    // 调试模式  打印 log
    private byte isDebug = 0;

    /*// 企业版 app 包名
    private String betaAppId;*/

    public PushConfig byPushConfig(PushConfigModelVO pushConfigModelVO){
        PushConfig data = new PushConfig();
        data.setId(pushConfigModelVO.getId());
        data.setPackageName(pushConfigModelVO.getPackageName());
        data.setAppStoreAppId(pushConfigModelVO.getAppStoreAppId());
        data.setBetaAppId(pushConfigModelVO.getBetaAppId());

        PushConfig.AndroidPush androidPush = new PushConfig.AndroidPush();
        androidPush.setXmAppSecret(pushConfigModelVO.getXmAppSecret());
        androidPush.setHwAppSecret(pushConfigModelVO.getHwAppSecret());
        androidPush.setHwAppId(pushConfigModelVO.getHwAppId());
        androidPush.setHwApiUrl(pushConfigModelVO.getHwApiUrl());
        androidPush.setHwIconUrl(pushConfigModelVO.getHwIconUrl());
        androidPush.setHwTokenUrl(pushConfigModelVO.getHwTokenUrl());
        androidPush.setIsOpen(pushConfigModelVO.getIsOpen());
        androidPush.setJPushAppKey(pushConfigModelVO.getJPushAppKey());
        androidPush.setJPushMasterSecret(pushConfigModelVO.getJPushMasterSecret());
        androidPush.setFcmDataBaseUrl(pushConfigModelVO.getFcmDataBaseUrl());
        androidPush.setFcmKeyJson(pushConfigModelVO.getFcmKeyJson());
        androidPush.setMzAppId(pushConfigModelVO.getMzAppId());
        androidPush.setMzAppSecret(pushConfigModelVO.getMzAppSecret());
        androidPush.setVivoAppId(pushConfigModelVO.getVivoAppId());
        androidPush.setVivoAppKey(pushConfigModelVO.getVivoAppKey());
        androidPush.setVivoAppSecret(pushConfigModelVO.getVivoAppSecret());
        androidPush.setOppoAppKey(pushConfigModelVO.getOppoAppKey());
        androidPush.setOppoMasterSecret(pushConfigModelVO.getOppoMasterSecret());
        data.setAndroidPush(androidPush);


        PushConfig.IOSPush iosPush = new PushConfig.IOSPush();
        iosPush.setAppStoreAppId(pushConfigModelVO.getAppStoreAppId());
        iosPush.setBetaAppId(pushConfigModelVO.getBetaAppId());

        iosPush.setBetaApnsPk(pushConfigModelVO.getBetaApnsPk());
        iosPush.setAppStoreApnsPk(pushConfigModelVO.getAppStoreApnsPk());
        iosPush.setPkPassword(pushConfigModelVO.getPkPassword());
        iosPush.setIsApnsSandbox(pushConfigModelVO.getIsApnsSandbox());
        iosPush.setIsDebug(pushConfigModelVO.getIsDebug());
        data.setIosPush(iosPush);

        return data;
    }
}
