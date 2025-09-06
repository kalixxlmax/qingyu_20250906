/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2024. All rights reserved.
 */
package com.basic.im.push.service.hwpush.util;

import com.basic.im.push.service.hwpush.messaging.HuaweiApp;
import com.basic.im.push.service.hwpush.messaging.HuaweiCredential;
import com.basic.im.push.service.hwpush.messaging.HuaweiOption;

public class InitAppUtils {

    /*public static HuaweiApp initializeApp() {
        String appId = ResourceBundle.getBundle("url").getString("appid");
        String appSecret = ResourceBundle.getBundle("url").getString("appsecret");
        // Create HuaweiCredential
        // This appId and appSecret come from Huawei Developer Alliance
        return initializeApp(appId, appSecret);
    }*/

    public static HuaweiApp initializeApp(String appId, String appSecret, String tokenUrl, String pushUrl, String packageName) {
        HuaweiCredential credential = HuaweiCredential.builder()
                .setAppId(appId)
                .setAppSecret(appSecret)
                .setTokenUrl(tokenUrl)
                .setPushUrl(pushUrl)
                .setPackageName(packageName)
                .build();

        // Create HuaweiOption
        HuaweiOption option = HuaweiOption.builder()
                .setCredential(credential)
                .build();

        // Initialize HuaweiApp
        return HuaweiApp.initializeApp(option);
    }
}
