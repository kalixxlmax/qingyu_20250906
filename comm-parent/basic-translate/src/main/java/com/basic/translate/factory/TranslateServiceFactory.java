package com.basic.translate.factory;


import com.basic.translate.service.TranslateService;
import com.basic.translate.util.ConfigUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TranslateServiceFactory {
    public static final String BAI_DU_TYPE="baidu";
    public static final String SOU_GOU_TYPE="sougou";
    public static final String YOU_DAO_TYPE="youdao";


    private static final Map<String, TranslateService> translateServiceMap = new ConcurrentHashMap<>();


    /**
     * 获取工厂UploadFileTemplateService
     */

    public static TranslateService getTranslateService(String type) {
        return translateServiceMap.get(type);
    }
    public static TranslateService getTranslateService() {
        return getTranslateService(ConfigUtils.getSystemConfig().getType());
    }

    /**
     * 工厂注册
     */
    public static void register(String storyType,TranslateService translateService) {
        translateServiceMap.put(storyType, translateService);
    }

}
