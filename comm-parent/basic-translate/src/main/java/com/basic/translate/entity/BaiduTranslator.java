package com.basic.translate.entity;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "translator.baidu")
public class BaiduTranslator {

    /**
     * 是否开启Baidu翻译功能
     */
    private boolean isEnabled;

    /**
     * 百度翻译 appId
     */
    private String appid;

    /**
     * 百度翻译秘钥
     */
    private String secretKey;

    /**
     * 百度翻译URL
     */
    private String url;

    /**
     * 百度翻译加密盐值
     */
    private int salt;

}
