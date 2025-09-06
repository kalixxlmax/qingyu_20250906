package com.basic.translate.entity;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "translator.sogou")
public class SogouTranslator {
    /**
     * 搜狗翻译加密盐值
     */
    private String salt;

    /**
     * 搜狗翻译URL
     */
    private String url;

    /**
     * 是否开启搜狗翻译
     */
    private boolean isEnabled;

    /**
     * 搜狗翻译 appId
     */
    private String appid;

    /**
     * 搜狗翻译秘钥
     */
    private String secretKey;

}
