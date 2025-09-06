package com.basic.translate.entity;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "translator.youdao")
public class YoudaoTranslator {

    /**
     * 是否开启有道翻译
     */
    private boolean isEnabled;

    /**
     * 有道翻译appId
     */
    private String appid;

    /**
     * 有道翻译秘钥
     */
    private String secretKey;

    /**
     * 有道翻译URL
     */
    private String url;
}
