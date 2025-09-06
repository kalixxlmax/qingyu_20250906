package com.basic.translate.entity;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "translator")
public class TranslatorConfig {
    /**
     * 翻译类型
     */
    private String type;
}
