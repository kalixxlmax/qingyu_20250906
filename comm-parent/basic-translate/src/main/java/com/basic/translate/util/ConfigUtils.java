package com.basic.translate.util;

import com.basic.translate.entity.TranslatorConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
public class ConfigUtils {
    @Autowired
    private TranslatorConfig config;

    private static TranslatorConfig translatorConfig;

    public static TranslatorConfig getSystemConfig() {
        return translatorConfig;
    }


    @PostConstruct
    public void initBean(){
        translatorConfig=config;
    }
}
