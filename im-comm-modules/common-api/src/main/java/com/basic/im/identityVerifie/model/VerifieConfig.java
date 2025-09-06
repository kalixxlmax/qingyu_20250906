package com.basic.im.identityVerifie.model;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author
 * @version V1.0
 * @Description: 身份实名认证
 */

@Data
@Configuration
@ConfigurationProperties(prefix = "verifieconfig")
public class VerifieConfig {

    private String verifieAppCode;// 认证code

    private String verifieUrl;// 认证请求地址

    /*private String verifieAppKey; // 认证key

    private String verifieAppSecret; // 认证Secret*/
}