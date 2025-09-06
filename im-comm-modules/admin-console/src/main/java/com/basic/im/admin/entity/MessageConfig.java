package com.basic.im.admin.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @Description: TODO (短信配置)
 * @Author xie yuan yang
 * @Date 2020/5/25
 **/
@Data
@Document("message_config")
public class MessageConfig {
    private @Id
    long id = 10000;

    // 是否发送短信验证码
    private int openSMS = 1;

    /* 天天国际短信服务*/
    private String host;

    private int port;

    private String api;

    // 短信平台用户名
    private String username;

    // 短信平台密码
    private String password;

    // 中文短信模板
    private String templateChineseSMS;

    // 英文短信模板
    private String templateEnglishSMS;

    /* 阿里云短信服务*/
    // 云通信短信API产品,无需替换
    private String product;

    // 产品域名,无需替换
    private String domain;

    // AK key
    private String accesskeyid;

    // AK value
    private String accesskeysecret;

    // 短信签名
    private String signname;

    // 中文短信模板标识
    private String chinase_templetecode;

    // 国际短信模板
    private String international_templetecode;

    // 云钱包开户短信验证码模板标识
    private String cloudWalletVerification;

    // 云钱包开户通知短信模板
    private String cloudWalletNotification;


    // 阿里云其他业务属性字段

    // 实人认证服务的业务场景标识
    private String bizType;
}
