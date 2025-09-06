package com.basic.im.sms.model;

public class SmsConfigModel {

    private int openSMS = 1;// 是否发送短信验证码
    // 天天国际短信服务
    private String host;
    private int port;
    private String api;
    private String username;// 短信平台用户名
    private String password;// 短信平台密码
    private String templateChineseSMS;// 中文短信模板
    private String templateEnglishSMS;// 英文短信模板
    // 阿里云短信服务
    private String product;// 云通信短信API产品,无需替换
    private String domain;// 产品域名,无需替换
    private String accesskeyid;// AK key
    private String accesskeysecret;// AK value
    private String signname;// 短信签名
    private String chinase_templetecode;// 中文短信模板标识
    private String international_templetecode;// 国际短信模板
    private String cloudWalletVerification;// 云钱包开户短信验证码模板标识
    private String cloudWalletNotification;// 云钱包开户通知短信模板

    // 阿里云其他业务属性字段
    private String bizType; //实人认证服务的业务场景标识


   /* public void setSmsConfig(SmsConfigModel smsConfig){
        this.openSMS = smsConfig.getOpenSMS();
        this.host = smsConfig.getHost();
        this.port = smsConfig.getPort();
        this.username = smsConfig.getUsername();
        this.password = smsConfig.getPassword();
        this.templateChineseSMS = smsConfig.getTemplateChineseSMS();
        this.templateEnglishSMS = smsConfig.getTemplateEnglishSMS();
        this.product = smsConfig.getProduct();
        this.domain = smsConfig.getDomain();
        this.accesskeyid = smsConfig.getAccesskeyid();
        this.accesskeysecret = smsConfig.getAccesskeysecret();
        this.signname = smsConfig.getSignname();
        this.chinase_templetecode = smsConfig.getChinase_templetecode();
        this.international_templetecode = smsConfig.getInternational_templetecode();
        this.cloudWalletVerification = smsConfig.getCloudWalletVerification();
        this.cloudWalletNotification = smsConfig.getCloudWalletNotification();
    }*/
}
