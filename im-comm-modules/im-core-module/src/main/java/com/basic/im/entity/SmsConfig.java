package com.basic.im.entity;

import io.swagger.annotations.ApiModel;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @Description: 短信配置
 * @Author xie yuan yang
 * @Date 2020/5/29
 **/
@ApiModel("短信配置实体")
@Data
@Document("smsConfig")
public class SmsConfig {
    @Id
    private long id = 10000;
    /**
     * 是否发送短信验证码
     **/
    private int openSMS = 1;

    /**
     * 天天国际短信服务
     **/
    private String host = "m.isms360.com";
    private int port = 8085;
    private String api = "/mt/MT3.ashx";
    /**
     * 短信平台用户名
     **/
    private String username;
    /**
     * 短信平台密码
     **/
    private String password;
    /**
     * 中文短信模板
     **/
    private String templateChineseSMS;
    /**
     * 英文短信模板
     **/
    private String templateEnglishSMS;




    /**
     * 阿里云短信服务
     **/

    /**
     * 云通信短信API产品,无需替换
     **/
    private String product = "Dysmsapi";
    /**
     * 产品域名,无需替换
     **/
    private String domain = "dysmsapi.aliyuncs.com";
    /**
     * AK key
     **/
    private String accesskeyid;
    /**
     * AK value
     **/
    private String accesskeysecret;
    /**
     * 短信签名
     **/
    private String signname;
    /**
     * 中文短信模板标识
     **/
    private String chinase_templetecode;
    /**
     * 国际短信模板
     **/
    private String international_templetecode;
    /**
     * 云钱包开户短信验证码模板标识
     **/
    private String cloudWalletVerification;
    /**
     * 云钱包开户通知短信模板
     **/
    private String cloudWalletNotification;

    // 阿里云其他业务属性字段
    /**
     * 实人认证服务的业务场景标识
     **/
    private String bizType;


    private Integer smsSendCount=10;        // 单个IP指定时间区间内超过请求次数需要验证码
    private Integer smsSendTime=60;         // 单个IP请求时间
    private Integer smsSendBlackCount = 20; // 单个IP在指定时间区间内请求次数达到后拉黑
    private Integer smsSendBlackTime = 60;  // 拉黑时间

    private Integer allSmsSendCount=100;     // 短信验证码指定时间区间内发送超出指定数量需要验证码
    private Integer allSmsSendTime=60;       // 短信验证码验证区间

}
