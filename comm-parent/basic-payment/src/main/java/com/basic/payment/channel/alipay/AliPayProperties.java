package com.basic.payment.channel.alipay;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

@Data
@Configuration
@ConfigurationProperties(prefix = "alipayconfig")
public  class AliPayProperties {
    // 支付宝认证应用Id
    private String appid;
    // 应用私钥
    private String app_private_key;
    // 字符编码格式
    private String charset;
    // 支付宝公钥
    private String alipay_public_key;
    // 支付宝回调地址
    private String callBackUrl;
    // 账户pid
    private String pid;

    // 请求网关地址
    private String url = "https://openapi.alipay.com/gateway.do";


    // 是否沙箱环境,1:沙箱,0:正式环境
    private byte isSandbox = 0;


    // 编码
    public static String CHARSET = "UTF-8";

    // RSA2
    public static String SIGNTYPE = "RSA2";


    // 返回格式
    public static String FORMAT = "json";

    /**
     * 初始化支付宝配置
     * @param configParam
     * @return
     */
    public AliPayProperties init(String configParam) {
        Assert.notNull(configParam, "init alipay config error");
        JSONObject paramObj = JSON.parseObject(configParam);
        this.setAppid(paramObj.getString("appid"));
        this.setApp_private_key(paramObj.getString("app_private_key"));
        this.setAlipay_public_key(paramObj.getString("alipay_public_key"));
        this.setIsSandbox(paramObj.getByteValue("isSandbox"));
        if(this.getIsSandbox() == 1){
            this.setUrl("https://openapi.alipaydev.com/gateway.do");
        }
        return this;
    }
}
