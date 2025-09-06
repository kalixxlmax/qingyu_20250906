package com.basic.payment.config;

import com.alibaba.fastjson.JSON;
import com.alipay.api.AlipayApiException;
import com.alipay.api.AlipayClient;
import com.alipay.api.DefaultAlipayClient;
import com.alipay.api.internal.util.AlipayUtils;
import com.github.binarywang.wxpay.config.WxPayConfig;
import com.github.binarywang.wxpay.service.WxPayService;
import com.github.binarywang.wxpay.service.impl.WxPayServiceImpl;
import com.basic.payment.channel.alipay.AliPayProperties;
import com.basic.payment.channel.alipay.util.AliPayUtil;
import com.basic.payment.channel.wechat.WxPayProperties;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({WxPayProperties.class,AliPayProperties.class})
public class PayConfiguration {


    @Autowired(required = false)
    //@Qualifier(value = "wxPayProperties")
    private WxPayProperties wxPayProperties;
    @Autowired
    private AliPayProperties aliPayProperties;

    @Bean
    public AlipayClient alipayClient(){

        System.out.println(" init aliPayConfig "+aliPayProperties.toString());
        AlipayClient alipayClient=null;
        try {
             alipayClient= new DefaultAlipayClient(AliPayUtil.ALIPAY_GATEWAY,
                     aliPayProperties.getAppid(),aliPayProperties.getApp_private_key(),
                     "json", AliPayUtil.CHARSET, aliPayProperties.getAlipay_public_key(), "RSA2");
            return alipayClient;
        } catch (Exception e) {
            e.printStackTrace();
            return alipayClient;
        }
    }

    @Bean
    public WxPayConfig wxConfig() {
        System.out.println(" init wxPayConfig "+wxPayProperties.toString());


        WxPayConfig payConfig = new WxPayConfig();
        payConfig.setAppId(StringUtils.trimToNull(wxPayProperties.getAppid()));
        payConfig.setMchId(StringUtils.trimToNull(wxPayProperties.getMchid()));
        payConfig.setKeyPath(StringUtils.trimToNull(wxPayProperties.getPkPath()));
        payConfig.setNotifyUrl(wxPayProperties.getCallBackUrl());
        payConfig.setMchKey(wxPayProperties.getApiKey());
        // 可以指定是否使用沙箱环境
        payConfig.setUseSandboxEnv(false);

        return payConfig;
    }

    @Bean
    public WxPayService wxService() {

        WxPayConfig payConfig =wxConfig();
        WxPayService wxPayService = new WxPayServiceImpl();
        wxPayService.setConfig(payConfig);
        return wxPayService;
    }

}
