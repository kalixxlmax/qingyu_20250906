package com.basic.translate.service.impl;


import com.basic.common.response.JSONBaseMessage;
import com.basic.translate.entity.YoudaoTranslator;
import com.basic.translate.factory.TranslateServiceFactory;
import com.basic.translate.service.TranslateService;
import com.basic.translate.util.CommonUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.Consts;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

@Component
public class YoudaoTranslateService implements TranslateService, InitializingBean {

    @Resource(name = "youdaoTranslator")
    private YoudaoTranslator youdaoTranslator;

    @Override
    public JSONBaseMessage getTranslatedContent(String content, String from, String to,String messageId) {
        if(!youdaoTranslator.isEnabled()) {
            return JSONBaseMessage.failure("youdao api is not enabled");
        }
        try {
            return sendRequest(initHttpPost(content, from,to), "errorCode", "translation", "",messageId);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return JSONBaseMessage.failure(null);
    }


    private HttpPost initHttpPost(String content, String from, String to) {
        HttpPost httpPost = new HttpPost(youdaoTranslator.getUrl());
        ArrayList<BasicNameValuePair> parameters = new ArrayList<BasicNameValuePair>() {
            {
                String salt = UUID.randomUUID().toString();
                String curTime = String.valueOf(System.currentTimeMillis() / 1000);
                String appId = youdaoTranslator.getAppid();
                String secretKey = youdaoTranslator.getSecretKey();
                add(new BasicNameValuePair("from", from));
                add(new BasicNameValuePair("to", to));
                add(new BasicNameValuePair("signType", "v3"));
                add(new BasicNameValuePair("curtime", curTime));
                add(new BasicNameValuePair("q", content));
                add(new BasicNameValuePair("appKey", appId));
                add(new BasicNameValuePair("salt", salt));
                add(new BasicNameValuePair("sign", DigestUtils.sha256Hex(appId + CommonUtils.truncateContent(content) + salt + curTime +
                        secretKey)));
            }
        };
        httpPost.setEntity(new UrlEncodedFormEntity(parameters, Consts.UTF_8));
        httpPost.addHeader("content-type", "application/x-www-form-urlencoded");
        httpPost.addHeader("accept", "application/json");
        return httpPost;
    }

    static {
        ERROR_MESSAGE.put(1010, "缺少必填的参数");
        ERROR_MESSAGE.put(102, "不支持的语言类型");
        ERROR_MESSAGE.put(103, "文本过长");
        ERROR_MESSAGE.put(104, "无效PID");
        ERROR_MESSAGE.put(105, "试用Pid限额已满");
        ERROR_MESSAGE.put(106, "pid小语种限额已满");
        ERROR_MESSAGE.put(107, "Pid请求流量过高");
        ERROR_MESSAGE.put(108, "随机数不存在");
        ERROR_MESSAGE.put(109, "签名不存在");
        ERROR_MESSAGE.put(110, "签名不正确");
        ERROR_MESSAGE.put(111, "文本不存在");
        ERROR_MESSAGE.put(112, "内部服务错误");
        ERROR_MESSAGE.put(113, "账户余额不足");
        ERROR_MESSAGE.put(201, "接口请求过快");
        ERROR_MESSAGE.put(202, "签名检验失败");
        ERROR_MESSAGE.put(203, "访问IP地址不在可访问IP列表");
        ERROR_MESSAGE.put(205, "请求的接口与应用的平台类型不一致");
        ERROR_MESSAGE.put(206, "因为时间戳无效导致签名校验失败");
        ERROR_MESSAGE.put(207, "重放请求");
        ERROR_MESSAGE.put(301, "辞典查询失败");
        ERROR_MESSAGE.put(302, "翻译查询失败");
        ERROR_MESSAGE.put(303, "服务端的其它异常");
        ERROR_MESSAGE.put(304, "会话闲置太久超时");
        ERROR_MESSAGE.put(401, "账户已经欠费停");
        ERROR_MESSAGE.put(402, "长请求过于频繁，请稍后访问");
        ERROR_MESSAGE.put(411, "访问频率受限,请稍后访问");
        ERROR_MESSAGE.put(412, "长请求过于频繁，请稍后访问");
        ERROR_MESSAGE.put(1411, "访问频率受限");
        ERROR_MESSAGE.put(2201, "超过最大识别字节数");
        ERROR_MESSAGE.put(2301, "服务的异常");
        ERROR_MESSAGE.put(2411, "访问频率受限,请稍后访问");
        ERROR_MESSAGE.put(2412, "超过最大请求字符数");
        ERROR_MESSAGE.put(9303, "服务器内部错误");
        ERROR_MESSAGE.put(17005, "服务调用失败");
    }
    @Override
    public void afterPropertiesSet() throws Exception {
        TranslateServiceFactory.register(TranslateServiceFactory.YOU_DAO_TYPE,this);
    }

}
