package com.basic.translate.service.impl;

import com.basic.common.response.JSONBaseMessage;
import com.basic.commons.constants.CommConstants;
import com.basic.translate.entity.SogouTranslator;
import com.basic.translate.factory.TranslateServiceFactory;
import com.basic.translate.service.TranslateService;
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

@Component
public class SogouTranslateService implements TranslateService, InitializingBean {

    @Resource(name = "sogouTranslator")
    private SogouTranslator sogouTranslator;


    @Override
    public JSONBaseMessage getTranslatedContent(String content, String from, String to,String messageId) {
        if(!sogouTranslator.isEnabled()) {
            return JSONBaseMessage.failure("sogou api is not enabled");
        }
        try {
            return sendRequest(initHttpPost(content,from,to), "errorCode", "translation", "",messageId);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return JSONBaseMessage.failure(null);
    }

    private HttpPost initHttpPost(String content,  String from, String to) {
        HttpPost httpPost = new HttpPost(sogouTranslator.getUrl());
        ArrayList<BasicNameValuePair> parameters = new ArrayList<BasicNameValuePair>() {
            {
                String appId = sogouTranslator.getAppid();
                String salt = sogouTranslator.getSalt();
                String secretKey = sogouTranslator.getSecretKey();
                add(new BasicNameValuePair("from", from));
                add(new BasicNameValuePair("to", to));
                add(new BasicNameValuePair("q", content));
                add(new BasicNameValuePair("pid", appId));
                add(new BasicNameValuePair("salt", salt));
                add(new BasicNameValuePair("sign", DigestUtils.md5Hex(appId + content + salt + secretKey)));
            }
        };
        httpPost.setEntity(new UrlEncodedFormEntity(parameters, Consts.UTF_8));
        httpPost.addHeader("content-type", "application/x-www-form-urlencoded");
        httpPost.addHeader("accept", "application/json");
        return httpPost;
    }

    static {
        ERROR_MESSAGE.put(0, "请求成功");
        ERROR_MESSAGE.put(1001, "不支持的语言类型");
        ERROR_MESSAGE.put(1002, "文本过长");
        ERROR_MESSAGE.put(1003, "无效PID");
        ERROR_MESSAGE.put(1004, "试用Pid限额已满");
        ERROR_MESSAGE.put(10041, "pid小语种限额已满");
        ERROR_MESSAGE.put(1005, "Pid请求流量过高");
        ERROR_MESSAGE.put(1007, "随机数不存在");
        ERROR_MESSAGE.put(1008, "签名不存在");
        ERROR_MESSAGE.put(1009, "签名不正确");
        ERROR_MESSAGE.put(10010, "文本不存在");
        ERROR_MESSAGE.put(1050, "内部服务错误");
        ERROR_MESSAGE.put(1101, "账户余额不足");
        ERROR_MESSAGE.put(1102, "接口请求过快");
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        TranslateServiceFactory.register(TranslateServiceFactory.SOU_GOU_TYPE,this);
    }
}
