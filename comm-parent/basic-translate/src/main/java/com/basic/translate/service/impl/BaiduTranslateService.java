package com.basic.translate.service.impl;

import com.basic.common.response.JSONBaseMessage;
import com.basic.commons.constants.CommConstants;
import com.basic.translate.entity.BaiduTranslator;
import com.basic.translate.factory.TranslateServiceFactory;
import com.basic.translate.service.TranslateService;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.client.methods.HttpGet;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

@Component
public class BaiduTranslateService implements TranslateService, InitializingBean {

    @Resource(name = "baiduTranslator")
    private BaiduTranslator baiduTranslator;

    @Override
    public JSONBaseMessage getTranslatedContent(String content, String from, String to,String messageId) {
        if (!baiduTranslator.isEnabled()) {
            return JSONBaseMessage.failure("baidu api is not enabled");
        }
        try {
            return sendRequest(initHttpGet(content, from,to), "error_code", "trans_result", "dst",messageId);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return JSONBaseMessage.failure(null);
    }

    private HttpGet initHttpGet(String content, String from,String to) throws UnsupportedEncodingException {
        content.replaceAll("%0a","%20");
        String appId = baiduTranslator.getAppid();
        int salt = baiduTranslator.getSalt();
        String secretKey = baiduTranslator.getSecretKey();
        return new HttpGet(String.format(baiduTranslator.getUrl(), appId, URLEncoder.encode(content, "UTF-8"), from, to, baiduTranslator.getSalt(), DigestUtils.md5Hex(appId + content + salt + secretKey)));
    }

    static {
        ERROR_MESSAGE.put(52000, "请求成功");
        ERROR_MESSAGE.put(52001, "请求超时");
        ERROR_MESSAGE.put(52002, "系统错误");
        ERROR_MESSAGE.put(52003, "未授权用户");
        ERROR_MESSAGE.put(54000, "必填参数为空");
        ERROR_MESSAGE.put(54001, "签名错误");
        ERROR_MESSAGE.put(54003, "访问频率受限");
        ERROR_MESSAGE.put(54004, "账户余额不足");
        ERROR_MESSAGE.put(54005, "长query请求频繁");
        ERROR_MESSAGE.put(58000, "客户端IP非法");
        ERROR_MESSAGE.put(58001, "译文语言方向不支持");
        ERROR_MESSAGE.put(58002, "服务当前已关闭");
        ERROR_MESSAGE.put(90107, "认证未通过或未生效");
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        TranslateServiceFactory.register(TranslateServiceFactory.BAI_DU_TYPE,this);
    }
}
