/*
 * Copyright 2020. Huawei Technologies Co., Ltd. All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.basic.im.push.service.hwpush.messaging;

import com.alibaba.fastjson.JSONObject;
import com.basic.im.utils.SKBeanUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Accept the appId and appSecret given by the user and build the credential
 * Every app has a credential which is for certification
 */
public class HuaweiCredential {
    private static final Logger logger = LoggerFactory.getLogger(HuaweiCredential.class);

//    private final String PUSH_AT_URL = ResourceBundle.getBundle("url").getString("token_server");
//    private static  String tokenUrl = pushConfig.getHw_tokenUrl();

    private final String tokenURl;
    private final String pushUrl;
    private final String packageName;

    private String appId;
    private String appSecret;

    private String accessToken;
    private long expireIn;
    private Lock lock;
    private final CloseableHttpClient httpClient = HttpClients.createDefault();

    private HuaweiCredential(Builder builder) {
        this.lock = new ReentrantLock();
        this.appId = builder.appId;
        this.appSecret = builder.appSecret;
        this.tokenURl = builder.tokenUrl;
        this.pushUrl = builder.pushUrl;
        this.packageName = builder.packageName;
    }

    /**
     * Refresh accessToken via HCM manually.
     */
    public final void refreshToken() {
        try {
            executeRefresh(packageName);
        } catch (IOException e) {
            logger.debug("Fail to refresh token!", e);
        }
    }


    private void executeRefresh(String packageName) throws IOException {
        Map<String,String> tokenMap = SKBeanUtils.getLocalSpringBeanManager().getCoreRedisRepository().getHWPushToken(packageName);
//        logger.info("HWPush executeRefresh redis tokenMap ：{}",JSONObject.toJSONString(tokenMap));
        // 优先返回redis中的access_token
        if(!tokenMap.isEmpty()){
            this.accessToken = tokenMap.get("access_token");
            this.expireIn = Long.parseLong(String.valueOf(tokenMap.get("expireIn")));
            return;
        }
        String requestBody = createRequestBody(appId, appSecret);

        HttpPost httpPost = new HttpPost(tokenURl);
        StringEntity entity = new StringEntity(requestBody);
        httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded;charset=utf-8");
        httpPost.setEntity(entity);
        CloseableHttpResponse response = httpClient.execute(httpPost);
        String jsonStr = EntityUtils.toString(response.getEntity());
        logger.info("executeRefresh jsonStr :{}",jsonStr);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 200) {
            JSONObject jsonObject = JSONObject.parseObject(jsonStr);
            this.accessToken = jsonObject.getString("access_token");
            this.expireIn = jsonObject.getLong("expires_in") * 1000L;
        } else {
            logger.debug("Fail to refresh token!");
        }
        // 缓存accessToken
        SKBeanUtils.getLocalSpringBeanManager().getCoreRedisRepository().saveHWPushToken(this.accessToken,packageName);
    }

    private String createRequestBody(String appId, String appSecret) {
        return MessageFormat.format("grant_type=client_credentials&client_secret={0}&client_id={1}", appSecret, appId);
    }

    /**
     * getter
     */
    public final String getAccessToken() {
        this.lock.lock();

        String tmp;
        try {
            tmp = this.accessToken;
        } finally {
            this.lock.unlock();
        }

        return tmp;
    }

    public final long getExpireIn() {
        this.lock.lock();

        long tmp;
        try {
            tmp = this.expireIn;
        } finally {
            this.lock.unlock();
        }

        return tmp;
    }

    protected CloseableHttpClient getHttpClient() {
        return httpClient;
    }

    public String getAppId() {
        return appId;
    }

    public String getPushUrl() {
        return pushUrl;
    }

    public String getPackageName(){
        return packageName;
    }

    /**
     * Builder for constructing {@link HuaweiCredential}.
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String appId;
        private String appSecret;
        // 自定义添加 2020-08-10
        private String tokenUrl;
        private String packageName;

        private String pushUrl;

        private Builder() {

        }

        public Builder setAppId(String appId) {
            this.appId = appId;
            return this;
        }

        public Builder setAppSecret(String appSecret) {
            this.appSecret = appSecret;
            return this;
        }

        public Builder setTokenUrl(String tokenUrl) {
            this.tokenUrl = tokenUrl;
            return this;
        }

        public Builder setPushUrl(String pushUrl) {
            this.pushUrl = pushUrl;
            return this;
        }

        public Builder setPackageName(String packageName) {
            this.packageName = packageName;
            return this;
        }

        public HuaweiCredential build() {
            return new HuaweiCredential(this);
        }
    }
}
