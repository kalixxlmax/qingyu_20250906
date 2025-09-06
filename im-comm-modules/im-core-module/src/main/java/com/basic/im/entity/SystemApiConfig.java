package com.basic.im.entity;

import java.util.List;

/**
 * 第三方系统集成对接 配置类 <br>
 *
 * 公私钥及 apiSecret 成对生成，可以重置
 * 系统管理员必须妥善保存，以防外泄
 *
 * @author: lidaye <br>
 * @date: 2021/11/10  <br>
 */
public class SystemApiConfig {

    public SystemApiConfig() {

    }

    /**
     * 接口请求白名单  为空即不限制
     */
    private List<String> requestApiList;


    /**
     * 接口调用凭证
     */
    private String apiSecret;

    /**
     * 接口请求私钥 保存服务端
     */
    private String privateKey;
    /**
     * 接口请求公钥 第三方集成使用加密
     */
    private String publicKey;


    public SystemApiConfig(String privateKey, String publicKey, String apiSecret) {
        this.privateKey = privateKey;
        this.publicKey = publicKey;
        this.apiSecret=apiSecret;
    }

    public void resetSystemApiConfig(String privateKey, String publicKey,String apiSecret) {
        this.privateKey = privateKey;
        this.publicKey = publicKey;
        this.apiSecret=apiSecret;
    }

    public String getPrivateKey() {
        return privateKey;
    }

    public void setPrivateKey(String privateKey) {
        this.privateKey = privateKey;
    }

    public String getPublicKey() {
        return publicKey;
    }

    public void setPublicKey(String publicKey) {
        this.publicKey = publicKey;
    }

    public List<String> getRequestApiList() {
        return requestApiList;
    }

    public void setRequestApiList(List<String> requestApiList) {
        this.requestApiList = requestApiList;
    }

    public String getApiSecret() {
        return apiSecret;
    }

    public void setApiSecret(String apiSecret) {
        this.apiSecret = apiSecret;
    }
}
