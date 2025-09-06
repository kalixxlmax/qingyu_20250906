package com.basic.sms.aliyun;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * 阿里云 SMS 配置属性.
 *
 * @author chat
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "aliyun.sms")
public class SmsProperties implements InitializingBean {

    //
    private String accessKeyId;
    private String accessKeySecret;
    private String signName;


   private Map<String, SmsTemplate> templates;

    @Override
    public void afterPropertiesSet() throws Exception {
        if (null != this.templates) {
            for (final SmsTemplate smsTemplate : this.templates.values()) {
                if (null == smsTemplate.getSignName()) {
                    smsTemplate.setSignName(this.signName);
                }
            }
        }
    }
}