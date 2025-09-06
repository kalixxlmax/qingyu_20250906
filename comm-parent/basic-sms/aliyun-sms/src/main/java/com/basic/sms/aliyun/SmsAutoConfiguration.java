package com.basic.sms.aliyun;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 阿里云 SMS 自动配置.
 *
 * @author chat
 */
@Configuration
@ConditionalOnClass(name = "com.aliyuncs.IAcsClient")
@EnableConfigurationProperties(SmsProperties.class)
public class SmsAutoConfiguration {
    private final SmsProperties smsProperties;

    public SmsAutoConfiguration(final SmsProperties smsProperties) {
        this.smsProperties = smsProperties;
    }

    /**
     * Configuration SmsClient bean.
     *
     * @return the sms client
     */
    @Bean
    @ConditionalOnMissingBean
    public SmsClient smsClient() {
        if (this.smsProperties.getTemplates() == null) {
            return new SmsClient(this.smsProperties.getAccessKeyId(), this.smsProperties.getAccessKeySecret());
        } else {
            return new SmsClient(
                    this.smsProperties.getAccessKeyId(),
                    this.smsProperties.getAccessKeySecret(),
                    this.smsProperties.getTemplates());
        }
    }
}