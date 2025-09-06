package com.basic.im.businessLicense.businessModel;

import com.basic.im.identityVerifie.model.VerifieConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author zhm
 * @version V1.0
 * @Description: 营业执照配置
 * @date 2020/3/17 11:05
 */
@Configuration
@ConfigurationProperties(prefix = "businessconfig")
public class BusinessConfig extends VerifieConfig {

}
