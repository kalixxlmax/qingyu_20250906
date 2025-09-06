package com.basic.im.logisticsInquire.logisticsModel;

import com.basic.im.identityVerifie.model.VerifieConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author zhm
 * @version V1.0
 * @Description: 物流查询
 * @date 2020/3/16 17:34
 */
@Configuration
@ConfigurationProperties(prefix = "logisticsconfig")
public class LogisticsConfig extends VerifieConfig {

}
