package com.basic.payment.channel.wechat;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

/**
 * @author : lidaeye
 * Time: 14:40
 * 这里放置各种配置数据
 */
@Data
@Configuration
@ConfigurationProperties(prefix="wxpayconfig")
public class WxPayProperties {

	// 微信认证的自己应用ID
	private String appid;
	// 商户ID
	private String mchid;
	// App secret
	private String secret;
	// api  API密钥
	private String apiKey;
	//
	/**
	 * 微信支付 回调 通知 url
	 * 默认   http://imapi.server.com/user/recharge/wxPayCallBack
	 *
	 */
	private String callBackUrl;


	//证书文件 名称
	private String pkPath;
}
