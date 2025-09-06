package com.basic.im.push.service;

import cn.jpush.api.JPushClient;
import com.google.common.collect.Maps;
import com.basic.im.entity.PushConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class JPushUtils extends PushServiceUtils {
	protected static final Logger log = LoggerFactory.getLogger(JPushUtils.class);
	// 创建JPushClient(极光推送的实例)
	protected static Map<String, JPushClient> jPushClientMqp;


	// 初始化连接
	private static JPushClient reSetSender(String packName) {
		PushConfig.IOSPush pushConfig = getIOSPushConfig(packName);
		jPushClientMqp = Maps.newConcurrentMap();
		JPushClient jPushClient = new JPushClient(pushConfig.getJPushMasterSecret(), pushConfig.getJPushAppKey());
		jPushClientMqp.put(packName, jPushClient);
		return jPushClient;
	}

	/**
	 * 根据包名获取推送Sender
	 *
	 * @param packName 包名
	 * @return Sender
	 */
	protected static JPushClient getJPushClient(String packName) {
		JPushClient jPushClient;
		if (null != jPushClientMqp) {
			jPushClient = jPushClientMqp.get(packName);
			if (null != jPushClient) {
				return jPushClient;
			} else {
				return reSetSender(packName);
			}
		} else {
			return reSetSender(packName);
		}
	}

}
