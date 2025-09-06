package com.basic.im.push.service;

import cn.jiguang.common.resp.APIConnectionException;
import cn.jiguang.common.resp.APIRequestException;
import cn.jpush.api.push.PushResult;
import cn.jpush.api.push.model.Message;
import cn.jpush.api.push.model.Options;
import cn.jpush.api.push.model.Platform;
import cn.jpush.api.push.model.PushPayload;
import cn.jpush.api.push.model.audience.Audience;
import cn.jpush.api.push.model.notification.AndroidNotification;
import cn.jpush.api.push.model.notification.IosNotification;
import cn.jpush.api.push.model.notification.Notification;
import com.alibaba.fastjson.JSONObject;
import com.basic.im.user.entity.User;
import com.basic.im.utils.SKBeanUtils;

import java.util.Map;

public class JPushServices extends JPushUtils {

	// 极光推送>>Android
	public static void jpushAndroid(Map<String, String> parm, User.DeviceInfo deviceInfo) {
		// 推送的关键,构造一个payload
		PushPayload payload = PushPayload.newBuilder().setPlatform(Platform.android())// 指定android平台的用户
				.setAudience(Audience.registrationId(parm.get("regId")))// registrationId指定用户
//				.setAudience(Audience.all())// Audience设置为all，说明采用广播方式推送，所有用户都可以接收到
				.setNotification(Notification.android(parm.get("msg"), parm.get("title"), parm))// title 标题
				.setOptions(Options.newBuilder().setApnsProduction(false).build())
				// 这里是指定开发环境,不用设置也没关系
				.setMessage(Message.content(parm.get("msg")))// 自定义信息
				.build();

		try {

			PushResult result = getJPushClient(deviceInfo.getPackName()).sendPush(payload);
			System.out.println("JPush to Android ===== > result  " + JSONObject.toJSONString(result));
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}

	/**
	 * @param parm
	 * @param deviceInfo
	 * @Description: IOS极光推送
	 **/
	public static void buildPushObject_ios_tagAnd_alertWithExtrasAndMessage(Map<String, String> parm, User.DeviceInfo deviceInfo) {
		boolean flag = 1 == SKBeanUtils.getImCoreService().getClientConfig().getIsOpenAPNSorJPUSH() ? false : true;
		//创建JPushClient
		PushPayload payload = PushPayload.newBuilder()
				.setPlatform(Platform.ios())//ios平台的用户
//	            .setAudience(Audience.all())//所有用户
				.setAudience(Audience.registrationId(parm.get("regId")))//registrationId指定用户
				.setNotification(Notification.newBuilder()
						.addPlatformNotification(IosNotification.newBuilder()
								.setAlert(parm.get("msg"))
								.incrBadge(1)
								.setSound("happy")//这里是设置提示音(更多可以去官网看看)
								.addExtras(parm)
								.build())
						.build())
				.setOptions(Options.newBuilder().setApnsProduction(flag).build())
				.setMessage(Message.newBuilder().setMsgContent(parm.get("msg")).addExtras(parm).build())//自定义信息
				.build();
		try {
			PushResult pu = getJPushClient(deviceInfo.getPackName()).sendPush(payload);
			log.info("JPush result : {}", pu.toString());
		} catch (APIConnectionException | APIRequestException e) {
			e.printStackTrace();
		}
	}

}
