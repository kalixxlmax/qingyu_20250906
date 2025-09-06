package com.basic.im.push.service;


import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.basic.im.entity.PushConfig;
import com.basic.im.push.vo.MsgNotice;
import com.basic.im.repository.CoreRedisRepository;
import com.basic.im.user.entity.User;
import com.basic.im.utils.SKBeanUtils;
import com.basic.utils.StringUtil;
import com.vivo.push.sdk.notofication.Message;
import com.vivo.push.sdk.notofication.Result;
import com.vivo.push.sdk.notofication.TargetMessage;
import com.vivo.push.sdk.server.Sender;
import lombok.extern.slf4j.Slf4j;

import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class VIVOPushService extends PushServiceUtils {

	static CoreRedisRepository getCoreRedisRepository() {
		return SKBeanUtils.getLocalSpringBeanManager().getCoreRedisRepository();
	}

	public static Map<String, Sender> expandSender;// app推送sender

	private static Sender reSetSender(String packName) throws Exception {
		Sender sender;
		PushConfig.AndroidPush pushConfig = getPushConfig(packName);
		String appSecret = pushConfig.getVivoAppSecret();
		synchronized (appSecret) {
			String authToken;
			//发送鉴权请求
			String pushToken = getCoreRedisRepository().getVivoPushToken(packName);
			if (!StringUtil.isEmpty(pushToken)) {
				authToken = pushToken;
			} else {
				//实例化Sender
				Sender appSender = new Sender(appSecret);//注册登录开发平台网站获取到的appSecret
				Result result = appSender.getToken(pushConfig.getVivoAppId(), pushConfig.getVivoAppKey());
				// 保存vivo推送token
				authToken = result.getAuthToken();
				getCoreRedisRepository().saveVivoToken(authToken, packName);
			}
			sender = new Sender(appSecret);
			sender.setAuthToken(authToken);
			//设置连接池参数，可选项
			sender.initPool(100, 50);
		}
				expandSender.put(packName, sender);
		return sender;
	}

	public static Sender getSender(String packName) throws Exception {
		Sender sender;
		if (null != expandSender) {
			sender = expandSender.get(packName);
			if (null != sender) {
				return sender;
			} else {
				return reSetSender(packName);
			}
		} else {
			expandSender = Maps.newConcurrentMap();
			return reSetSender(packName);
		}
	}

	/**
	 * @param pushId
	 * @param msgNotice
	 * @return
	 * @throws Exception
	 * @Description: 通知栏消息
	 **/
	public static Message buildMessage(String pushId, String packageName, MsgNotice msgNotice) throws Exception {
		String url = null;
		if (msgNotice.getIsGroup()) {
			url = "intent://" + packageName + "/notification#Intent;scheme=chat;launchFlags=0x10000000;S.userId=" + msgNotice.getRoomJid() + ";end";
		} else {
			url = "intent://" + packageName + "/notification#Intent;scheme=chat;launchFlags=0x10000000;S.userId=" + msgNotice.getFrom() + ";end";
		}
		Message message = new Message.Builder()
				.regId(pushId)//仅构建单推消息体需要
				.notifyType(2)// 响铃
				.classification(1)
				.title(msgNotice.getTitle())// 通知标题
				.content(msgNotice.getText())// 通知内容
				.timeToLive(1000)// 消息的生命周期,消息在服务器保存的时间, 单位: 秒
				.skipType(4)// 跳转类型1：打开APP首页 2：打开链接  3：自定义 4：打开app内指定页面
				.skipContent(url)
				.networkType(-1)// 可选项，发送推送使用的网络方式(-1 :任何网络下，1：仅wifi下)
				.requestId(pushId).build();
		return message;
	}

	public static void noticeColumnMessagePush(User.DeviceInfo deviceInfo, MsgNotice msgNotice, boolean initFlag) throws Exception {
		if (initFlag) {
			expandSender = Maps.newConcurrentMap();
		}
		try {
			Result resultMessage = getSender(deviceInfo.getPackName()).sendSingle(buildMessage(deviceInfo.getPushToken(), deviceInfo.getPackName(), msgNotice));
			if (null != resultMessage && 0 != resultMessage.getResult()) {
				log.info("VIVOPush error code:{} comment:{}", resultMessage.getResult(), resultMessage.getDesc());
			} else {
				log.info("VIVOPush SUCCESS resultMessage:{}", resultMessage.toString());
			}
		} catch (Exception e) {
			log.error(e.getMessage(),e);
		}

	}

	public static String saveListPayload(MsgNotice msgNotice, PushConfig.AndroidPush androidPush,String packageName) throws Exception {
		String url;
		if (null == msgNotice.getObjectId()) {
			url = "intent://" + packageName + "/notification#Intent;scheme=sk;launchFlags=0x10000000;end";
		} else {
			url = "intent://" + packageName + "/notification#Intent;scheme=sk;launchFlags=0x10000000;S.url=" + URLEncoder.encode(msgNotice.getObjectId(), "UTF-8") + ";end";
		}
		log.info("vivo save listPayload url : {}", url);
		/*Sender sender1 = new Sender(androidPush.getVivoAppSecret());// 实例化Sender
		Result result = sender1.getToken(androidPush.getVivoAppId(), androidPush.getVivoAppKey());// 发送鉴权请求*/
		Sender sender = new Sender(androidPush.getVivoAppSecret());
		Result token = sender.getToken(androidPush.getVivoAppId(), androidPush.getVivoAppKey());// 发送鉴权请求
		sender.setAuthToken(token.getAuthToken());
		sender.initPool(20, 10);// 设置连接池参数，可选项
		Message saveList = new Message.Builder().notifyType(2)// 响铃
				.title(msgNotice.getTitle())// 通知标题
				.content(msgNotice.getText())// 通知内容
				.timeToLive(1000)// 消息的生命周期,消息在服务器保存的时间, 单位: 秒
				.skipType(4)// 跳转类型1：打开APP首页 2：打开链接  3：自定义 4：打开app内指定页面
				.skipContent(url)
				.requestId(StringUtil.randomUUID()).build();// 构建要保存的批量推送消息体
		Result resultPayload = sender.saveListPayLoad(saveList);// 发送保存群推消息请求
		resultPayload.getResult();// 获取服务器返回的状态码，0成功，非0失败
		resultPayload.getDesc();// 获取服务器返回的调用情况文字描述
		resultPayload.getTaskId();// 如请求发送成功，将获得该条消息的任务编号，即taskId
		return resultPayload.getTaskId();
	}

	/**
	 * @param msgNotice
	 * @param regIds
	 * @throws Exception
	 * @Description: 批量推送
	 **/
	public static void listSend(MsgNotice msgNotice, Set<String> regIds) throws Exception {
//        Set<String> regid = new HashSet<>();//构建批量推送用户群
//        regid.add(saveListPayload());// 2 - 1000

		List<PushConfig> parentPushConfig = getParentPushConfig();
		if (null == parentPushConfig || parentPushConfig.isEmpty()) {
			return;
		} else {
			parentPushConfig.stream().forEach(pushConfig -> {
				try {
					TargetMessage targetMessage = new TargetMessage.Builder()
							.taskId(saveListPayload(msgNotice,pushConfig.getAndroidPush(),pushConfig.getPackageName()))
							.regIds(regIds)
							.requestId(StringUtil.randomUUID())
							.build();//构建批量推送的消息体
					Result resultTarget = getSender(pushConfig.getPackageName()).sendToList(targetMessage);//批量推送给用户
					resultTarget.getResult();//获取服务器返回的状态码，0成功，非0失败
					resultTarget.getDesc();//获取服务器返回的调用情况文字描述
					log.info("VIVO resultTarget 批量推送 ： {}", JSONObject.toJSONString(resultTarget));
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		}
	}

}
