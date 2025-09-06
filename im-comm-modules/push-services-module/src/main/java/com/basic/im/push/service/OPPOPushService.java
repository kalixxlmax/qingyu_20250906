package com.basic.im.push.service;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.oppo.push.server.*;
import com.basic.im.entity.PushConfig;
import com.basic.im.push.vo.MsgNotice;
import com.basic.im.user.entity.User;
import lombok.extern.slf4j.Slf4j;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class OPPOPushService extends PushServiceUtils {
	/*private static String appKey = getPushConfig().getOppoAppKey();
	private static String masterSecret = getPushConfig().getOppoMasterSecret();
	private static Sender sender;*/

	private static Map<String, Sender> expandSender;// app推送sender

	private static Sender reSetSender(String packName) throws Exception {
		PushConfig.AndroidPush pushAndroidConfig = getPushConfig(packName);
		if(null==pushAndroidConfig){
			log.error("pushConfig is NUll {}",packName);
			return null;
		}
		Sender sender =  new Sender(pushAndroidConfig.getOppoAppKey(), pushAndroidConfig.getOppoMasterSecret());
		expandSender.put(packName,sender);
		return sender;
	}

	private static Sender getSender(String packName) throws Exception {
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

	/** @Description: 通知栏消息
	 * @param deviceInfo
	 * @param msgNotice
	 * @return
	 * @throws Exception
	 **/
	public static void buildMessage(User.DeviceInfo deviceInfo, MsgNotice msgNotice, boolean flag, boolean initFlag) throws Exception {
		if (initFlag) {
			expandSender = Maps.newConcurrentMap();
		}
		// 发送单推通知栏消息
		try {
			Notification notification = getNotification(deviceInfo.getPackName(),msgNotice,flag); //创建通知栏消息体
			Target target = Target.build(deviceInfo.getPushToken()); //创建发送对象
			Sender sender = getSender(deviceInfo.getPackName());
			if(null==sender){
				return;
			}
			Result result = sender.unicastNotification(notification, target);  //发送单推消息
			/**
			 * result.getStatusCode(); // 获取http请求状态码
			 * result.getReturnCode(); // 获取平台返回码
			 * result.getMessageId();  // 获取平台返回的messageId
			 * */
			if(null != result && 0 != result.getStatusCode()){
				log.info("  OPPOPush result :  {}", JSONObject.toJSONString(result));
			}else{
				log.info("OPPOPush error returnCode:{}; messageId:{}", result.getReturnCode(),result.getMessageId());
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage());
		}

	}


	/** @Description:创建通知栏消息体
	 * @param msgNotice
	 * @return
	 **/
	private static Notification getNotification(String packageName,MsgNotice msgNotice,boolean flag) {
		String url = null;
		if(msgNotice.getIsGroup()) {
			url="chat://"+packageName+"/notification?userId="+msgNotice.getRoomJid();
		} else {
			url="chat://"+packageName+"/notification?userId="+msgNotice.getFrom();
		}

		Notification notification = new Notification();

		notification.setTitle(msgNotice.getTitle());
		notification.setContent(msgNotice.getText());
		if(flag) {
			notification.setChannelId(Integer.valueOf(String.valueOf(20200408)));
		} else {
			notification.setChannelId(Integer.valueOf(String.valueOf(20200302)));
		}

		// App开发者自定义消息Id，OPPO推送平台根据此ID做去重处理，对于广播推送相同appMessageId只会保存一次，对于单推相同appMessageId只会推送一次
		notification.setAppMessageId(UUID.randomUUID().toString());
		// 应用接收消息到达回执的回调URL，字数限制200以内，中英文均以一个计算
//		notification.setCallBackUrl("http://www.test.com");
		// App开发者自定义回执参数，字数限制50以内，中英文均以一个计算
//		notification.setCallBackParameter("");
		// 点击动作类型0，启动应用；1，打开应用内页（activity的intent
		// action）；2，打开网页；4，打开应用内页（activity）；【非必填，默认值为0】;5,Intent scheme URL
		notification.setClickActionType(5);
		// 应用内页地址【click_action_type为1或4时必填，长度500】
//		notification.setClickActionActivity("com.coloros.push.demo.component.InternalActivity");
		// 网页地址【click_action_type为2必填，长度500】
		notification.setClickActionUrl(url);
		// 动作参数，打开应用内页或网页时传递给应用或网页【JSON格式，非必填】，字符数不能超过4K，示例：{"key1":"value1","key2":"value2"}
//		notification.setActionParameters("{\"key1\":\"value1\",\"key2\":\"value2\"}");
		// 展示类型 (0, “即时”),(1, “定时”)
		notification.setShowTimeType(1);
		// 定时展示开始时间（根据time_zone转换成当地时间），时间的毫秒数
		notification.setShowStartTime(System.currentTimeMillis() + 1000 * 60 * 3);
		// 定时展示结束时间（根据time_zone转换成当地时间），时间的毫秒数
		notification.setShowEndTime(System.currentTimeMillis() + 1000 * 60 * 5);
		// 是否进离线消息,【非必填，默认为True】
		notification.setOffLine(true);
		// 离线消息的存活时间(time_to_live) (单位：秒), 【off_line值为true时，必填，最长3天】
		notification.setOffLineTtl(24 * 3600);
		// 时区，默认值：（GMT+08:00）北京，香港，新加坡
		notification.setTimeZone("GMT+08:00");
		// 0：不限联网方式, 1：仅wifi推送
		notification.setNetworkType(0);

		return notification;

	}

	// 创建广播消息
	public static void broadcastMessage(MsgNotice msgNotice) throws Exception {
		List<PushConfig> parentPushConfig = getParentPushConfig();
		if (null == parentPushConfig || parentPushConfig.isEmpty()) {
			return;
		} else {
			parentPushConfig.stream().forEach(pushConfig -> {
				String url = null;
				if(null == msgNotice.getObjectId()) {
					url="chat://"+pushConfig.getPackageName()+"/notification";
				} else {
					try {
						url="chat://"+pushConfig.getPackageName()+"/notification?url="+URLEncoder.encode(msgNotice.getObjectId(), "UTF-8");
					} catch (UnsupportedEncodingException e) {
						e.printStackTrace();
					}
				}
				try {
					Notification broadNotification = getNotification(pushConfig.getPackageName(),msgNotice,false);// 创建通知栏消息体
					broadNotification.setClickActionUrl(url);
					Sender sender = getSender(pushConfig.getPackageName());
					if(null==sender){
						return;
					}
					Result saveResult = sender.saveNotification(broadNotification); // 发送保存消息体请求

					saveResult.getStatusCode(); // 获取http请求状态码

					saveResult.getReturnCode(); // 获取平台返回码
					log.info("OPPO broadCast Message result ： {},   url : {} ",JSONObject.toJSONString(saveResult),url);
					String messageId = saveResult.getMessageId(); // 获取messageId

					Target target = new Target(); // 创建广播目标

					target.setTargetValue("");
					target.setTargetType(TargetType.REGISTRATION_ID);

					Result broadResult = sender.broadcastNotification(messageId, target); // 发送广播消息

					broadResult.getTaskId(); // 获取广播taskId

					List<Result.BroadcastErrorResult> errorList = broadResult.getBroadcastErrorResults();
					if (errorList.size() > 0) { // 如果大小为0，代表所有目标发送成功

						for (Result.BroadcastErrorResult error : errorList) {
							error.getErrorCode(); // 错误码
							error.getTargetValue(); // 目标
							log.info("OPPO PUSH  error : {}"+JSONObject.toJSONString(error));
						}

					}
				} catch (Exception e) {
					e.printStackTrace();
					log.error(e.getMessage());
				}
			});
		}

	}
}
