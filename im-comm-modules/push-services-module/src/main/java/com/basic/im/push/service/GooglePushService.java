package com.basic.im.push.service;

import com.alibaba.fastjson.JSON;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.messaging.AndroidConfig;
import com.google.firebase.messaging.AndroidNotification;
import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.Message;
import com.google.firebase.messaging.Message.Builder;
import com.basic.im.push.vo.MsgNotice;
import com.basic.im.user.entity.User;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GooglePushService extends PushServiceUtils{

//	private static String dataBaseUrl = getPushConfig().getFcmDataBaseUrl();

	static Builder builder=Message.builder();

	static com.google.firebase.messaging.AndroidConfig.Builder androidBuilder=AndroidConfig.builder();

	static AndroidNotification.Builder androidNotifiBuilder=AndroidNotification.builder();

	static FirebaseApp firebaseApp=null;


	/**
	 * 初始化FireBaseApp
	 */
	private static void initFireBase(String packageName){

		try {
			GooglePushUtil googlePushUtil=new GooglePushUtil();
			FirebaseOptions options = new FirebaseOptions.Builder()
					.setCredentials(GoogleCredentials.fromStream(googlePushUtil.getJson(packageName)))
					.setDatabaseUrl(getPushConfig(packageName).getFcmDataBaseUrl())
					.build();
			log.info("initFireBase options :{}",options.toString());
			log.info("initFireBase options :{}", JSON.toJSONString(options));
			if(firebaseApp==null) {
				firebaseApp = FirebaseApp.initializeApp(options);
			}
			log.info("initFireBase : {}",firebaseApp.toString());

			androidBuilder.setRestrictedPackageName(packageName);
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage(),e);
		}
	}

	/**
	 * 发送消息
	 * @param deviceInfo
	 * @param msgNotice
	 */
	public static void fcmPush(User.DeviceInfo deviceInfo, MsgNotice msgNotice, Boolean flag) {
		try {
			initFireBase(deviceInfo.getPackName());
			androidNotifiBuilder.setColor("#55BEB7");// 设置通知颜色
			androidNotifiBuilder.setBody(msgNotice.getText());// 设置通知内容
			//androidNotifiBuilder.setIcon("");// 设置通知图标
			androidNotifiBuilder.setTitle(msgNotice.getTitle());// 设置通知标题
			/*if(flag){
				// 音视频使用默认提示音
				androidNotifiBuilder.setSound("default");
			}*/
			androidNotifiBuilder.setChannelId(flag ? "20200408" : "20200302");
			log.info("androidNotifiBuilder channelId : {}", flag ? "20200408" : "20200302");
			AndroidNotification androidNotification = androidNotifiBuilder.build();

			androidBuilder.setNotification(androidNotification);

			AndroidConfig androidConfig = androidBuilder.build();

			builder.setToken(deviceInfo.getPushToken());// 设置token
			builder.setAndroidConfig(androidConfig);
			builder.putData("userId", msgNotice.getIsGroup() ? msgNotice.getRoomJid() : String.valueOf(msgNotice.getFrom()));// 自定义数据
			Message message = builder.build();

			String fcm = FirebaseMessaging.getInstance().send(message);
			log.info("google fcm push success : {}", fcm);
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage(), e);
		}
	}
}
