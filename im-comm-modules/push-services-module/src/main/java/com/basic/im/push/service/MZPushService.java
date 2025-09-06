package com.basic.im.push.service;

import com.alibaba.fastjson.JSONObject;
import com.meizu.push.sdk.constant.PushType;
import com.meizu.push.sdk.server.IFlymePush;
import com.meizu.push.sdk.server.constant.ResultPack;
import com.meizu.push.sdk.server.model.push.PushResult;
import com.meizu.push.sdk.server.model.push.VarnishedMessage;
import com.basic.im.entity.PushConfig;
import com.basic.im.push.vo.MsgNotice;
import com.basic.im.user.entity.User;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class MZPushService extends PushServiceUtils {

	/**
	 * 通知栏消息
	 *
	 * @param deviceInfo
	 * @param msgNotice
	 * @throws IOException
	 */
	public static void varnishedMessagePush(User.DeviceInfo deviceInfo, MsgNotice msgNotice) throws IOException {
		String url = null;
		if (msgNotice.getIsGroup()) {
			url = "chat://" + deviceInfo.getPackName() + "/notification?userId=" + msgNotice.getRoomJid();
		} else {
			url = "chat://" + deviceInfo.getPackName() + "/notification?userId=" + msgNotice.getFrom();
		}

		IFlymePush push = new IFlymePush(getPushConfig(deviceInfo.getPackName()).getMzAppSecret());

		// 组装消息
		VarnishedMessage message = new VarnishedMessage.Builder().appId(getPushConfig(deviceInfo.getPackName()).getMzAppId())
				.title(msgNotice.getTitle())// 通知标题
				.content(msgNotice.getText())// 通知内容
				.noticeBarType(2) // 通知栏样式 0 标准    2安卓原生
				.clickType(2) // 点击动作  0 打开应用    1 打开网页应用     2 打开url页面     3 应用客户端自定义
				.url(url)
				.build();

		// 目标用户
		List<String> pushIds = new ArrayList<String>();
		pushIds.add(deviceInfo.getPushToken());

		// 1 调用推送服务
		ResultPack<PushResult> result = push.pushMessage(message, pushIds);
		log.info("IFlymePush result is {}", result.isSucceed());
		if (result.isSucceed()) {
			// 2 调用推送服务成功 （其中map为设备的具体推送结果，一般业务针对超速的code类型做处理）
			PushResult pushResult = result.value();
//            String msgId = pushResult.getMsgId();//推送消息ID，用于推送流程明细排查
			Map<String, List<String>> targetResultMap = pushResult.getRespTarget();//推送结果，全部推送成功，则map为empty
			if (targetResultMap != null && !targetResultMap.isEmpty()) {
				log.info("push fail token:{}", targetResultMap);
			}
		} else {
			log.info(String.format("pushMessage error code:{} comment:{}", result.code(), result.comment()));
		}
	}

	/**
	 * @param msgNotice
	 * @throws IOException
	 * @Description: 魅族全推
	 **/
	public static void pushToAPP(MsgNotice msgNotice) throws IOException {
		List<PushConfig> parentPushConfig = getParentPushConfig();
		if (null == parentPushConfig || parentPushConfig.isEmpty()) {
			return;
		} else {
			parentPushConfig.stream().forEach(pushConfig -> {
				String url = null;
				if (null == msgNotice.getObjectId()) {
					url = "chat://" + pushConfig.getPackageName() + "/notification";
				} else {
					try {
						url = "chat://" + pushConfig.getPackageName() + "/notification?url=" + URLEncoder.encode(msgNotice.getObjectId(), "UTF-8");
					} catch (UnsupportedEncodingException e) {
						e.printStackTrace();
					}
				}

				IFlymePush push = new IFlymePush(pushConfig.getAndroidPush().getMzAppSecret());
				//组装消息
				VarnishedMessage message = new VarnishedMessage.Builder()
						.appId(pushConfig.getAndroidPush().getMzAppId()).title(msgNotice.getTitle()).content(msgNotice.getText())
						.noticeBarType(2) // 通知栏样式 0 标准    2安卓原生
						.clickType(2) // 点击动作  0 打开应用    1 打开网页应用     2 打开url页面     3 应用客户端自定义
						.url(url)
						.build();
				ResultPack<Long> result = null;
				try {
					result = push.pushToApp(PushType.STATUSBAR, message);
				} catch (IOException e) {
					e.printStackTrace();
				}
				log.info("IFlymePush result is {}", JSONObject.toJSONString(result));
				if (!result.isSucceed()) {
					log.info("pushMessage error code:{} comment:{}", result.code(), result.comment());
				}
			});
		}
	}
}
