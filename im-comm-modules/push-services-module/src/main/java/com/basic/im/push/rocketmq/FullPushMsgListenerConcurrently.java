package com.basic.im.push.rocketmq;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.push.service.*;
import com.basic.im.push.service.hwpush.examples.SendNotifyMessage;
import com.basic.im.push.service.hwpush.exception.HuaweiMesssagingException;
import com.basic.im.push.vo.MsgNotice;
import com.basic.im.push.vo.PushMessageDTO;
import com.basic.im.user.entity.User;
import com.basic.utils.DateUtil;
import com.basic.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/** @version:（1.0） 
 * @ClassName	SystemNoticeMsgListenerConcurrently
 * @Description: 全量推送 
 * @date:2019年5月29日下午3:12:10  
 */ 
@Slf4j
@Service
@RocketMQMessageListener(topic = "fullPushMessage", consumerGroup = "my-consumer-fullPushMessage")
public class FullPushMsgListenerConcurrently implements  RocketMQListener<String>{



	@Override
	public void onMessage(String body) {
		try {
			if(KConstants.isDebug) {
				log.info(" new msg ==> "+body);
			}

			PushMessageDTO pushMessageDTO= JSON.parseObject(body,PushMessageDTO.class);
			push(pushMessageDTO);


		} catch (Exception e) {
			log.error(e.getMessage(),e);
		}
	}

	private MsgNotice parseMsgNotice(PushMessageDTO pushMessageDTO) {
		MsgNotice notice=new MsgNotice();
		try {
			notice.setTitle(pushMessageDTO.getTitle());
			notice.setText(pushMessageDTO.getContent());
			if(null != pushMessageDTO.getObjectId()) {
				notice.setObjectId(pushMessageDTO.getObjectId());
			}
			if(0 != pushMessageDTO.getType()) {
				notice.setType(pushMessageDTO.getType());
			}

		} catch (Exception e) {
			throw e;
		}
		return notice;
	}



	private void push(final PushMessageDTO pushMessageDTO) {

		MsgNotice notice = parseMsgNotice(pushMessageDTO);
		if(null==notice) {
			return;
		}
		User.DeviceInfo deviceInfo = new User.DeviceInfo();
		String packName = pushMessageDTO.getPushPackageName();
		String token = pushMessageDTO.getPushToken();
		deviceInfo.setPackName(packName);
		deviceInfo.setPushToken(token);

		switch(notice.getType()){
			case 1:
				log.info("===== 全量推送 ======");
				fullPushService.pushToDevice(notice);
				break;

			case 2 ://华为推送
				try {
					log.info("===== 华为推送 ======推送包名：" + packName +",推送token："+token+".");
					if (isPushConfigNull(packName,token)){ return; }
					SendNotifyMessage.sendNotification(notice,deviceInfo,false,true);
				} catch (HuaweiMesssagingException e) {
					e.printStackTrace();
				}
				break;

			case  3: //VIVO推送
				try {
					log.info("===== VIVO推送 ======推送包名：" + packName +",推送token："+token+".");
					if (isPushConfigNull(packName,token)){ return; }
					VIVOPushService.noticeColumnMessagePush(deviceInfo, notice, true);
				} catch (Exception e) {
					e.printStackTrace();
				}
				break;

			case  4: //OPPO推送
				try {
					log.info("===== OPPO推送 ======推送包名：" + packName +",推送token："+token+".");
					if (isPushConfigNull(packName,token)){ return; }
					OPPOPushService.buildMessage(deviceInfo, notice,false, true);
				} catch (Exception e) {
					e.printStackTrace();
				}
				break;

			case  5://小米推送
				log.info("===== 小米推送 ======推送包名：" + packName +",推送token："+token+".");
				if (isPushConfigNull(packName,token)){ return; }
				XMPushService.pushToRegId(notice, notice.getFileName(),deviceInfo,false, true);
				break;

			case  6://魅族推送
				try {
					log.info("===== 魅族推送 ======推送包名：" + packName +",推送token："+token+".");
					if (isPushConfigNull(packName,token)){ return; }
					MZPushService.varnishedMessagePush(deviceInfo, notice);
				} catch (IOException e) {
					e.printStackTrace();
				}
				break;

			case  7://IOS推送
				log.info("===== APNS（IOS）推送 ======推送包名：" + packName +",推送token："+token+".");
				if (isPushConfigNull(packName,token)){ return; }
				ApnsHttp2PushService.pushMsgToUser(deviceInfo,notice,ApnsHttp2PushService.PushEnvironment.Pro,false,true);
				break;


			case  8://极光推送
				log.info("===== 极光推送 ======推送包名：" + packName +",推送token："+token+".");
				if (isPushConfigNull(packName,token)){ return; }
				Map<String, String> content = Maps.newConcurrentMap();
				content.put("msg", notice.getText());
				content.put("regId", token);
				content.put("title", notice.getTitle());
				JPushServices.buildPushObject_ios_tagAnd_alertWithExtrasAndMessage(content,deviceInfo);
				break;

			default:
				log.info("===== 全量推送 ======");
				fullPushService.pushToDevice(notice);
		}

	}



	@Autowired
	private FullPushService fullPushService;



	/**
	 * @Description 判断包名 和 token是否为空
	 * @Date 18:29 2020/8/13
	 **/
	public boolean isPushConfigNull(String packName,String token){
		if (null == packName || null == token || StringUtil.isEmpty(packName) || StringUtil.isEmpty(token)){
			return true;
		}
		return false;
	}
}
