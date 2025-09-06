package com.basic.im.push.server;

import com.alibaba.fastjson.JSON;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.config.XMPPConfig;
import com.basic.im.message.IMessageRepository;
import com.basic.im.message.MessageType;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractMessagePushService implements InitializingBean {

	protected final Logger log = LoggerFactory.getLogger(this.getClass());
	/**
	 * 获取系统号
	 */
	@Resource(name="systemAdminMap")
	protected Map<String,String> systemAdminMap;

	@Autowired(required = false)
	protected IMessageRepository messageRepository;

	@Autowired(required = false)
	protected XMPPConfig xmppConfig;



	public void initSystemUser(){
		Map<String, String> systemMap = systemAdminMap;

		log.info(" systemAdminMap {}", JSON.toJSONString(systemMap));
		List<String> mapKeyList = new ArrayList<String>(systemMap.keySet());
		for(int i = 0; i < mapKeyList.size(); i++){
			try {
				messageRepository.registerSystemNo(mapKeyList.get(i), DigestUtils.md5Hex(systemMap.get(mapKeyList.get(i))));
			} catch (Exception e) {
				log.error(e.getMessage(),e);
			}
		}
	}
	



	private byte[] generateId(String username) throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("SHA-256");
		return md.digest(username.getBytes());
	}

	public abstract void onMessage(MessageBean message);



	public abstract void sendGroup(MessageBean message);

	public abstract void sendBroadCast(MessageBean message);

	public abstract void send(MessageBean message);

	public boolean isNeedSeqNo(int type){
		/**
		 * 用户信息更新不需要序列号
		 */
		if(806==type){
			return true;
		}else if(8==(type/100)||9==(type/100)||MessageType.OPENREDPAKET==type||45==type){
			/**
			 * 领取红包消息不要序列号
			 */
			return false;
		}else if(MessageType.inviteJoinRoom==type){
			return true;
		}else if(type>=MessageType.OFFLINE&&type>=MessageType.JOINLIVE) {
			/**
			 * 直播间消息
			 */
			return false;
		}else if(type>=MessageType.LiveRoomSignOut&&type>=MessageType.LiveRoomSettingAdmin) {
			/**
			 * 直播间消息
			 */
			return false;
		}else if(type>=MessageType.RemoveLiveRoom&&type>=MessageType.UpdateLiveRoomNameOrNotive) {
			/**
			 * 直播间消息
			 */
			return false;
		}
		return true;
	}
}
