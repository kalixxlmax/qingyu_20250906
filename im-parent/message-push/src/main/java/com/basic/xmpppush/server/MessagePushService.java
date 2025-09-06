package com.basic.xmpppush.server;

import com.alibaba.fastjson.JSONObject;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.friends.service.FriendsManager;
import com.basic.im.friends.service.FriendsRedisRepository;
import com.basic.im.push.server.AbstractMessagePushService;
import com.basic.im.user.service.UserRedisService;
import com.basic.im.utils.SKBeanUtils;
import com.chat.imclient.BaseClientHandler;
import com.chat.imclient.BaseClientListener;
import com.chat.imclient.BaseIMClient;
import com.chat.imserver.common.message.AuthMessage;
import com.chat.imserver.common.message.ChatMessage;
import com.chat.imserver.common.message.MessageHead;
import com.chat.imserver.common.packets.ChatType;
import com.chat.imserver.common.utils.StringUtils;
import com.basic.utils.DateUtil;
import com.basic.utils.StringUtil;
import com.basic.xmpppush.config.IMConfig;
import com.basic.xmpppush.prometheus.CustomIndicator;
import com.google.gson.JsonObject;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.tio.core.ChannelContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@Service
public class MessagePushService extends AbstractMessagePushService {


	private Map<String,Object> messageMap=new ConcurrentHashMap<>();


	@Autowired(required = false)
	protected IMConfig imConfig;


	@Autowired(required = false)
	protected FriendsRedisRepository friendsRedisRepository;

	@Autowired(required = false)
	protected FriendsManager friendsManager;


	@Autowired(required = false)
	protected UserRedisService userRedisService;



	private List<String> sysUserList=null;
	private synchronized List<String> getUserList(){
		if(null!=sysUserList)
			return sysUserList;


		sysUserList= Collections.synchronizedList(new ArrayList<String>());
		for (String string : systemAdminMap.keySet()) {
			sysUserList.add(string);
		}
		return sysUserList;
	}


	// 新的队列
	public static  ConcurrentLinkedQueue<ChatMessage> queue = new ConcurrentLinkedQueue<>();

	@Autowired(required = false)
	private MeterRegistry registry;

	@Override
	public void afterPropertiesSet() throws Exception {
		initThread();
	}




	public  void initThread(){
		ImPushQueueThread work;
		IMClient client=null;
		/*if(!StringUtil.isEmpty(imConfig.getUserIds())){
			String[] userIdList = imConfig.getUserIds().split(",");
			for(int i=0;i<getUserList().size();i++) {
				if (getUserList().get(i) == "10000") {
					continue;
				}
			}
		}*/

		List<String> userList = getUserList();

		if(!userList.isEmpty()) {
			for (int i = 0; i < userList.size(); i++) {
				if (userList.get(i) == "10000") {
					continue;
				}

				try {
					client = getIMClient(userList.get(i));
				} catch (Exception e) {
					log.error(e.getMessage(), e);
				}
				work = new ImPushQueueThread(client);
				work.start();
			}
		}else {
			try {
				client = getIMClient("10005");
			} catch (Exception e) {
				log.error(e.getMessage(), e);
			}
			work = new ImPushQueueThread(client);
			work.start();
		}


	}

	public ChatMessage bulidChatMessage(MessageBean messageBean){
		ChatMessage message = null;
		MessageHead messageHead = null;
		try {
			message = new ChatMessage();
			messageHead = new MessageHead();
			if(!StringUtil.isEmpty(messageBean.getTo())){
				messageHead.setTo(messageBean.getTo());
			}else{
				messageHead.setTo(messageBean.getToUserId());
			}

			byte chatType= ChatType.CHAT;
			if(1==messageBean.getMsgType()) {
				chatType=ChatType.GROUPCHAT;
				messageHead.setTo(messageBean.getRoomJid());
				//messageBean.setToUserId(messageBean.getRoomJid());
			}else if(2==messageBean.getMsgType()) {
				chatType=ChatType.ALL;
			}

			messageHead.setChatType(chatType);
			if(null!=messageBean.getMessageId()) {
				messageHead.setMessageId(messageBean.getMessageId());
			} else {
				messageHead.setMessageId(StringUtils.newStanzaId());
			}
			if(null!=messageBean.getContent()) {
				message.setContent(messageBean.getContent().toString());
			}
			message.setFromUserId(messageBean.getFromUserId());
			message.setFromUserName(messageBean.getFromUserName());
			message.setToUserId(messageBean.getToUserId());
			message.setToUserName(messageBean.getToUserName());
			message.setType((short)messageBean.getType());
			message.setSubType(messageBean.getSubType());
			//messageBean.setTimeSend(timeSend);
			if (null != messageBean.getTimeSend()) {
				message.setTimeSend(Long.parseLong(messageBean.getTimeSend().toString()));
			}

			if (null != messageBean.getObjectId()) {
				message.setObjectId(messageBean.getObjectId().toString());
			}
			if (null != messageBean.getFileName()) {
				message.setFileName(messageBean.getFileName());
			}
			if(0!=messageBean.getFileSize()){
				message.setFileSize(messageBean.getFileSize());
			}

			if(0!=messageBean.getTimeLen()){
				message.setFileTime(messageBean.getTimeLen());
			}

			message.setOther(messageBean.getOther());


			message.setMessageHead(messageHead);

			if(isNeedSeqNo(message.getType())) {
				message.setSeqNo(-1);
			}else if(ChatType.GROUPCHAT == messageHead.getChatType()&&(9==(message.getType()/100)||806==message.getType())){
				message.setSeqNo(-1);
			}


			/**
			 * 回复给访客的消息
			 */
			if( null != messageBean.getSrvId() && 0 != messageBean.getSrvId()){
				message.setSrvId(messageBean.getSrvId());
			}

		} catch (Exception e) {
			log.error(e.getMessage(),e);
		}
		return message;
	}


	private void setTimeSend(com.basic.im.comm.model.MessageBean messageBean){
		if(null==messageBean.getTimeSend()){
			messageBean.setTimeSend((System.currentTimeMillis()));
		}
	}

	@Override
	public void onMessage(MessageBean messageBean) {
		String messageId = messageBean != null && messageBean.getMessageId() != null ? messageBean.getMessageId() : "未知";
		
		log.info("[消息处理流程-步骤16] MessagePushService开始处理消息 - 消息ID: {}, 发送者: {}, 接收者: {}, 消息类型: {}", 
				messageId, messageBean.getFromUserId(), messageBean.getToUserId(), messageBean.getType());
		log.debug("[消息处理流程-步骤16] 完整消息对象: {}", JSONObject.toJSON(messageBean));

		ChatMessage message = null;
		try {
			log.debug("[消息处理流程-步骤17] 设置消息发送时间戳 - 消息ID: {}", messageId);
			setTimeSend(messageBean);
			log.debug("[消息处理流程-步骤17-完成] 时间戳设置完成 - 消息ID: {}, 时间戳: {}", messageId, messageBean.getTimeSend());
			
			// 检查是否为广播消息
			if(2 == messageBean.getMsgType()){
				log.info("[消息处理流程-步骤18] 检测到广播消息，转发到广播处理逻辑 - 消息ID: {}", messageId);
				sendBroadCast(messageBean);
				log.info("[消息处理流程-步骤18-完成] 广播消息处理完成 - 消息ID: {}", messageId);
				return;
			}
			
			log.debug("[消息处理流程-步骤19] 开始构建ChatMessage对象 - 消息ID: {}", messageId);
			message = bulidChatMessage(messageBean);
			
			if(null == message) {
				log.warn("[消息处理流程-步骤19-失败] ChatMessage构建失败，消息为null - 消息ID: {}", messageId);
				return;
			}
			log.debug("[消息处理流程-步骤19-成功] ChatMessage对象构建成功 - 消息ID: {}", messageId);
			
			// Prometheus指标统计
			if(null != registry) {
				log.debug("[消息处理流程-步骤20] 更新Prometheus指标统计 - 消息ID: {}", messageId);
				CustomIndicator.singleMessageSum(registry).increment();
			}
			
			log.info("[消息处理流程-步骤21] 将ChatMessage放入处理队列 - 消息ID: {}, 当前队列大小: {}", messageId, queue.size());
			queue.offer(message);
			log.info("[消息处理流程-步骤21-完成] 消息已成功放入队列 - 消息ID: {}, 队列大小: {}", messageId, queue.size());
			
		} catch (Exception e) {
			log.error("[消息处理流程-步骤16-异常] MessagePushService处理消息异常 - 消息ID: {}, 异常信息: {}", messageId, e.getMessage(), e);
		}
	}

	/**
	 * 发送单聊消息
	 * @param body
	 */
	@Override
	public void send(MessageBean body){
		try {

			// 把消息丢进queue队列中

		} catch (Exception e) {
			e.printStackTrace();
			log.error("放进队列失败! ==="+e.getMessage());
		}
	}



	/**
	 * 发送群组消息
	 * @param body
	 * @throws Exception
	 */
	@Override
	public void sendGroup(MessageBean body)  {
        setTimeSend(body);
        ChatMessage message=bulidChatMessage(body);
        if(null!=message){
        	return;
		}
		try {
			if(null!=registry) {
				//累加到Prometheus
				CustomIndicator.groupMessageSum(registry).increment();
			}
			// 把消息丢进queue队列中
			queue.offer(message);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("放进队列失败!" + (null!=message?message.toString():""));
		}

	}

	@Override
	public void sendBroadCast(MessageBean messageBean){
		List<Integer> list;
		/*list = friendsRedisRepository.getFriendsUserIdsList(Integer.valueOf(messageBean.getFromUserId()));
		if(list.size()==0){
			list =friendsManager.queryFansId(Integer.valueOf(messageBean.getFromUserId()));
			//SKBeanUtils.getRedisService().saveFriendsUserIdsList(Integer.valueOf(messageBean.getFromUserId()),list);
		}*/

		if(Integer.valueOf(messageBean.getFromUserId())>10200){
			list = friendsRedisRepository.getFriendsUserIdsList(Integer.valueOf(messageBean.getFromUserId()));
			if(list.size()==0){
				list = friendsManager.queryFollowId(Integer.valueOf(messageBean.getFromUserId()));
				//SKBeanUtils.getRedisService().saveFriendsUserIdsList(Integer.valueOf(body.getFromUserId()),list);
			}
		}else{
			//list = userRedisService.getNoSystemNumUserIds();
			Query query = new Query().addCriteria(Criteria.where("_id").gt(10200));
			list=SKBeanUtils.getDatastore().findDistinct(query,"_id","user",Integer.class);
				//SKBeanUtils.getRedisService().saveNoSystemNumUserIds(list);


		}
		for(Integer userId:list){
			ChatMessage message=null;
			MessageHead messageHead=null;
			try {
				message=new ChatMessage();
				messageHead=new MessageHead();
				messageHead.setFrom(messageBean.getFromUserId());
				messageHead.setTo(userId.toString());
				messageHead.setChatType(ChatType.CHAT);
				message.setFromUserId(messageBean.getFromUserId());
				message.setFromUserName(messageBean.getFromUserName());
				message.setToUserId(userId.toString());
				message.setToUserName(messageBean.getToUserName());
				message.setType((short)messageBean.getType());
				message.setSeqNo(-1);

				message.setTimeSend(System.currentTimeMillis());
				message.setContent(messageBean.getContent().toString());
				if(null!=messageBean.getMessageId()) {
					messageHead.setMessageId(messageBean.getMessageId());
				} else {
					messageHead.setMessageId(StringUtils.newStanzaId());
				}
				message.setMessageHead(messageHead);

				// 把消息丢进queue队列中
				queue.offer(message);
				Thread.sleep(20);
			} catch (Exception e) {
				log.error(e.getMessage(),e);


			}

		}
	}


	private IMClient getIMClient(String userId) {
		IMClient client=new IMClient();
		client.setUserId(userId);

		client.setPingTime(imConfig.getPingTime());

		BaseClientHandler clientHandler=new BaseClientHandler() {


			@Override
			public void handlerReceipt(String messageId) {
				System.out.println("handlerReceipt ===> "+messageId);
				messageMap.remove(messageId);
			}
		};
		clientHandler.setImClient(client);
		BaseClientListener clientListener=new BaseClientListener() {

			@Override
			public AuthMessage authUserMessage(ChannelContext channelContext, BaseIMClient client) {
				MessageHead messageHead=new MessageHead();

				messageHead.setChatType(ChatType.CHAT);
				channelContext.userid=userId;
				messageHead.setFrom(userId+"/Server");
				messageHead.setTo("service");
				messageHead.setMessageId(UUID.randomUUID().toString().replaceAll("-", ""));

				AuthMessage authMessage=new AuthMessage();
				authMessage.setToken(imConfig.getServerToken());
				authMessage.setPassword("");
				authMessage.setDeviceId("1111");
				authMessage.setVersion((short)4);
				authMessage.setApiKey("1");
				authMessage.setAppName("1");
				authMessage.setCompanyName("1");
				authMessage.setSecret("1");
				authMessage.setMessageHead(messageHead);
				return authMessage;
			}
		};
		clientListener.setImClient(client);
		client.initIMClient(imConfig.getHost(),imConfig.getPort(),clientHandler,clientListener);

		return client;
	}



	/**
	 * 推送Queue队列中的消息
	 * @throws InterruptedException
	 */
	public void runQueuePush(IMClient client)
			throws Exception{
		ChatMessage message = queue.poll();
		String messageId = "未知";
		String clientUserId = client != null ? client.getUserId() : "未知客户端";

		if(message == null){
			return;
		}
		
		try {
			messageId = message.getMessageHead() != null ? message.getMessageHead().getMessageId() : "未设置";
			log.info("[消息处理流程-步骤22] 队列处理线程开始处理消息 - 消息ID: {}, 处理客户端: {}, 接收者: {}", 
					messageId, clientUserId, message.getToUserId());
			
			if(null == client) {
				log.warn("[消息处理流程-步骤22-警告] 客户端为null，等待500ms - 消息ID: {}", messageId);
				Thread.sleep(500);
				return;
			}
			
			// 设置消息发送方
			if(null != message.getMessageHead()){
				String fromField = client.getUserId() + "/Server";
                message.getMessageHead().setFrom(fromField);
                log.debug("[消息处理流程-步骤23] 设置消息发送方 - 消息ID: {}, 发送方: {}", messageId, fromField);
            }
			
			log.debug("[消息处理流程-步骤24] 创建消息跟踪对象 - 消息ID: {}", messageId);
			ChatMessageVo messageVo = new ChatMessageVo();
			messageVo.setCreateTime(DateUtil.currentTimeSeconds());
			messageVo.setMessage(message);
			
			log.info("[消息处理流程-步骤25] 通过IMClient发送消息到目标客户端 - 消息ID: {}, 客户端: {}, 目标用户: {}", 
					messageId, clientUserId, message.getToUserId());
			client.sendMessage(message);
			
			// 将消息放入跟踪Map
			if(null != message.getMessageHead()) {
				messageMap.put(message.getMessageHead().getMessageId(), messageVo);
				log.debug("[消息处理流程-步骤26] 消息已加入跟踪Map - 消息ID: {}, Map大小: {}", messageId, messageMap.size());
			}
			
			log.info("[消息处理流程-步骤25-成功] 消息推送成功 - 消息ID: {}, 目标用户: {}, 处理客户端: {}", 
					messageId, message.getToUserId(), clientUserId);
					
		} catch (Exception e) {
			log.error("[消息处理流程-步骤22-异常] 队列消息处理异常，消息重新放回队列 - 消息ID: {}, 客户端: {}, 异常信息: {}", 
					 messageId, clientUserId, e.getMessage(), e);
			queue.offer(message);
		}
	}



	/**
	 *
	 * @Description: TODO(在线程中消费队列中的消息)
	 * @author Administrator
	 * @date 2018年12月26日 上午11:26:22
	 * @version V1.0
	 */
	public class ImPushQueueThread extends Thread {
		private IMClient client=null;

		public ImPushQueueThread() {}

		public ImPushQueueThread(IMClient client) {
			this.client=client;
		}

		@Override
		public void run() {
			while (true) {
				if(!queue.isEmpty()){
					try {
						runQueuePush(client);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}else{
					try {
						Thread.sleep(100);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}

	}


	@Getter
	@Setter
	public class ChatMessageVo {
		private long createTime;
		private ChatMessage message;
	}


	/**
	 * 定时重发
	 * @throws InterruptedException
	 */
	/*public void timer(){
		new Thread(new Runnable() {

			@Override
			public void run() {
				while(true){
					Long startTime=System.currentTimeMillis();
					if(messageMap.size()>10){
						log.info("开始时间"+DateUtil.currentTimeSeconds()+"   map大小   "+messageMap.size());
					}
					Set<Map.Entry<String, ChatMessageVo>> set=messageMap.entrySet();
					for (Map.Entry<String, ChatMessageVo> entry : set) {
						ChatMessageVo messageVo=entry.getValue();
						if(messageVo!=null){
							if(DateUtil.currentTimeSeconds()-messageVo.getCreateTime()>=30){
								queue.offer(messageVo.getMessage());
							}
						}else {
							return;
						}
					}

					Long endTime=System.currentTimeMillis();
					if((endTime-startTime)>1000){
						log.info("执行map所需要的时间========"+(endTime-startTime));
					}
					try {
						Thread.sleep(30000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}

		}).start();

	}*/
}
