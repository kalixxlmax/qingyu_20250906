package com.basic.im.push.rocketmq;

import com.alibaba.fastjson.JSON;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.push.server.AbstractMessagePushService;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
@Component
@RocketMQMessageListener(topic = "xmppMessage", consumerGroup = "my-consumer-xmpppush")
public class SkXmppMsgListenerConcurrentlyMQ  implements RocketMQListener<String>{
	
	private static final Logger log = LoggerFactory.getLogger(SkXmppMsgListenerConcurrentlyMQ.class);
	

	@Resource
	private RocketMQTemplate rocketMQTemplate;


	/*@Resource
	private XMPPConfig xmppConfig;

	@Autowired
	private UserCoreService userCoreService;

	@Autowired
	private UserCoreRedisRepository userCoreRedisRepository;*/


	@Autowired
	private AbstractMessagePushService messagePushService;
	

	
	@Override
	public void onMessage(String body) {
		MessageBean message = null;
		String messageId = "未知";
		try {
			log.info("[消息处理流程-步骤12] RocketMQ消费者接收到新消息 - 消息体长度: {}", body != null ? body.length() : 0);
			log.debug("[消息处理流程-步骤12] 接收到的完整消息内容: {}", body);
			
			log.debug("[消息处理流程-步骤13] 开始解析JSON消息体为MessageBean对象");
			message = JSON.parseObject(body, MessageBean.class);
			
			if(null != message){
				messageId = message.getMessageId() != null ? message.getMessageId() : "未设置";
				log.info("[消息处理流程-步骤13-成功] JSON解析成功 - 消息ID: {}, 发送者: {}, 接收者: {}, 消息类型: {}", 
						messageId, message.getFromUserId(), message.getToUserId(), message.getType());
				
				log.info("[消息处理流程-步骤14] 将消息传递给MessagePushService处理 - 消息ID: {}", messageId);
				messagePushService.onMessage(message);
				log.info("[消息处理流程-步骤14-完成] MessagePushService处理完成 - 消息ID: {}", messageId);
			} else {
				log.warn("[消息处理流程-步骤13-失败] JSON解析结果为null - 原始消息: {}", body);
			}
		} catch (Exception e) {
			log.error("[消息处理流程-步骤12-异常] RocketMQ消息处理异常 - 消息ID: {}, 异常信息: {}, 原始消息: {}", messageId, e.getMessage(), body, e);
			log.info("[消息处理流程-步骤15] 消息处理失败，准备重新放入队列 - 消息ID: {}", messageId);
			sendAgainToMQ(message);
		}
	}


	/**
	 * 将消息重新放入队列
	 * @param messageBean
	 */
	public synchronized void sendAgainToMQ(MessageBean messageBean){
		String messageId = messageBean != null && messageBean.getMessageId() != null ? messageBean.getMessageId() : "未知";
		try {
			if(messageBean != null) {
				log.info("[消息处理流程-步骤15] 开始重新发送消息到RocketMQ队列 - 消息ID: {}", messageId);
				rocketMQTemplate.convertAndSend("xmppMessage", messageBean.toString());
				log.info("[消息处理流程-步骤15-成功] 消息重新放入队列成功 - 消息ID: {}", messageId);
			} else {
				log.warn("[消息处理流程-步骤15-失败] 消息对象为null，无法重新放入队列");
			}
		} catch (Exception e) {
			log.error("[消息处理流程-步骤15-异常] 重新放入队列失败 - 消息ID: {}, 异常信息: {}", messageId, e.getMessage(), e);
		}
	}
	

}
