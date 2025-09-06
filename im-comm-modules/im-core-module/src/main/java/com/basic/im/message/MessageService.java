package com.basic.im.message;

import com.basic.commons.thread.ThreadUtils;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.config.MQConfig;
import com.basic.im.constant.TopicConstant;
import com.basic.im.model.PressureParam;
import com.basic.im.support.Callback;
import com.basic.im.utils.MqMessageSendUtil;
import com.basic.im.vo.JSONMessage;
import com.basic.utils.StringUtil;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;

import javax.annotation.Resource;
import java.util.List;

//@Repository
public abstract class MessageService implements IMessageService{

    @Resource
    @Lazy
    private RocketMQTemplate rocketMQTemplate;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());


    @Autowired(required=false)
    @Lazy
    private MQConfig mqConfig;

    //private UserCoreService userCoreService;


    public abstract void setTimeSend(com.basic.im.comm.model.MessageBean messageBean);


    @Override
    public void publishMessageToMQ(String topic, String message) {
        logger.info("开始发布消息到MQ - 主题: {}", topic);
        logger.debug("发布消息内容: {}", message);
        
        try {
            if(StringUtil.isEmpty(topic)){
                logger.warn("主题为空，无法发送消息 - 消息内容: {}", message);
                return;
            }
            
            logger.debug("准备同步发送消息到主题: {}", topic);
            SendResult result = rocketMQTemplate.syncSend(topic, message);
            
            if(SendStatus.SEND_OK!=result.getSendStatus()){
                logger.error("MQ消息发送失败 - 主题: {}, 状态: {}, 结果: {}", 
                           topic, result.getSendStatus(), result.toString());
            }else{
                logger.info("MQ消息发送成功 - 主题: {}, 队列ID: {}, 偏移量: {}", 
                          topic, result.getMessageQueue().getQueueId(), result.getQueueOffset());
            }
        } catch (Exception e) {
            logger.error("发布消息到MQ异常 - 主题: {}, 异常信息: {}", topic, e.getMessage(), e);
        }
    }


    @Override
    public void sendMessageByOrder(MessageBean messageBean, String orderKey) {
        logger.info("开始有序发送消息 - 发送者: {}, 接收者: {}, 排序键: {}", 
                   messageBean.getFromUserId(), messageBean.getToUserId(), orderKey);
        
        setTimeSend(messageBean);
        
        if (StringUtil.isEmpty(messageBean.getMessageId())) {
            messageBean.setMessageId(StringUtil.randomUUID());
            logger.debug("生成消息ID: {}", messageBean.getMessageId());
        }
        
        logger.info("准备有序发送到主题: {}, 消息ID: {}, 排序键: {}", 
                   TopicConstant.XMPP_MESSAGE_TOPIC, messageBean.getMessageId(), orderKey);
        logger.debug("有序发送消息内容: {}", messageBean.toString());
        
        try {
            MqMessageSendUtil.sendOrderly(TopicConstant.XMPP_MESSAGE_TOPIC, messageBean.toString(), orderKey, false);
            logger.info("有序消息发送完成 - 消息ID: {}, 排序键: {}", messageBean.getMessageId(), orderKey);
        } catch (Exception e) {
            logger.error("有序发送消息异常 - 消息ID: {}, 排序键: {}, 异常信息: {}", 
                       messageBean.getMessageId(), orderKey, e.getMessage(), e);
        }
    }


    /** 发送单聊消息
     * @param messageBean
     */
    public void send(com.basic.im.comm.model.MessageBean messageBean){
        logger.info("[消息处理流程-步骤7] MessageService开始处理消息发送 - 发送者: {}, 接收者: {}, 消息类型: {}, 聊天类型: {}", 
                   messageBean.getFromUserId(), messageBean.getToUserId(), messageBean.getType(), 
                   messageBean.getMsgType() == 0 ? "单聊" : (messageBean.getMsgType() == 1 ? "群聊" : "广播"));
        
        logger.debug("[消息处理流程-步骤8] 设置消息发送时间戳");
        setTimeSend(messageBean);
        logger.debug("[消息处理流程-步骤8-完成] 消息发送时间戳已设置: {}", messageBean.getTimeSend());

        if(StringUtil.isEmpty(messageBean.getMessageId())){
            messageBean.setMessageId(StringUtil.randomUUID());
            logger.debug("[消息处理流程-步骤9] 生成新的消息ID: {}", messageBean.getMessageId());
        } else {
            logger.debug("[消息处理流程-步骤9] 使用已有消息ID: {}", messageBean.getMessageId());
        }

        logger.info("[消息处理流程-步骤10] 准备发送消息到RocketMQ消息队列 - 主题: xmppMessage, 消息ID: {}", messageBean.getMessageId());
        logger.debug("[消息处理流程-步骤10] 完整消息内容: {}", messageBean.toString());

        try {
            logger.debug("[消息处理流程-步骤11] 开始同步发送消息到RocketMQ");
            SendResult result = rocketMQTemplate.syncSend("xmppMessage", messageBean.toString());
            
            if(SendStatus.SEND_OK!=result.getSendStatus()){
                logger.error("[消息处理流程-步骤11-失败] RocketMQ消息发送失败 - 消息ID: {}, 发送结果: {}, 状态: {}", 
                           messageBean.getMessageId(), result.toString(), result.getSendStatus());
            }else{
                logger.info("[消息处理流程-步骤11-成功] RocketMQ消息发送成功 - 消息ID: {}, 队列ID: {}, 偏移量: {}, 事务ID: {}", 
                          messageBean.getMessageId(), result.getMessageQueue().getQueueId(), 
                          result.getQueueOffset(), result.getTransactionId());
            }
        } catch (Exception e) {
            logger.error("[消息处理流程-步骤11-异常] RocketMQ发送消息异常 - 消息ID: {}, 异常信息: {}", messageBean.getMessageId(), e.getMessage(), e);
            throw new RuntimeException("消息发送失败: " + e.getMessage(), e);
        }
        
        logger.info("[消息处理流程-步骤11-完成] MessageService消息发送流程结束 - 消息ID: {}, 消息已进入RocketMQ队列等待消费", messageBean.getMessageId());
    }


    /**
     * 发送单聊消息
     * @param messageBean
     * @param userIdList
     */
    public void send(com.basic.im.comm.model.MessageBean messageBean, List<Integer> userIdList){
        logger.info("开始批量发送单聊消息 - 发送者: {}, 接收者数量: {}, 消息类型: {}", 
                   messageBean.getFromUserId(), userIdList.size(), messageBean.getType());
        logger.debug("接收者列表: {}", userIdList);
        
        setTimeSend(messageBean);
        if(StringUtil.isEmpty(messageBean.getMessageId())){
            messageBean.setMessageId(StringUtil.randomUUID());
            logger.debug("生成批量消息基础ID: {}", messageBean.getMessageId());
        }

        int successCount = 0;
        int failCount = 0;
        
        for(Integer i:userIdList){
            try {
                messageBean.setToUserId(i.toString());
                //messageBean.setToUserName(userCoreService.getNickName(i));
                messageBean.setMsgType(0);// 单聊消息
                
                logger.debug("发送消息给用户: {}", i);
                send(messageBean);
                successCount++;
            } catch (Exception e) {
                failCount++;
                logger.error("发送消息给用户 {} 失败 - 异常信息: {}", i, e.getMessage(), e);
            }
        }
        
        logger.info("批量单聊消息发送完成 - 成功: {}, 失败: {}, 总数: {}", 
                   successCount, failCount, userIdList.size());
    }

    /**
     * 发送 消息 到 群组中
     */
    public void sendMsgToMucRoom(MessageBean messageBean, String... roomJidArr){
        logger.info("开始发送群组消息 - 发送者: {}, 群组数量: {}, 消息类型: {}", 
                   messageBean.getFromUserId(), roomJidArr.length, messageBean.getType());
        logger.debug("目标群组列表: {}", java.util.Arrays.toString(roomJidArr));
        
        setTimeSend(messageBean);

        if(StringUtil.isEmpty(messageBean.getMessageId())){
            messageBean.setMessageId(StringUtil.randomUUID());
            logger.debug("生成群组消息ID: {}", messageBean.getMessageId());
        }
        
        int successCount = 0;
        int failCount = 0;
        
        for (String jid : roomJidArr) {
            try {
                logger.debug("准备发送消息到群组: {}", jid);
                messageBean.setMsgType(1);
                messageBean.setRoomJid(jid);
                
                logger.debug("发送群组消息内容: {}", messageBean.toString());
                SendResult result = rocketMQTemplate.syncSend("xmppMessage",messageBean.toString());
                
                if(SendStatus.SEND_OK!=result.getSendStatus()){
                    logger.error("群组消息发送失败 - 群组: {}, 消息ID: {}, 状态: {}, 结果: {}", 
                               jid, messageBean.getMessageId(), result.getSendStatus(), result.toString());
                    failCount++;
                }else{
                    logger.info("群组消息发送成功 - 群组: {}, 消息ID: {}, 队列ID: {}", 
                              jid, messageBean.getMessageId(), result.getMessageQueue().getQueueId());
                    successCount++;
                }
            } catch (Exception e) {
                failCount++;
                logger.error("发送群组消息异常 - 群组: {}, 消息ID: {}, 异常信息: {}", 
                           jid, messageBean.getMessageId(), e.getMessage(), e);
            }
        }
        
        logger.info("群组消息发送完成 - 成功: {}, 失败: {}, 总群组数: {}", 
                   successCount, failCount, roomJidArr.length);
    }


    /** @Description:（群控制消息）
     * @param jid
     * @param messageBean
     * @throws Exception
     **/
    public void sendMsgToGroupByJid(String jid, MessageBean messageBean){
        logger.info("异步发送群控制消息 - 群组: {}, 发送者: {}, 消息类型: {}", 
                   jid, messageBean.getFromUserId(), messageBean.getType());
        
        setTimeSend(messageBean);
        
        try {
            ThreadUtils.executeInThread((Callback) obj -> {
                logger.debug("在异步线程中执行群控制消息发送 - 群组: {}", jid);
                sendMsgToMucRoom(messageBean, jid);
            });
            logger.debug("群控制消息已提交到异步线程 - 群组: {}", jid);
        } catch (Exception e) {
            logger.error("提交群控制消息到异步线程失败 - 群组: {}, 异常信息: {}", jid, e.getMessage(), e);
        }
    }

    @Override
    public void syncSendMsgToGroupByJid(String jid, MessageBean messageBean){
        logger.info("同步发送群组消息 - 群组: {}, 发送者: {}, 消息类型: {}", 
                   jid, messageBean.getFromUserId(), messageBean.getType());
        
        setTimeSend(messageBean);
        
        try {
            ThreadUtils.executeInThread((Callback) obj -> {
                logger.debug("在异步线程中执行同步群组消息发送 - 群组: {}", jid);
                sendMsgToMucRoom(messageBean, jid);
            });
            logger.debug("同步群组消息已提交到异步线程 - 群组: {}", jid);
        } catch (Exception e) {
            logger.error("提交同步群组消息到异步线程失败 - 群组: {}, 异常信息: {}", jid, e.getMessage(), e);
        }
    }

    public void sendManyMsgToGroupByJid(String jid,List<MessageBean> messageList) {
        logger.info("开始批量发送群组消息 - 群组: {}, 消息数量: {}", jid, messageList.size());
        
        int successCount = 0;
        int failCount = 0;
        
        try {
            for(MessageBean messageBean : messageList){
                try {
                    logger.debug("发送第 {} 条群组消息 - 群组: {}, 消息类型: {}", 
                               successCount + failCount + 1, jid, messageBean.getType());
                    
                    setTimeSend(messageBean);
                    sendMsgToMucRoom(messageBean, jid);
                    successCount++;
                } catch (Exception e) {
                    failCount++;
                    logger.error("发送第 {} 条群组消息失败 - 群组: {}, 异常信息: {}", 
                               successCount + failCount, jid, e.getMessage(), e);
                }
            }
        }catch (Exception e) {
            logger.error("批量发送群组消息异常 - 群组: {}, 异常信息: {}", jid, e.getMessage(), e);
        }
        
        logger.info("批量群组消息发送完成 - 群组: {}, 成功: {}, 失败: {}, 总数: {}", 
                   jid, successCount, failCount, messageList.size());
    }

    public void send(List<Integer> userIdList, List<MessageBean> messageList)  {
        logger.info("开始异步批量发送消息 - 用户数量: {}, 消息数量: {}", userIdList.size(), messageList.size());
        logger.debug("用户列表: {}", userIdList);
        
        ThreadUtils.executeInThread(new Callback() {

            @Override
            public void execute(Object obj) {
                logger.debug("在异步线程中开始执行批量消息发送");
                
                int totalMessages = 0;
                int successCount = 0;
                int failCount = 0;

                try {
                    for (MessageBean messageBean : messageList) {
                        for (int userId : userIdList) {
                            totalMessages++;
                            try {
                                setTimeSend(messageBean);
                                messageBean.setMsgType(0);// 单聊消息
                                
                                if(messageBean.getToUserId().equals(String.valueOf(userId))){
                                    logger.debug("发送消息给匹配用户: {}, 消息类型: {}", userId, messageBean.getType());
                                    send(messageBean);
                                    successCount++;
                                } else {
                                    logger.debug("跳过不匹配用户: {}, 目标用户: {}", userId, messageBean.getToUserId());
                                }
                            } catch (Exception e) {
                                failCount++;
                                logger.error("发送消息给用户 {} 失败 - 异常信息: {}", userId, e.getMessage(), e);
                            }
                        }
                    }
                    
                    logger.info("异步批量消息发送完成 - 总处理: {}, 成功: {}, 失败: {}", 
                               totalMessages, successCount, failCount);
                } catch (Exception e1) {
                    logger.error("异步批量发送消息异常 - 异常信息: {}", e1.getMessage(), e1);
                }
            }
        });
        
        logger.debug("批量消息发送任务已提交到异步线程");
    }
    //发送消息验证
    public void pushAuthLoginDeviceMessage(Integer userId,String strKey){
        logger.info("开始发送设备登录验证消息 - 用户ID: {}", userId);
        logger.debug("验证密钥: {}", strKey);
        
        try {
            MessageBean messageBean = new MessageBean();
            //信息编号
            messageBean.setType(MessageType.AUTHLOGINDEVICE);
            //信息接受方
            messageBean.setTo(userId.toString());
            //信息推送方
            messageBean.setFromUserId(userId.toString());

            messageBean.setToUserId(userId.toString());
            //信息推送方名称
            messageBean.setFromUserName("localhost");
            //信息类型
            messageBean.setMsgType(0);
            //推送的信息
            messageBean.setContent(strKey);
            //信息编号
            String messageId = StringUtil.randomUUID();
            messageBean.setMessageId(messageId);
            
            logger.debug("设备验证消息构建完成 - 消息ID: {}, 用户ID: {}", messageId, userId);
            
            //发送消息
            send(messageBean);
            
            logger.info("设备登录验证消息发送完成 - 用户ID: {}, 消息ID: {}", userId, messageId);
        } catch (Exception e) {
            logger.error("发送设备登录验证消息异常 - 用户ID: {}, 异常信息: {}", userId, e.getMessage(), e);
        }
    }

    public abstract void createMucRoomToIMServer(String roomJid,String password,String userId, String roomName);


    public abstract Number createTimeSendAdd(int num);

    public abstract void deleteTigaseUser(Integer userId);

    public abstract JSONMessage pressureMucTest(PressureParam param, Integer userId);
}
