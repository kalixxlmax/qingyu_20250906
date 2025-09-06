package com.basic.im.push.rocketmq;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.constant.TopicConstant;
import com.basic.im.message.MessageType;
import com.basic.im.push.plugin.CustomerMessagePushPlugin;
import com.basic.im.push.plugin.DefaultMessagePushPlugin;
import com.basic.utils.DateUtil;
import com.basic.utils.StringUtil;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

/**
 * 推送内容支持国际化
 * 不考虑国际化使用PushMessageListenerConcurrently
 */
@Service
@ConditionalOnProperty(prefix = "im.pushConfig", name = "IsOpen", havingValue = "0", matchIfMissing = true)
@RocketMQMessageListener(topic = TopicConstant.PUSH_MESSAGE_TOPIC, consumerGroup = "GID_consumer_pushMessage")
public class PushMessageInternationListenerConcurrently implements RocketMQListener<String> {



    private static final Logger log = LoggerFactory.getLogger(PushMessageInternationListenerConcurrently.class);


    @Autowired
    private CustomerMessagePushPlugin customerMessagePushPlugin;


    @Autowired
    private DefaultMessagePushPlugin defaultMessagePushPlugin;


    @Override
    public void onMessage(String body) {
        JSONObject jsonMsg = null;
        try {
            if (KConstants.isDebug) {
                log.info(" new msg ==> " + body);
            }
            try {
                jsonMsg = JSON.parseObject(body);
				if(null!=jsonMsg.get("timeSend")){
					if((DateUtil.currentTimeSeconds()-KConstants.Expire.HOUR)>jsonMsg.getLong("timeSend")) {
						return;
					}
				}
                if((null!=jsonMsg.get("srvId") && !StringUtil.isEmpty(jsonMsg.get("srvId")+"")) || MessageType.VISITOR_STATUS_NOTICE == jsonMsg.getInteger("type")){
                    /**
                     * 客服相关消息
                     */
                    customerMessagePushPlugin.hanlderPushMessage(jsonMsg);
                    return;
                }
                defaultMessagePushPlugin.hanlderPushMessage(jsonMsg);

            } catch (Exception e) {
                log.error(e.getMessage(), e);
                return;
            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
			/*try {
				if((DateUtil.currentTimeSeconds()-KConstants.Expire.HOUR)>jsonMsg.getLong("timeSend")) {
					return;
				}
			} catch (Exception e2) {
				return;
			}*/
//			reSendPushToMq(body);
        }
    }




}
