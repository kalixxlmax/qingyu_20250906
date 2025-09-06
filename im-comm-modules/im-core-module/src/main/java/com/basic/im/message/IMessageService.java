package com.basic.im.message;

import com.basic.im.comm.model.MessageBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface IMessageService  {



    void send(MessageBean messageBean);

    void sendMessageByOrder(MessageBean messageBean,String orderKey);

    void publishMessageToMQ(String topic,String message);

    void syncSendMsgToGroupByJid(String jid, MessageBean messageBean);
}
