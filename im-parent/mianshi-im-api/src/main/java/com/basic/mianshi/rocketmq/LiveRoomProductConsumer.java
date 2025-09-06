package com.basic.mianshi.rocketmq;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.basic.im.live.entity.LiveRoom;
import com.basic.im.live.service.impl.LiveRoomManagerImpl;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * LiveRoomProductConsumer <br>
 *
 * @author: lidaye <br>
 * @date: 2021/3/2 0002  <br>
 */
//@Component
//@RocketMQMessageListener(topic = "live-product-order", consumerGroup = "consumer-live-product-order")
public class LiveRoomProductConsumer implements RocketMQListener<String> {

    private static final Logger logger = LoggerFactory.getLogger(LiveRoomProductConsumer.class);


    @Autowired
    private LiveRoomManagerImpl liveRoomManager;
    @Override
    public void onMessage(String message) {
        logger.info("onmessage {}",message);


        JSONObject orderData= JSON.parseObject(message);


    }
}
