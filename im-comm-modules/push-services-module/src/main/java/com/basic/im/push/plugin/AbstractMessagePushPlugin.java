package com.basic.im.push.plugin;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.basic.im.push.vo.MsgNotice;
import com.basic.im.push.vo.PushMessageDTO;
import com.basic.im.user.service.UserCoreRedisRepository;
import com.basic.im.user.service.UserCoreService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @description: AbstractMessagePushPlugin <br>
 * @date: 2020/7/28 0028  <br>
 * @author: lidaye <br>
 * @version: 1.0 <br>
 */
@Slf4j
public abstract class AbstractMessagePushPlugin {


    @Autowired
    protected UserCoreService userCoreService;



    @Autowired
    protected UserCoreRedisRepository userCoreRedisRepository;


    public boolean hanlderPushMessage(JSONObject messageBean){
        try {
            PushMessageDTO pushMessageDTO= JSON.parseObject(messageBean.toJSONString(),PushMessageDTO.class);

            MsgNotice msgNotice = parseMsgNotice(pushMessageDTO);
            pushOne(msgNotice.getTo(),msgNotice);

        }catch (Exception e){
            log.error(e.getMessage(),e);
            return false;
        }

        return true;

    }

    protected void pushOne(int to, MsgNotice msgNotice) {


    }


    protected MsgNotice parseMsgNotice(PushMessageDTO message) throws Exception {

        return null;
    }

}
