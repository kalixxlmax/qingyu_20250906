package com.basic.im.service;


import com.alibaba.fastjson.JSONObject;
import com.basic.im.user.model.KSession;
import com.basic.im.user.model.UserExample;
import com.basic.im.user.service.AbstractUserHandlerImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

@Service
public class UserHandlerImpl extends AbstractUserHandlerImpl {


    @Autowired
    protected ApplicationContext applicationContext;


    @Override
    public void addFriendsHandler(Integer userId, Integer toUserId) {

    }

    @Override
    public void publishEvent(Object event) {
        applicationContext.publishEvent(event);
    }

}
