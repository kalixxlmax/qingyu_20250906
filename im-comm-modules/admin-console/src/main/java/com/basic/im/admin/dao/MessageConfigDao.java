package com.basic.im.admin.dao;

import com.basic.im.admin.entity.MessageConfig;
import com.basic.mongodb.springdata.IBaseMongoRepository;

/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2020/5/25
 **/
public interface MessageConfigDao extends IBaseMongoRepository<MessageConfig, Long> {

    //获取配置
    MessageConfig getMessageConfig();

    //设置配置
    MessageConfig setMessageConfig(MessageConfig messageConfig);
}
