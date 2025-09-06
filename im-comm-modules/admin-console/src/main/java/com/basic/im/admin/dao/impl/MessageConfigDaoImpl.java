package com.basic.im.admin.dao.impl;

import com.basic.im.admin.dao.MessageConfigDao;
import com.basic.im.admin.entity.MessageConfig;
import com.basic.im.repository.MongoRepository;
import org.springframework.stereotype.Repository;


/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2020/5/25
 **/
@Repository
public class MessageConfigDaoImpl extends MongoRepository<MessageConfig, Long> implements MessageConfigDao {
    @Override
    public Class<MessageConfig> getEntityClass() {
        return MessageConfig.class;
    }


    @Override
    public MessageConfig getMessageConfig() {
        return findOne(createQuery());
    }

    @Override
    public MessageConfig setMessageConfig(MessageConfig messageConfig) {
        MessageConfig save = save(messageConfig);
        return save;
    }
}
