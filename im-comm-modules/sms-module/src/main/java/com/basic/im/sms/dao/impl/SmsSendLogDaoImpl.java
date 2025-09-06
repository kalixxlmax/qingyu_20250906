package com.basic.im.sms.dao.impl;

import com.basic.im.repository.MongoRepository;
import com.basic.im.sms.dao.SmsSendLogDao;
import com.basic.im.sms.entity.SmsSendLog;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Repository;


@Repository
public class SmsSendLogDaoImpl extends MongoRepository<SmsSendLog, ObjectId> implements SmsSendLogDao {

    @Override
    public Class<SmsSendLog> getEntityClass() {
        return SmsSendLog.class;
    }
}
