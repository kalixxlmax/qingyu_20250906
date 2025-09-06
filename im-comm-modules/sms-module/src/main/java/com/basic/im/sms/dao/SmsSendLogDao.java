package com.basic.im.sms.dao;

import com.basic.im.repository.IMongoDAO;
import com.basic.im.sms.entity.SmsSendLog;
import org.bson.types.ObjectId;

public interface SmsSendLogDao extends IMongoDAO<SmsSendLog, ObjectId> {
}
