package com.basic.im.pay.dao;

import com.basic.im.pay.entity.TransfersRecord;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

/**
 * 提现记录
 */
public interface TransfersRecordDao extends IMongoDAO<TransfersRecord, ObjectId> {

    void addTransfersRecord(TransfersRecord entity);

}
