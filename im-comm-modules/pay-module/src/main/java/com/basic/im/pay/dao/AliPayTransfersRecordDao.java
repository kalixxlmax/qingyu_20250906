package com.basic.im.pay.dao;

import com.basic.im.repository.IMongoDAO;
import com.basic.im.pay.entity.AliPayTransfersRecord;
import org.bson.types.ObjectId;

public interface AliPayTransfersRecordDao extends IMongoDAO<AliPayTransfersRecord, ObjectId> {

    void addAliPayTransfersRecord(AliPayTransfersRecord aliPayTransfersRecord);
}
