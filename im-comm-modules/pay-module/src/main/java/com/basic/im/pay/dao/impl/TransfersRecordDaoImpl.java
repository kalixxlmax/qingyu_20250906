package com.basic.im.pay.dao.impl;

import com.basic.im.pay.dao.TransfersRecordDao;
import com.basic.im.pay.entity.TransfersRecord;
import com.basic.im.repository.MongoRepository;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Repository;

@Repository
public class TransfersRecordDaoImpl extends MongoRepository<TransfersRecord, ObjectId> implements TransfersRecordDao {

    @Override
    public Class<TransfersRecord> getEntityClass() {
        return TransfersRecord.class;
    }

    @Override
    public void addTransfersRecord(TransfersRecord entity) {
        getDatastore().save(entity);
    }


}
