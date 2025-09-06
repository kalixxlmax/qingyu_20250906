package com.basic.im.pay.dao.impl;
import com.basic.im.pay.dao.AliPayTransfersRecordDao;
import com.basic.im.pay.entity.AliPayTransfersRecord;
import com.basic.im.repository.MongoRepository;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Repository;

/**
 * @author zhm
 * @version V1.0
 * @Description: TODO(todo)
 * @date 2019/9/4 9:47
 */
@Repository
public class AliPayTransfersRecordDaoImpl extends MongoRepository<AliPayTransfersRecord, ObjectId> implements AliPayTransfersRecordDao {



    @Override
    public Class<AliPayTransfersRecord> getEntityClass() {
        return AliPayTransfersRecord.class;
    }

    @Override
    public void addAliPayTransfersRecord(AliPayTransfersRecord aliPayTransfersRecord) {
        getDatastore().save(aliPayTransfersRecord);
    }
}
