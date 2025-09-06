package com.basic.im.user.dao.impl;

import com.basic.im.repository.MongoRepository;
import com.basic.im.user.dao.MoneyLogDao;
import com.basic.im.user.entity.UserMoneyLog;
import com.basic.im.utils.MongoUtil;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

@Repository
public class MoneyLogDaoImpl extends MongoRepository<UserMoneyLog, ObjectId> implements MoneyLogDao {

    @Override
    public Class<UserMoneyLog> getEntityClass() {
        return UserMoneyLog.class;
    }

    @Override
    public boolean saveMoneyLog(UserMoneyLog userMoneyLog) {
        userMoneyLog.setCreateTime(System.currentTimeMillis());
       return null!=getDatastore().save(userMoneyLog);
    }

    @Override
    public boolean isExistMoneyLogProcessed(UserMoneyLog userMoneyLog) {
        Query query =createQuery("userId",userMoneyLog.getUserId());
        addToQuery(query,"businessId",userMoneyLog.getBusinessId());
        addToQuery(query,"businessType",userMoneyLog.getBusinessType());
        addToQuery(query,"logType",userMoneyLog.getLogType());

        if(0<userMoneyLog.getSubBusinessType()){
            addToQuery(query,"subBusinessType",userMoneyLog.getSubBusinessType());
        }


        return getDatastore().exists(query,getEntityClass());

    }
    @Override
    public boolean isExistMoneyLogProcessed(UserMoneyLog userMoneyLog, byte logType) {
        Query query =createQuery("businessId",userMoneyLog.getBusinessId());
        if(0!=userMoneyLog.getUserId()) {
            addToQuery(query,"userId",userMoneyLog.getUserId());
        }

        addToQuery(query,"businessType",userMoneyLog.getBusinessType());

        addToQuery(query,"logType",logType);

        return getDatastore().exists(query,getEntityClass());

    }

    @Override
    public boolean isExistMoneyLogProcessed(String businessId,long userId,byte businessType,byte logType) {
        Query query =createQuery("businessId",businessId);
        if(0!=userId) {
            addToQuery(query,"userId",userId);
        }

        addToQuery(query,"businessType",businessType);

        addToQuery(query,"logType",logType);

        return getDatastore().exists(query,getEntityClass());

    }

    @Override
    public double countUserMoney(Integer userId) {
        Criteria addCriteria = Criteria.where("userId").is(userId).and("changeType").is(1);
        Criteria reduceCriteria = Criteria.where("userId").is(userId).and("changeType").is(2);
        Double addAmount = MongoUtil.sumAggregation(addCriteria,"userId","moeny",this.getDatastore(),this.getEntityClass(), 0.0);
        Double reduceAmount = MongoUtil.sumAggregation(reduceCriteria,"userId","moeny",this.getDatastore(),this.getEntityClass(), 0.0);
        if (addAmount!=null && addAmount>0 && reduceAmount!=null){
            return addAmount-reduceAmount;
        }
        return 0;
    }
}
