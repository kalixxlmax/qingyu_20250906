package com.basic.im.pay.dao.impl;

import com.basic.im.pay.dao.PayOrderDao;
import com.basic.im.pay.entity.PayOrder;
import com.basic.im.repository.MongoRepository;
import com.basic.utils.StringUtil;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

/**
 * @author zhm
 * @version V1.0
 * @Description: TODO(todo)
 * @date 2019/9/2 17:35
 */
@Repository
public class PayOrderDaoImpl extends MongoRepository<PayOrder, ObjectId> implements PayOrderDao {



    @Override
    public Class<PayOrder> getEntityClass() {
        return PayOrder.class;
    }

    @Override
    public void addPayOrder(PayOrder payOrder) {
        getDatastore().save(payOrder);
    }

    @Override
    public PayOrder getPayOrder(ObjectId prepayId, String appId) {
        Query query =createQuery(prepayId);
        if(!StringUtil.isEmpty(appId)){
           addToQuery(query,"appId",appId);
        }
        return findOne(query);
    }
}
