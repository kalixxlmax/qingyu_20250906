package com.basic.im.pay.dao;

import com.basic.im.pay.entity.PayOrder;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

public interface PayOrderDao extends IMongoDAO<PayOrder, ObjectId> {

    void addPayOrder(PayOrder payOrder);

    PayOrder getPayOrder(ObjectId prepayId,String appId);
}
