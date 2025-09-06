package com.basic.payment.repository.impl;

import com.basic.mongodb.springdata.BaseMongoRepository;
import com.basic.payment.constant.PayConstant;
import com.basic.payment.entity.PaymentChannelDO;
import com.basic.payment.entity.PaymentOrderDO;
import com.basic.payment.repository.PaymentChannelRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Repository
public class PaymentChannelRepositoryImpl extends BaseMongoRepository<PaymentChannelDO, ObjectId> implements PaymentChannelRepository {

    @Override
    public Set<String> queryOpenPaymentChannelIds() {
        Query query = createQuery("status", PayConstant.PAY_CHANNEL_ENABLE);
        List<String> channelIds = getDatastore().findDistinct(query, "channelId", getEntityClass(), String.class);
        return channelIds.stream().collect(Collectors.toSet());
    }
    @Override
    public List<PaymentChannelDO> queryOpenPaymentChannel() {
        Query query = createQuery("status", PayConstant.PAY_CHANNEL_ENABLE);
        return queryListsByQuery(query);
    }
    @Override
    public boolean addPaymentChannel(PaymentChannelDO paymentChannelDO) {

        save(paymentChannelDO);
        return true;
    }
    @Override
    public boolean updatePaymentChannelStatus(String channelId, byte status) {
        updateAttribute("channelId",channelId,"status",status);

        return true;
    }

    @Override
    public PaymentChannelDO queryPaymentChannel(String channelId) {
        return queryOne("channelId",channelId);
    }
}
