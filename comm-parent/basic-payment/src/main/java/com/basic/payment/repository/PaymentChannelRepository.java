package com.basic.payment.repository;

import com.basic.mongodb.springdata.IBaseMongoRepository;
import com.basic.payment.dto.PaySuccessDTO;
import com.basic.payment.entity.PaymentChannelDO;
import com.basic.payment.entity.PaymentOrderDO;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Set;

public interface PaymentChannelRepository extends IBaseMongoRepository<PaymentChannelDO, ObjectId> {

    Set<String> queryOpenPaymentChannelIds();

    /**
     * 查询支付支付方式
     * @return
     */
    List<PaymentChannelDO> queryOpenPaymentChannel();


    boolean addPaymentChannel(PaymentChannelDO paymentChannelDO);


    boolean updatePaymentChannelStatus(String channelId, byte status);


    PaymentChannelDO queryPaymentChannel(String channelId);
}
