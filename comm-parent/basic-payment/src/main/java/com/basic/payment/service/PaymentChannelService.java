package com.basic.payment.service;

import com.basic.payment.entity.PaymentChannelDO;

import java.util.List;
import java.util.Set;

public interface PaymentChannelService  {

    Set<String> queryOpenPaymentChannelIds();

    /**
     * 查询支付支付方式
     * @return
     */
    List<PaymentChannelDO> queryOpenPaymentChannel();


    boolean addPaymentChannel(PaymentChannelDO paymentChannelDO);


    PaymentChannelDO queryPaymentChannel(String channelId);

    boolean updatePaymentChannelStatus(String channelId, byte status);


    boolean paymentChannelIsEnable(String channelId);
}
