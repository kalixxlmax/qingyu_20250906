package com.basic.payment.service.impl;

import com.basic.payment.entity.PaymentChannelDO;
import com.basic.payment.repository.PaymentChannelRepository;
import com.basic.payment.repository.PaymentRedisRepository;
import com.basic.payment.service.PaymentChannelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;

@Service
public class PaymentChannelServiceImpl implements PaymentChannelService {

    @Autowired
    private PaymentChannelRepository paymentChannelRepository;

    @Autowired
    private PaymentRedisRepository paymentRedisRepository;

    @Override
    public Set<String> queryOpenPaymentChannelIds() {
       Set<String> channelIds=paymentRedisRepository.queryPaymentChannel();
       if(null==channelIds||0==channelIds.size()){
           channelIds=paymentChannelRepository.queryOpenPaymentChannelIds();
           paymentRedisRepository.savePaymentChannel(channelIds);
       }
       return channelIds;
    }
    @Override
    public List<PaymentChannelDO> queryOpenPaymentChannel() {
        return paymentChannelRepository.queryOpenPaymentChannel();
    }

    @Override
    public boolean addPaymentChannel(PaymentChannelDO paymentChannelDO) {
         paymentChannelRepository.addPaymentChannel(paymentChannelDO);
         paymentRedisRepository.deletePaymentChannel();
         return true;
    }

    @Override
    public PaymentChannelDO queryPaymentChannel(String channelId) {
        return paymentChannelRepository.queryPaymentChannel(channelId);
    }



    @Override
    public boolean updatePaymentChannelStatus(String channelId, byte status) {
         paymentChannelRepository.updatePaymentChannelStatus(channelId,status);
         paymentRedisRepository.deletePaymentChannel();
        return true;
    }
    @Override
    public boolean paymentChannelIsEnable(String channelId){

        return queryOpenPaymentChannelIds().contains(channelId);
    }
}
