package com.basic.payment.api.impl;

import com.basic.payment.api.IPaymentNoticeApiService;
import com.basic.payment.constant.PayConstant;
import com.basic.payment.dto.PayCallBackResultDTO;
import com.basic.payment.repository.PaymentOrderRepository;
import com.basic.payment.service.PayStrategyService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

/**
 * 回调通知处理
 */
@Slf4j
public abstract class AbstractPaymentNoticeService implements IPaymentNoticeApiService {





    @Autowired
    protected PayStrategyService payStrategyService;

    @Autowired
    protected PaymentOrderRepository paymentOrderRepository;




    @Override
    public String handlePayCallBackNotify(String channelId, Map<String, String> params) {
        PayCallBackResultDTO result = payStrategyService.handlePayCallBackNotify(channelId, params);

        if(result.isSuccess()&&null!=result.getPaySuccessDTO()){
            paySuccessNotify(result.getPaySuccessDTO());

        }
        return result.getResult();
    }

    @Override
    public String handlePayCallBackNotify(String channelId, String params) {
        PayCallBackResultDTO result = payStrategyService.handlePayCallBackNotify(channelId, params);

        if(PayConstant.RETURN_VALUE_SUCCESS.equalsIgnoreCase(result.getResult())&&null!=result.getPaySuccessDTO()){
            paySuccessNotify(result.getPaySuccessDTO());

        }
        return result.getResult();
    }


}
