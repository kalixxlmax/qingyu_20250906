package com.basic.payment.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SubmitOrderResultDTO  extends BaseDTO{

    /**
     * 支付订单号
     *
     */
    private String payOrderId;

    /**
     * 商户订单号
     *
     */
    private String mchOrderNo;




}
