package com.basic.payment.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PayRequestResultDTO extends BaseDTO{


    /**
     * 支付状态,0-订单生成,1-支付中(目前未使用),2-支付成功,3-业务处理完成
     */
    private byte status;



    /**
     * 支付订单号
     *
     */
    private String payOrderId;



    private long amount;


}
