package com.basic.payment.model;

import lombok.Data;

/**
 * 支付时请求参数
 */
@Data
public class OrderRefundRequest extends BaseModel{

    /**
     * 退款金额
     */

    private String refundAmount;

    /**
     * 退款原因
     */
    private String refundReason;


    /**
     * 订单总金额
     */
    private String orderAmount;

    /**
     * 商户退款单号
     */
    private String outRefundNo;



    private String notifyUrl;


    private String deviceInfo;


}
