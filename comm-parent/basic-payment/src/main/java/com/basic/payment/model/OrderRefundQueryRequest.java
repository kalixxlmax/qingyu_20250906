package com.basic.payment.model;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * 订单退款查询
 */
@Data
@Accessors(chain = true)
public class OrderRefundQueryRequest extends BaseModel{


    /**
     * 商户退款单号
     */
    private String outRefundNo;

    /**
     * 第三方 退款单号
     */
    private String refundId;


}
