package com.basic.payment.model;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * 支付订单查询
 * Created by 廖师兄
 * 2018-05-31 17:52
 */
@Data
@Accessors(chain = true)
public class OrderQueryRequest extends BaseModel{

    /**
     * 卖家端自定义的的操作员 ID
     */
    private String requestId;
}
