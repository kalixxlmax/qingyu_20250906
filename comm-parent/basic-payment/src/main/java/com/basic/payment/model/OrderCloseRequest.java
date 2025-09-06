package com.basic.payment.model;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * 关闭订单时请求参数
 * https://docs.open.alipay.com/api_1/alipay.trade.close
 */
@Data
@Accessors(chain = true)
public class OrderCloseRequest extends BaseModel {


    /**
     * 卖家端自定义的的操作员 ID
     */
    private String requestId;
}
