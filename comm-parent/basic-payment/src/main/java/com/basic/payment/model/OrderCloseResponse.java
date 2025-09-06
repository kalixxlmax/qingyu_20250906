package com.basic.payment.model;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * 退款返回的参数
 * Created by 廖师兄
 * 2017-07-08 23:40
 */
@Data
@Accessors(chain = true)
public class OrderCloseResponse extends BaseModel{

    /**
     * 卖家端自定义的的操作员 ID
     */
    private String requestId;

}
