package com.basic.payment.model;

import com.basic.payment.dto.BaseDTO;
import lombok.Data;


/**
 * description: BaseModel <br>
 * date: 2020/6/28 0028  <br>
 * author: chat  <br>
 * version: 1.0 <br>
 */
@Data
public class BaseModel extends BaseDTO {

    /**
     * 支付服务 订单号
     *
     */
    private String payOrderId;


    /**
     * 渠道ID
     *
     */
    private String channelId;


    /**
     * 商户订单号
     *
     */
    private String mchOrderNo;


    /**
     * 渠道订单号
     *
     */
    private String channelOrderNo;
}
