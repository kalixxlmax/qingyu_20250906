package com.basic.payment.entity;

import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serializable;


/**
 * 支付渠道实体类
 */

@Data
@Document("payment_channel")
public class PaymentChannelDO implements Serializable {

    /** ID */
    @Id
    private ObjectId id;

    /** 渠道名称 */
    private String channelName;

    /** 渠道ID */

    private String channelId;

    /**
     * 策略执行beanId
     */
    private String strategyBeanId;


    /**
     * 渠道商户ID
     *
     */
    private String channelMchId;

    /**
     * 商户ID
     *
     */
    private String mchId;

    /**
     * 渠道状态,0-停止使用,1-使用中
     *
     */
    private byte status;

    /**
     * 配置参数,json字符串
     *
     */
    private String param;

    /**
     * 备注
     *
     */
    private String remark;

}
