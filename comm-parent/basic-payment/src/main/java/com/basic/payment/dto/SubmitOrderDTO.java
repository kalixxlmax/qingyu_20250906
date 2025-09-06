package com.basic.payment.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.Value;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

@Getter
@Setter
public class SubmitOrderDTO extends BaseDTO{


    /**
     * 商户ID
     *
     */
    private String mchId;

    /**
     * 商户订单号
     *
     */
    @NotBlank(message = "mchOrderNo is null")
    private String mchOrderNo;



    /**
     * 商户合并父订单号
     */
    private String parentOrderId;


    /**
     * 支付金额,单位分
     *
     */
    @Min(value = 1)
    private long amount;

    /**
     * 三位货币代码,人民币:cny
     *
     */

    private String currency="cny";

    /**
     * 商户业务用户ID
     */
    private String userId;



    /**
     * 订单标题
     *
     */
    @NotBlank(message = "subject is null")
    private String subject;

    /**
     * 订单描述信息
     *
     */
    @NotBlank(message = "body is null")
    private String body;

    /**
     * 下单签名
     */
    @NotBlank(message = "sign is null")
    private String sign;


    /**
     * 客户端IP
     *
     */
    private String clientIp;

     /**
     * 通知地址
     *
     */
    private String notifyUrl;


    /**
     * 应用id
     */
    private String appId;

    /**
     * 应用type
     */
    private String appType;
}
