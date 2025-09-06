package com.basic.payment.dto;

import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotBlank;

@Getter
@Setter
public class PaySuccessDTO extends BaseDTO{

    /**
     * 支付订单号
     *
     */
    @NotBlank(message = "支付令牌不能为空")
    private String payOrderId;


    /**
     * 渠道ID
     *
     */
    @NotBlank(message = "没有选择支付方式")
    private String channelId;

    /**
     * 支付金额,单位分
     *
     */
    private long amount;

    /**
     * 商户业务用户ID
     */
    private String userId;

    /**
     * 商户订单号
     *
     */
    private String mchOrderNo;

    /**
     * 商户合并父订单号
     */
    private String parentOrderId;

    /**
     * 支付渠道订单号
     *
     */
    private String channelOrderNo;


    /**
     * 客户端IP
     *
     */
    private String clientIp;

    /**
     * 设备
     *
     */
    private String device;


    /**
     * 支付成功时间
     */
    private long paySuccessTime;


    /**
     * 支付渠道回调报文
     */
    private String channelCallBackMessage;


}
