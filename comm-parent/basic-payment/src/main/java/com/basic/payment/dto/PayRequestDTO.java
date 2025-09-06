package com.basic.payment.dto;

import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotBlank;

@Getter
@Setter
public class PayRequestDTO extends BaseDTO{

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
     * 验证金额是否正确
     *
     */
    private long amount;

}
