package com.basic.payment.enums;

/**
 * description: 支付渠道枚举定义 <br>
 * date: 2020/6/28 0028  <br>
 * author: chat  <br>
 * version: 1.0 <br>
 */
public enum  PayChannelEnum {
    /**
     * 支付渠道枚举
     */

    PAY_CHANNEL_BALANCE("BALANCE", "支付宝"),


    PAY_CHANNEL_WXPAY("ALIPAY", "支付宝"),

    PAY_CHANNEL_ALIPAY("WXPAY", "微信"),
            ;

    private String code;

    private String name;


    PayChannelEnum(String code, String name) {
        this.code = code;
        this.name = name;
    }

    public String getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}
