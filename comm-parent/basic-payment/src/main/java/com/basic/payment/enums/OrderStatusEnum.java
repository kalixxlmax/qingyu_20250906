package com.basic.payment.enums;

import lombok.Getter;

/**
 * description: 订单状态<br>
 * date: 2020/6/28 0028  <br>
 * author: chat  <br>
 * version: 1.0 <br>
 */
@Getter
public enum OrderStatusEnum {


    /**
     * 订单状态
     */

    SUCCESS("支付成功"),

    REFUND("转入退款"),

    NOTPAY("未支付"),

    CLOSED("已关闭"),

    REVOKED("已撤销（刷卡支付）"),

    USERPAYING("用户支付中"),

    PAYERROR("支付失败"),

    UNKNOW("未知状态"),
    ;

    /**
     * 描述 微信退款后有内容
     */
    private String desc;

    OrderStatusEnum(String desc) {
        this.desc = desc;
    }

    public static OrderStatusEnum findByName(String name) {
        for (OrderStatusEnum orderStatusEnum : OrderStatusEnum.values()) {
            if (name.toLowerCase().equals(orderStatusEnum.name().toLowerCase())) {
                return orderStatusEnum;
            }
        }
        throw new RuntimeException("错误的支付状态");
    }
}
