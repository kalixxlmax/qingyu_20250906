package com.basic.im.pay.constants;

import java.util.Arrays;

public enum ConsumeRecordEnum {

    /**
     * 用户充值
     */
//    USER_RECHARGE(1,"user_recharge","user recharge"),
//
//    USER_WITHDRAW(2,"user_withdraw","user withdraw"),
//
//    SYSTEM_RECHARGE(3,"system_recharge","system recharge"),
//
//    SEND_REDPACKET(4,"send_redpacket","send redpacket"),
//
//    RECEIVE_REDPACKET(5,"receive_redpacket","receive redpacket"),
//
//    REFUND_REDPACKET(6,"refund_redpacket","refund redpacket"),
//
//    SEND_TRANSFER(7,"send_transfer","send transfer"),
//    RECEIVE_TRANSFER(8,"receive_transfer","receive transfer"),
//    REFUND_TRANSFER(9,"refund_transfer","refund transfer"),
//
//    SEND_PAYMENTCODE(10,"send_paymentcode","send paymentcode"),
//    RECEIVE_PAYMENTCODE(11,"receive_paymentcode","receive paymentcode"),
//
//    SEND_QRCODE(12,"send_qrcode","send qrcode"),
//
//    RECEIVE_QRCODE(13,"receive_qrcode","receive qrcode"),
//
//
//    LIVE_GIVE(14,"live_give","live give"),
//
//    LIVE_RECEIVE(15,"live_receive","Live gift receiving"),
//
//    SYSTEM_HANDCASH(16,"system_handcash","Manual withdrawal"),
//
//
//    SDKTRANSFR_PAY(17,"sdktransfr_pay","Third-party payment"),
//
//
//    MANUALPAY_RECHARGE(18,"manualpay_recharge","Scan code and recharge manually"),
//
//    MANUALPAY_WITHDRAW(19,"manualpay_withdraw","Manual cash withdrawal by scanning code"),
//
//
//
//    MALL_ORDER_INCOME(60,"mall_order_income","Mall order revenue"),
//
//    MALL_ORDER_REFUND(61,"mall_order_refund","Store order refund"),
//
//    MALL_ORDER_PAY(62,"mall_order_pay","Shop order payment"),
//
//    MALL_FORWARD_REWARD(63,"mall_reward_income","Forward sharing revenue"),
//
//    MALL_SHOP_INVITE_REWARD(64,"mall_shop_invite_reward","Team order reward income"),
//
//    ORDER_SERVICEFEE(65,"mall_order_servicefee","Order platform handling fee"),
//
//    FORWARD_REWARD_PAY(66,"forward_reward_pay","Forward sharing expenditure"),
//
//    MALL_SHOP_INVITE_REWARD_PAY(67,"mall_shop_invite_reward_pay","Team order reward payout"),
//    MALL_SHOP_INVITE_REWARD_PAY2(20,"mall_shop_invite_reward_pay","Team order reward payout"),
//    UNKNOWN(0,"UNKNOWN","unknown"),
//    ;

    /**
     * 用户充值
     */
    USER_RECHARGE(1,"user_recharge","用户充值"),

    USER_WITHDRAW(2,"user_withdraw","用户提现"),

    SYSTEM_RECHARGE(3,"system_recharge","管理员充值"),

    SEND_REDPACKET(4,"send_redpacket","发送红包"),

    RECEIVE_REDPACKET(5,"receive_redpacket","领取红包"),

    REFUND_REDPACKET(6,"refund_redpacket","红包退款"),

    SEND_TRANSFER(7,"send_transfer","转账"),
    RECEIVE_TRANSFER(8,"receive_transfer","接收转账"),
    REFUND_TRANSFER(9,"refund_transfer","转账退款"),

    SEND_PAYMENTCODE(10,"send_paymentcode","付款码付款"),
    RECEIVE_PAYMENTCODE(11,"receive_paymentcode","付款码收款"),

    SEND_QRCODE(12,"send_qrcode","二维码付款"),

    RECEIVE_QRCODE(13,"receive_qrcode","二维码收款"),


    LIVE_GIVE(14,"live_give","直播送礼物"),

    LIVE_RECEIVE(15,"live_receive","直播收礼物"),

    SYSTEM_HANDCASH(16,"system_handcash","手动提现"),


    SDKTRANSFR_PAY(17,"sdktransfr_pay","第三方支付"),


    MANUALPAY_RECHARGE(18,"manualpay_recharge","扫码手动充值"),

    MANUALPAY_WITHDRAW(19,"manualpay_withdraw","扫码手动提现"),



    MALL_ORDER_INCOME(60,"mall_order_income","商城订单收入"),

    MALL_ORDER_REFUND(61,"mall_order_refund","商城订单退款"),

    MALL_ORDER_PAY(62,"mall_order_pay","商城订单付款"),

    MALL_FORWARD_REWARD(63,"mall_reward_income","转发分成收入"),

    MALL_SHOP_INVITE_REWARD(64,"mall_shop_invite_reward","团队下单奖励收入"),

    ORDER_SERVICEFEE(65,"mall_order_servicefee","订单平台手续费支出"),

    FORWARD_REWARD_PAY(66,"forward_reward_pay","转发分成支出"),

    MALL_SHOP_INVITE_REWARD_PAY(67,"mall_shop_invite_reward_pay","团队下单奖励支出"),
    MALL_SHOP_INVITE_REWARD_PAY2(20,"mall_shop_invite_reward_pay","团队下单奖励支出"),
    UNKNOWN(0,"UNKNOWN","未知");
    private  byte type;


    private String code;

    private  String desc;

    private ConsumeRecordEnum(int type,String code, String desc){
        this.type= (byte) type;
        this.code=code;
        this.desc=desc;
    }


    public byte getType() {
        return type;
    }

    public void setType(int type) {
        this.type = (byte) type;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }


    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public static String getDesc(int type){
        return Arrays.asList(ConsumeRecordEnum.values()).stream()
                .filter(value -> value.getType()== type)
                .findFirst().orElse(ConsumeRecordEnum.UNKNOWN).getDesc();
    }
    public static String getCode(int type){
        return Arrays.asList(ConsumeRecordEnum.values()).stream()
                .filter(value -> value.getType()== type)
                .findFirst().orElse(ConsumeRecordEnum.UNKNOWN).getCode();
    }
}



