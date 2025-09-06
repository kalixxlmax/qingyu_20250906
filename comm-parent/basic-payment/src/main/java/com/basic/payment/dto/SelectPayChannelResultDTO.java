package com.basic.payment.dto;

import com.basic.payment.constant.PayConstant;
import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;

@Setter
@Getter
public class SelectPayChannelResultDTO extends BaseDTO{

    public SelectPayChannelResultDTO() {
        /*this.payChannelList=new HashSet<>();
        payChannelList.add(PayConstant.PAY_CHANNEL_BALANCE);*/
        /*payChannelList.add(PayConstant.PAY_CHANNEL_ALIPAY);
        payChannelList.add(PayConstant.PAY_CHANNEL_WXPAY);*/
    }

    /**
     * 支付订单号
     *
     */
    private String payOrderId;

    /**
     * 商户订单号
     *
     */
    private String mchOrderNo;


    /**
     * 支付金额,单位分
     *
     */
    private long amount;

    /**
     * 三位货币代码,人民币:cny
     *
     */
    private String currency;


    /**
     * 订单标题
     *
     */
    private String subject;

    /**
     * 订单描述信息
     *
     */
    private String body;


    /**
     * 支付渠道列表
     */
    private Set<String> payChannelList;


}
