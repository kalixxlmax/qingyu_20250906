package com.basic.payment.model;

import com.github.binarywang.wxpay.constant.WxPayConstants;
import com.basic.mongodb.utils.BeanCopyUtils;
import com.basic.payment.dto.BaseDTO;
import com.basic.payment.entity.PaymentOrderDO;
import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotBlank;

@Getter
@Setter
public class PayRequestModel extends BaseDTO{

    public PayRequestModel() {

    }

    public PayRequestModel(PaymentOrderDO paymentOrderDO) {
        super();
        BeanCopyUtils.copyProperties(paymentOrderDO,this);
    }

    /**
     * 支付订单号
     *
     */
    private String payOrderId;

    /**
     * 商户ID
     *
     */
    private String mchId;

    /**
     * 商户订单号
     *
     */
    private String mchOrderNo;

    /**
     * 商户业务用户ID
     */
    private String userId;

    /**
     * 渠道ID
     *
     */
    private String channelId;

    /**
     * 支付金额,单位分
     *
     */
    private Long amount;

    /**
     * 三位货币代码,人民币:cny
     *
     */
    private String currency;



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
     * 商品标题
     *
     */
    private String subject;

    /**
     * 商品描述信息
     *
     */
    private String body;

    /**
     * 特定渠道发起时额外参数
     *
     */
    private String extra;

    /**
     * 渠道商户ID
     *
     */
    private String channelMchId;

    /**
     * 渠道订单号
     *
     */
    private String channelOrderNo;



    /**
     * 通知地址
     *
     */
    private String notifyUrl;





    /**
     * 创建时间
     */
    private long createTime;


    /**
     * 支付渠道回调报文
     */
    private String channelCallBackMessage;

    /**订单签名
     *
     */
    private String sign;

    /**
     * 商户 回调路径
     */
    private String callBackUrl;

    private String tradeType=WxPayConstants.TradeType.APP;


}
