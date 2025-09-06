package com.basic.payment.entity;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@Document("payment_order")
public class PaymentOrderDO implements Serializable {

    @Id
    private ObjectId id;


    /**
     * 支付状态,0-订单生成,1 -支付中(目前未使用),2 -支付成功,3-业务处理完成
     * -1 支付失败
     */
    private byte status;


    /**
     * 支付订单号
     *
     */
    @Indexed(unique = true)
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
     * 商户合并父订单号
     */
    private String parentOrderId;

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
    private long amount;

    /**
     * 已退款 金额
     */
    private long refundAmount;

    /**
     * 三位货币代码,人民币:CNY
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
     * 渠道支付错误码
     *
     */
    private String errCode;

    /**
     * 渠道支付错误描述
     *
     */
    private String errMsg;


    /**
     * 通知地址
     *
     */
    private String notifyUrl;

    /**
     * 通知次数
     *
     */
    private byte notifyCount;

    /**
     * 最后一次通知时间
     *
     */
    private long lastNotifyTime;

    /**
     * 扩展参数1
     *
     */
    private String param1;

    /**
     * 扩展参数2
     *
     */
    private String param2;

    /**
     * 订单失效时间
     *
     */
    private Long expireTime;

    /**
     * 创建时间
     */
    private long createTime;

    /**
     * 支付成功时间
     */
    private long paySuccessTime;

    /**
     * 退款时间
     */
    private long refundTime;

    /**
     * 更新时间
     */
    private long modifyTime;

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


    /**
     * 应用id
     */
    private String appId;

    /**
     * 应用type
     */
    private String appType;


    /**
     * 关联的交易
     */
    private List<String> relationShipArr;

    @Override
    public String toString(){
        return JSON.toJSONString(this);
    }



}
