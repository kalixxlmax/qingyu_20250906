package com.basic.payment.strategy;

import com.alibaba.fastjson.JSONObject;
import com.github.binarywang.wxpay.exception.WxPayException;
import com.basic.payment.dto.PayCallBackResultDTO;
import com.basic.payment.ex.PayOrderException;
import com.basic.payment.model.*;

import java.util.Map;

/**
 * @author chat 
 *
 * 支付策略模式 接口
 */
public interface PayStrategy {

    String PAY_ORDER_LOCK = "lock:payorder:%s";

    /**
     * 发起支付请求
     * @param payRequestModel
     * @return
     * @throws PayOrderException
     */
    JSONObject doPayReq(PayRequestModel payRequestModel) throws PayOrderException;

    /**
     * 处理支付回调
     * @param params
     * @return
     */
    PayCallBackResultDTO handlePayCallBackNotify(String params) ;
    /**
     * 处理支付回调
     * @param params
     * @return
     */
    PayCallBackResultDTO handlePayCallBackNotify(Map<String, Object> params);

    /**
     * 查询支付订单
     * @param request
     * @return
     */
    JSONObject queryOrder(OrderQueryRequest request);

    /**
     * 关闭交易
     * @param request
     * @return
     */
    JSONObject closeOrder(OrderCloseRequest request);

    /**
     * 订单退款
     * @param request
     * @return
     */
    JSONObject refund(OrderRefundRequest request);

    JSONObject queryOrder(String channelOrderNo, String payOrderId);

    JSONObject closeOrder(String channelOrderNo, String payOrderId);

    /**
     * 查询退款
     * @param request
     * @return
     */
    JSONObject queryRefund(OrderRefundQueryRequest request);

    /**
     * 撤销退款
     * @param payOrderId
     * @param channelOrderNo
     * @return
     */
    JSONObject reverseOrder(String payOrderId, String channelOrderNo);


}
