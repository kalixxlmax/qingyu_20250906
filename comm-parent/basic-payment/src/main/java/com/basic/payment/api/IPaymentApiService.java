package com.basic.payment.api;


import com.alibaba.fastjson.JSONObject;
import com.basic.payment.dto.*;
import com.basic.payment.ex.PayOrderException;
import com.basic.payment.model.OrderCloseRequest;
import com.basic.payment.model.OrderQueryRequest;
import com.basic.payment.model.OrderRefundQueryRequest;
import com.basic.payment.model.OrderRefundRequest;

import java.util.Map;

/**
 * 聚合平台接口
 */
public interface IPaymentApiService {


    /**
     * 提交支付订单接口
      * @return  返回订单支付的json
     */
   public JSONObject submitPayOrder(String jsonParam);

   public JSONObject submitPayOrder(SubmitOrderDTO submitOrderDTO);

    /**
     * 发起支付请求的第一步   选择支付方式
     *
     * @param payOrderId
     * @return
     * @throws PayOrderException
     */
    SelectPayChannelResultDTO selectPayChannelRequest(String payOrderId) throws PayOrderException;

    /**
     * 发起支付请求 的第二步
     * @param jsonParam
     * @return
     */
   public JSONObject doPayRequestBefore(String jsonParam);

    /**
     * 发起支付请求 的第二步
     * @param payRequestDTO
     * @return
     */
   public JSONObject doPayRequestBefore(PayRequestDTO payRequestDTO)throws PayOrderException;


    /**
     * 支付平台回调通知
     * @param params
     * @return
     */
   public String handlePayCallBackNotify(String channelId, Map<String, String> params);


    /**
     * 支付成功 修改订单状态
     * 余额支付--
     * @param successDTO
     * @return
     */
   public boolean paySuccess(PaySuccessDTO successDTO);


    long queryPayOrderAmount(String payOrderId);

    /**
     * 查询支付订单
     * @param payOrderId    支付ID
     * @param executeNotify  是否执行通知
     * @return
     */
   public JSONObject queryPayOrder(String payOrderId, String executeNotify);

    /**
     * 查询支付订单
     * @param payOrderId    支付ID
     * @return
     */
    public PaymentOrderDTO queryPayOrder(String payOrderId);


    /**
     * 查询支付平台 订单
     * @param request
     * @return
     */
    JSONObject queryPayChannelOrder(OrderQueryRequest request);

    /**
     * 查询支付平台 退款
     * @param request
     * @return
     */
    JSONObject queryRefund(OrderRefundQueryRequest request);


    /**
     * 支付平台 取消支付订单
     * @param request
     * @return
     */
    JSONObject closeOrder(OrderCloseRequest request);


    /**
     * 支付平台 退款 请求
     * @param request
     * @return
     */
    JSONObject refund(OrderRefundRequest request);



}
