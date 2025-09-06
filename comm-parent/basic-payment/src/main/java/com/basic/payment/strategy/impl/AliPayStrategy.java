package com.basic.payment.strategy.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alipay.api.AlipayApiException;
import com.alipay.api.AlipayClient;
import com.alipay.api.domain.*;
import com.alipay.api.request.*;
import com.alipay.api.response.*;
import com.github.binarywang.wxpay.bean.notify.WxPayOrderNotifyResult;
import com.github.binarywang.wxpay.exception.WxPayException;
import com.basic.mongodb.utils.BeanCopyUtils;
import com.basic.payment.channel.alipay.AliPayProperties;
import com.basic.payment.channel.alipay.util.AliPayParam;
import com.basic.payment.channel.alipay.util.AliPayUtil;
import com.basic.payment.constant.PayConstant;
import com.basic.payment.dto.PayCallBackResultDTO;
import com.basic.payment.dto.PaySuccessDTO;
import com.basic.payment.entity.PaymentOrderDO;
import com.basic.payment.model.*;
import com.basic.payment.repository.PaymentOrderRepository;
import com.basic.payment.repository.PaymentRedisRepository;
import com.basic.payment.strategy.PayStrategy;
import com.basic.redisson.ex.LockFailException;
import com.basic.utils.DateUtil;
import com.basic.utils.Money;
import com.basic.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;


@Component
@Slf4j
public class AliPayStrategy implements PayStrategy {

    final String logPrefix = "【支付宝统一下单】";

    @Autowired
    protected PaymentOrderRepository paymentOrderRepository;

    @Autowired
    private PaymentRedisRepository paymentRedisRepository;

    @Autowired
    private AlipayClient alipayClient;

    @Autowired
    private AliPayProperties aliPayProperties;

    @Override
    public JSONObject doPayReq(PayRequestModel payRequestModel) {
        String amount = Money.fromCent(payRequestModel.getAmount());
        JSONObject resultJson=new JSONObject();

        String  orderInfo = getOrderInfo(payRequestModel.getSubject(), payRequestModel.getBody(), amount, payRequestModel.getPayOrderId());
        resultJson.put("orderInfo", orderInfo);
        resultJson.put("payOrderId",payRequestModel.getPayOrderId());
        log.info("{} >>> 下单成功", logPrefix);
        log.info("支付宝订单号:payOrderId={},orderInfo ={}", payRequestModel.getPayOrderId(), orderInfo);
        return resultJson;
    }


    /**
     * create the order info. 创建订单信息
     *
     */
    private String getOrderInfo(String subject, String body, String price,String orderNo) {

        //实例化具体API对应的request类,类名称和接口名称对应,当前调用接口名称：alipay.trade.app.pay
        AlipayTradeAppPayRequest request = new AlipayTradeAppPayRequest();
        //SDK已经封装掉了公共参数，这里只需要传入业务参数。以下方法为sdk的model入参方式(model和biz_content同时存在的情况下取biz_content)。
        AlipayTradeAppPayModel model = new AlipayTradeAppPayModel();

        model.setBody(body);
        model.setSubject(subject);
        model.setOutTradeNo(orderNo);
        model.setTimeoutExpress("30m");
        model.setTotalAmount(price);
        model.setProductCode("QUICK_MSECURITY_PAY");
        model.setGoodsType("1");
        request.setBizModel(model);
        request.setNotifyUrl(aliPayProperties.getCallBackUrl());
        try {
            //这里和普通的接口调用不同，使用的是sdkExecute
            AlipayTradeAppPayResponse response = alipayClient.sdkExecute(request);
            log.info("返回order  {}", response.getBody());//就是orderString 可以直接给客户端请求，无需再做处理。

            return response.getBody();
        } catch (AlipayApiException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public PayCallBackResultDTO handlePayCallBackNotify(String params) {
        return processPayCallBackResult(JSON.parseObject(params).getInnerMap());
    }

    @Override
    public PayCallBackResultDTO handlePayCallBackNotify(Map<String,Object> params) {
        return processPayCallBackResult(params);
    }




    private PayCallBackResultDTO processPayCallBackResult(Map<String,Object> params){
        String tradeNo = params.get("out_trade_no").toString();
        String redisLock = paymentRedisRepository.buildRedisKey(PAY_ORDER_LOCK, tradeNo);

        try {
            AliPayParam aliCallBack=new AliPayParam();
            try {
                BeanUtils.populate(aliCallBack,params);
            } catch (IllegalAccessException e) {
                return PayCallBackResultDTO.fail(PayConstant.RETURN_ALIPAY_VALUE_FAIL);
            } catch (InvocationTargetException e) {
                return PayCallBackResultDTO.fail(PayConstant.RETURN_ALIPAY_VALUE_FAIL);
            }
            if(null==aliCallBack){
                log.info(" {} 异常,params {}",logPrefix,JSON.toJSON(params));
            }
            return (PayCallBackResultDTO) paymentRedisRepository.executeOnLock(redisLock, back->{
                return processPayCallBackResultOnLock(tradeNo,aliCallBack);
            });
        } catch (LockFailException e) {
            return PayCallBackResultDTO.fail(PayConstant.RETURN_ALIPAY_VALUE_FAIL);
        } catch (InterruptedException e) {
            return PayCallBackResultDTO.fail(PayConstant.RETURN_ALIPAY_VALUE_FAIL);
        }

    }

    private PayCallBackResultDTO processPayCallBackResultOnLock(String tradeNo, AliPayParam result){

        PaymentOrderDO payOrder = paymentOrderRepository.queryPayOrder(tradeNo);
        if(null==payOrder){
            log.info(" {} 异常,支付订单不存在 payOrderId {}",logPrefix,tradeNo);
            return PayCallBackResultDTO.fail(PayConstant.RETURN_ALIPAY_VALUE_FAIL);
        }else if(PayConstant.PAY_STATUS_INIT!=payOrder.getStatus()&&PayConstant.PAY_STATUS_PAYING!=payOrder.getStatus()){

            log.info(" {} 状态异常, payOrderId {} status ",logPrefix,tradeNo,payOrder.getStatus());
        }
        payOrder.setChannelCallBackMessage(JSON.toJSONString(result));
        payOrder.setChannelId(PayConstant.PAY_CHANNEL_ALIPAY);
        payOrder.setChannelOrderNo(result.getTrade_no());

       /* payOrder.setChannelOrderNo(result.get);
        payOrder.setDevice(result.getd);*/
        if(PayConstant.AlipayConstant.TRADE_STATUS_SUCCESS.equals(result.getTrade_status())){
            payOrder.setStatus(PayConstant.PAY_STATUS_SUCCESS);
            payOrder.setPaySuccessTime(DateUtil.currentTimeSeconds());
            payOrder.setLastNotifyTime(DateUtil.currentTimeSeconds());

            paymentOrderRepository.updatePayOrderCallBackResult(payOrder);
            log.info("{} 支付成功===> payOrderId {}",logPrefix,payOrder.getPayOrderId());

            PaySuccessDTO successDTO=new PaySuccessDTO();
            successDTO.setMchOrderNo(payOrder.getMchOrderNo());
            successDTO.setPaySuccessTime(payOrder.getPaySuccessTime());
            successDTO.setAmount(payOrder.getAmount());
            successDTO.setChannelId(payOrder.getChannelId());
            successDTO.setUserId(payOrder.getUserId());
            successDTO.setPayOrderId(payOrder.getPayOrderId());

            return PayCallBackResultDTO.success(PayConstant.RETURN_ALIPAY_VALUE_SUCCESS,successDTO);

        }else if(PayConstant.AlipayConstant.TRADE_STATUS_CLOSED.equals(result.getTrade_status())) {
            payOrder.setLastNotifyTime(DateUtil.currentTimeSeconds());
            payOrder.setStatus(PayConstant.PAY_STATUS_FAILED);
            log.info("{} 支付已取消===> payOrderId {}",logPrefix,payOrder.getPayOrderId());
        }

        return PayCallBackResultDTO.success(PayConstant.RETURN_ALIPAY_VALUE_SUCCESS,null);






    }


    @Override
    public JSONObject queryOrder(String channelOrderNo,String payOrderId) {

        //实例化具体API对应的request类,类名称和接口名称对应
        AlipayTradeQueryRequest request = new AlipayTradeQueryRequest();
        //SDK已经封装掉了公共参数，这里只需要传入业务参数。以下方法为sdk的model入参方式(model和biz_content同时存在的情况下取biz_content)。
        AlipayTradeQueryModel model =new AlipayTradeQueryModel();
        model.setOutTradeNo(payOrderId);
        model.setTradeNo(channelOrderNo);
        request.setBizModel(model);
        try {
            AlipayTradeQueryResponse response = alipayClient.execute(request);

            if( response.isSuccess()){
                return JSON.parseObject(response.getBody());
            }

        } catch (AlipayApiException e) {
           log.error(" errCode {} errMsg {}",e.getErrCode(),e.getErrMsg());
            log.error(e.getMessage(),e);

        }
        return null;
    }
    @Override
    public JSONObject queryOrder(OrderQueryRequest request) {
        return queryOrder(request.getChannelOrderNo(),request.getPayOrderId());
    }


    @Override
    public JSONObject closeOrder(String channelOrderNo,String payOrderId) {
        AlipayTradeCloseRequest request=new AlipayTradeCloseRequest();
        AlipayTradeCloseModel model=new AlipayTradeCloseModel();
        model.setOutTradeNo(payOrderId);
        model.setTradeNo(channelOrderNo);
        request.setBizModel(model);
        AlipayTradeCloseResponse response=null;
        try {
             response = alipayClient.execute(request);
            if( response.isSuccess()){
                return JSON.parseObject(response.getBody());
            }
        } catch (AlipayApiException e) {
            log.error(" errCode {} errMsg {}",e.getErrCode(),e.getErrMsg());
            log.error(e.getMessage(),e);
        }
        return null;
    }

    @Override
    public JSONObject closeOrder(OrderCloseRequest request) {
        return closeOrder(request.getChannelOrderNo(),request.getPayOrderId());
    }

    @Override
    public JSONObject refund(OrderRefundRequest refundRequest) {
        AlipayTradeRefundRequest request=new AlipayTradeRefundRequest();
        AlipayTradeRefundModel model=new AlipayTradeRefundModel();
        AlipayTradeRefundResponse response=null;

        model.setRefundReason(refundRequest.getRefundReason());
        model.setOutTradeNo(refundRequest.getPayOrderId());
        model.setTradeNo(refundRequest.getChannelOrderNo());
        if(StringUtil.isEmpty(refundRequest.getOutRefundNo())){
            refundRequest.setOutRefundNo(refundRequest.getPayOrderId());
        }
        model.setOutRequestNo(refundRequest.getOutRefundNo());
        String amount = Money.fromCent(refundRequest.getRefundAmount());
        model.setRefundAmount(amount);
        model.setRefundCurrency("CNY");

        request.setBizModel(model);
        try {
            response = alipayClient.execute(request);
            if( response.isSuccess()){
                return JSON.parseObject(response.getBody());
            }
        } catch (AlipayApiException e) {
            log.error(" errCode {} errMsg {}",e.getErrCode(),e.getErrMsg());
            log.error(e.getMessage(),e);

        }
        return null;
    }

    @Override
    public JSONObject queryRefund(OrderRefundQueryRequest queryRequest) {
        AlipayTradeFastpayRefundQueryRequest request=new AlipayTradeFastpayRefundQueryRequest();
        AlipayTradeFastpayRefundQueryModel model=new AlipayTradeFastpayRefundQueryModel();
        model.setTradeNo(queryRequest.getChannelOrderNo());
        model.setOutTradeNo(queryRequest.getPayOrderId());
        if(StringUtil.isEmpty(queryRequest.getOutRefundNo())){
            queryRequest.setOutRefundNo(queryRequest.getPayOrderId());
        }
        model.setOutRequestNo(queryRequest.getOutRefundNo());
        AlipayTradeFastpayRefundQueryResponse response=null;
        request.setBizModel(model);
        try {
            response = alipayClient.execute(request);
            if(response.isSuccess()) {
                return JSON.parseObject(response.getBody());
            }
        } catch (AlipayApiException e) {
            log.error(" errCode {} errMsg {}",e.getErrCode(),e.getErrMsg());
            log.error(e.getMessage(),e);

        }
        return null;
    }



    @Override
    public JSONObject reverseOrder(String payOrderId, String channelOrderNo) {
        AlipayTradeRefundRequest request=new AlipayTradeRefundRequest();
        AlipayTradeRefundModel model=new AlipayTradeRefundModel();
        AlipayTradeRefundResponse response=null;

        try {
            response = alipayClient.execute(request);
            return JSON.parseObject(response.getBody());
        } catch (AlipayApiException e) {
            log.error(" errCode {} errMsg {}",e.getErrCode(),e.getErrMsg());
            log.error(e.getMessage(),e);

        }
        return null;
    }
}
