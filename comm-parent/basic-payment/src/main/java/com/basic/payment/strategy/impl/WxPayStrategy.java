package com.basic.payment.strategy.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.binarywang.wxpay.bean.notify.WxPayOrderNotifyResult;
import com.github.binarywang.wxpay.bean.order.WxPayAppOrderResult;
import com.github.binarywang.wxpay.bean.request.WxPayRefundQueryRequest;
import com.github.binarywang.wxpay.bean.request.WxPayRefundRequest;
import com.github.binarywang.wxpay.bean.request.WxPayUnifiedOrderRequest;
import com.github.binarywang.wxpay.bean.result.WxPayOrderCloseResult;
import com.github.binarywang.wxpay.bean.result.WxPayOrderQueryResult;
import com.github.binarywang.wxpay.bean.result.WxPayRefundQueryResult;
import com.github.binarywang.wxpay.bean.result.WxPayRefundResult;
import com.github.binarywang.wxpay.config.WxPayConfig;
import com.github.binarywang.wxpay.constant.WxPayConstants;
import com.github.binarywang.wxpay.exception.WxPayException;
import com.github.binarywang.wxpay.service.WxPayService;
import com.github.binarywang.wxpay.service.impl.WxPayServiceImpl;
import com.basic.payment.channel.wechat.WxPayProperties;
import com.basic.payment.constant.PayConstant;
import com.basic.payment.dto.PayCallBackResultDTO;
import com.basic.payment.dto.PayRequestResultDTO;
import com.basic.payment.dto.PaySuccessDTO;
import com.basic.payment.entity.PaymentOrderDO;
import com.basic.payment.ex.PayOrderException;
import com.basic.payment.model.*;
import com.basic.payment.repository.PaymentOrderRepository;
import com.basic.payment.repository.PaymentRedisRepository;
import com.basic.payment.strategy.PayStrategy;
import com.basic.redisson.ex.LockFailException;
import com.basic.utils.DateUtil;
import com.basic.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.Map;
@Service
@Slf4j
public class WxPayStrategy implements PayStrategy {

    @Autowired
   private WxPayConfig payConfig ;


   @Autowired
   private WxPayService payService ;


    @Autowired
    protected PaymentOrderRepository paymentOrderRepository;

    @Autowired
    private PaymentRedisRepository paymentRedisRepository;


    final String logPrefix = "【微信支付统一下单】";



    @Override
    public JSONObject doPayReq(PayRequestModel payRequest) throws PayOrderException {

        JSONObject resultJson =null;

        WxPayUnifiedOrderRequest orderRequest = buildUnifiedOrderRequest(payRequest, this.payConfig);
        this.payService.getConfig().setTradeType(payRequest.getTradeType());
        String payOrderId = payRequest.getPayOrderId();
        try {
            Serializable result=null;
            if(WxPayConstants.TradeType.APP.equals(orderRequest.getTradeType())){
                WxPayAppOrderResult appOrderResult=payService.createOrder(orderRequest);
                log.info("微信支付订单号:payOrderId={},prepayId={}", payOrderId, appOrderResult.getPrepayId());
                result=appOrderResult;
            }else {
                result=payService.createOrder(orderRequest);
            }

            resultJson= JSON.parseObject(JSON.toJSONString(result));
            log.info("{} >>> 下单成功", logPrefix);



            return resultJson;
        } catch (WxPayException e) {
            log.error("下单失败",e);
            //出现业务错误
            log.info("{}下单返回失败", logPrefix);
            log.info("err_code:{}", e.getErrCode());
            log.info("err_code_des:{}", e.getErrCodeDes());

            throw  new PayOrderException(PayConstant.ERROR_PAY_CHANNEL_REQEST);

            // return XXPayUtil.makeRetData(XXPayUtil.makeRetMap(PayConstant.RETURN_VALUE_SUCCESS, "", PayConstant.RETURN_VALUE_FAIL, "0111", "调用微信支付失败," + e.getErrCode() + ":" + e.getErrCodeDes()), resKey);
        }
    }

    @Override
    public PayCallBackResultDTO handlePayCallBackNotify(String params){
        log.info("====== 开始处理微信支付回调通知 ======");
        String logPrefix = "【处理微信支付回调】";

        try {
            WxPayOrderNotifyResult result = payService.parseOrderNotifyResult(params);
            return processPayCallBackResult(result,params);
        } catch (WxPayException e) {
            log.info(" {} 异常,{}",logPrefix,e.getErrCode(),e.getErrCodeDes());
        }

        return null;
    }

    @Override
    public PayCallBackResultDTO handlePayCallBackNotify(Map<String,Object> params) {
        log.info("====== 开始处理微信支付回调通知 ======");
        String logPrefix = "【处理微信支付回调】";
        return processPayCallBackResult(null,null);
    }




    private PayCallBackResultDTO processPayCallBackResult(WxPayOrderNotifyResult result,String params){
        String redisLock = paymentRedisRepository.buildRedisKey(PAY_ORDER_LOCK, result.getOutTradeNo());

        try {
            return (PayCallBackResultDTO) paymentRedisRepository.executeOnLock(redisLock, back->{
                 return processPayCallBackResultOnLock(result,params);
             });
        } catch (LockFailException e) {
            return PayCallBackResultDTO.fail();
        } catch (InterruptedException e) {
            return PayCallBackResultDTO.fail();
        }

    }

    private PayCallBackResultDTO processPayCallBackResultOnLock(WxPayOrderNotifyResult result,String params){

        PaymentOrderDO payOrder = paymentOrderRepository.queryPayOrder(result.getOutTradeNo());
        if(null==payOrder){
            log.info(" {} 异常,支付订单不存在 payOrderId {}",logPrefix,result.getOutTradeNo());
            return PayCallBackResultDTO.fail();
        }else if(PayConstant.PAY_STATUS_INIT!=payOrder.getStatus()&&PayConstant.PAY_STATUS_PAYING!=payOrder.getStatus()){

            log.info(" {} 状态异常, payOrderId {} status ",logPrefix,result.getOutTradeNo(),payOrder.getStatus());
        }
        try {
            payOrder.setChannelOrderNo(result.getTransactionId());
            payOrder.setDevice(result.getDeviceInfo());
            payOrder.setChannelId(PayConstant.PAY_CHANNEL_WXPAY);
            result.checkResult(payService,payConfig.getSignType(),true);
            payOrder.setStatus(PayConstant.PAY_STATUS_SUCCESS);
            payOrder.setPaySuccessTime(DateUtil.currentTimeSeconds());
            log.info("{} 支付成功===> payOrderId {}",logPrefix,payOrder.getPayOrderId());

            PaySuccessDTO successDTO=new PaySuccessDTO();
            successDTO.setChannelOrderNo(result.getTransactionId());
            successDTO.setMchOrderNo(payOrder.getMchOrderNo());
            successDTO.setPaySuccessTime(payOrder.getPaySuccessTime());
            successDTO.setAmount(payOrder.getAmount());
            successDTO.setChannelId(payOrder.getChannelId());
            successDTO.setUserId(payOrder.getUserId());
            successDTO.setPayOrderId(payOrder.getPayOrderId());
            successDTO.setDevice(result.getDeviceInfo());

            return PayCallBackResultDTO.success(successDTO);
        } catch (WxPayException e) {
            log.info(" {} 异常,{}",logPrefix,e.getErrCode(),e.getErrCodeDes());
            return PayCallBackResultDTO.fail();
        }
        finally {
            payOrder.setLastNotifyTime(DateUtil.currentTimeSeconds());
            payOrder.setChannelCallBackMessage(params);


            paymentOrderRepository.updatePayOrderCallBackResult(payOrder);
        }






    }


    /**
     * 构建微信统一下单请求数据
     * @param payOrder
     * @param wxPayConfig
     * @return
     */
    WxPayUnifiedOrderRequest buildUnifiedOrderRequest(PayRequestModel payOrder, WxPayConfig wxPayConfig) {
        String tradeType = payOrder.getTradeType();
        String payOrderId = payOrder.getPayOrderId();
        Integer totalFee = payOrder.getAmount().intValue();// 支付金额,单位分
        String deviceInfo = payOrder.getDevice();
        String body = payOrder.getBody();
        String detail = null;
        String attach = null;
        String outTradeNo = payOrderId;
        String feeType = "CNY";
        String spBillCreateIP = payOrder.getClientIp();
        String timeStart = null;
        String timeExpire = null;
        String goodsTag = null;
        String notifyUrl = wxPayConfig.getNotifyUrl();
        String productId = null;
        if(tradeType.equals(PayConstant.WxConstant.TRADE_TYPE_NATIVE)) {
            productId = JSON.parseObject(payOrder.getExtra()).getString("productId");
        }
        String limitPay = null;
        String openId = null;
        if(tradeType.equals(PayConstant.WxConstant.TRADE_TYPE_JSPAI)) {
            openId = JSON.parseObject(payOrder.getExtra()).getString("openId");
        }
        String sceneInfo = null;
        if(tradeType.equals(PayConstant.WxConstant.TRADE_TYPE_MWEB)) {
            sceneInfo = JSON.parseObject(payOrder.getExtra()).getString("sceneInfo");
        }
        // 微信统一下单请求对象
        WxPayUnifiedOrderRequest request = new WxPayUnifiedOrderRequest();
        request.setDeviceInfo(deviceInfo);
        request.setBody(body);
        request.setDetail(detail);
        request.setAttach(attach);
        request.setOutTradeNo(outTradeNo);
        request.setFeeType(feeType);
        request.setTotalFee(totalFee);
        request.setSpbillCreateIp(spBillCreateIP);
        request.setTimeStart(timeStart);
        request.setTimeExpire(timeExpire);
        request.setGoodsTag(goodsTag);
        request.setNotifyUrl(notifyUrl);
        request.setTradeType(tradeType);
        request.setProductId(productId);
        request.setLimitPay(limitPay);
        request.setOpenid(openId);
        request.setSceneInfo(sceneInfo);

        return request;
    }

    @Override
    public JSONObject queryOrder(OrderQueryRequest queryRequest) {

        return queryOrder(queryRequest.getChannelOrderNo(),queryRequest.getPayOrderId());
    }
    @Override
    public JSONObject queryOrder(String channelOrderNo,String payOrderId) {
        try {
            WxPayOrderQueryResult result = payService.queryOrder(channelOrderNo, payOrderId);

            return JSON.parseObject(JSON.toJSONString(result));
        } catch (WxPayException e) {
            log.error(" errCode {} errMsg {}",e.getErrCode(),e.getErrCodeDes());
            log.error(e.getMessage(),e);
        }
        return null;
    }

    @Override
    public JSONObject closeOrder(String channelOrderNo,String payOrderId) {

        try {
            WxPayOrderCloseResult result = payService.closeOrder(payOrderId);

            return JSON.parseObject(JSON.toJSONString(result));
        } catch (WxPayException e) {
            log.error(" errCode {} errMsg {}",e.getErrCode(),e.getErrCodeDes());
            log.error(e.getMessage(),e);
        }
        return null;
    }



    @Override
    public JSONObject closeOrder(OrderCloseRequest closeRequest) {
        return queryOrder(closeRequest.getChannelOrderNo(),closeRequest.getPayOrderId());
    }

    @Override
    public JSONObject refund(OrderRefundRequest refundRequest) {
        try {

            WxPayRefundRequest request=new WxPayRefundRequest();
            request.setTransactionId(refundRequest.getChannelOrderNo());
            request.setOutTradeNo(refundRequest.getPayOrderId());
            request.setTotalFee(Integer.parseInt(refundRequest.getOrderAmount()));
            request.setRefundFee(Integer.parseInt(refundRequest.getRefundAmount()));
            request.setRefundDesc(refundRequest.getRefundReason());
            request.setNotifyUrl(refundRequest.getNotifyUrl());

            request.setDeviceInfo(refundRequest.getDeviceInfo());
            if(StringUtil.isEmpty(refundRequest.getOutRefundNo())){
                refundRequest.setOutRefundNo(refundRequest.getPayOrderId());
            }
            request.setOutRefundNo(refundRequest.getOutRefundNo());

            WxPayRefundResult result = payService.refund(request);

            return JSON.parseObject(JSON.toJSONString(result));
        } catch (WxPayException e) {
            log.error(" errCode {} errMsg {}",e.getErrCode(),e.getErrCodeDes());
            log.error(e.getMessage(),e);
        }
        return null;
    }
    @Override
    public JSONObject queryRefund(OrderRefundQueryRequest queryRequest) {
        try {

            WxPayRefundQueryRequest request=new WxPayRefundQueryRequest();
            request.setTransactionId(queryRequest.getChannelOrderNo());
            request.setOutTradeNo(queryRequest.getPayOrderId());
            request.setOutRefundNo(queryRequest.getOutRefundNo());
            request.setRefundId(queryRequest.getRefundId());
            if(StringUtil.isEmpty(queryRequest.getOutRefundNo())){
                queryRequest.setOutRefundNo(queryRequest.getPayOrderId());
            }
            request.setOutRefundNo(queryRequest.getOutRefundNo());
            WxPayRefundQueryResult result = payService.refundQuery(request);

            return JSON.parseObject(JSON.toJSONString(result));
        } catch (WxPayException e) {
            log.error(" errCode {} errMsg {}",e.getErrCode(),e.getErrCodeDes());
            log.error(e.getMessage(),e);
        }
        return null;
    }


    @Override
    public JSONObject reverseOrder(String payOrderId, String channelOrderNo) {
        return null;
    }
}
