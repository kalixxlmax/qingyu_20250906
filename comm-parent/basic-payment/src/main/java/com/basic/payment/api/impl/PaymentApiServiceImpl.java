package com.basic.payment.api.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.basic.mongodb.utils.BeanCopyUtils;
import com.basic.payment.api.IPaymentApiService;
import com.basic.payment.api.IPaymentNoticeApiService;
import com.basic.payment.constant.PayConstant;
import com.basic.payment.dto.*;
import com.basic.payment.entity.PaymentOrderDO;
import com.basic.payment.ex.PayOrderException;
import com.basic.payment.model.*;
import com.basic.payment.repository.PaymentOrderRepository;
import com.basic.payment.service.PayStrategyService;
import com.basic.payment.service.PaymentChannelService;
import com.basic.payment.util.XXPayUtil;
import com.basic.utils.SnowflakeUtils;
import com.basic.utils.StringUtils;
import jodd.util.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
public class PaymentApiServiceImpl implements IPaymentApiService {


    @Autowired
    private PaymentOrderRepository paymentOrderRepository;


    @Autowired
    private IPaymentNoticeApiService paymentNoticeApiService;


    @Autowired
    private PaymentChannelService paymentChannelService;

    @Autowired
    private PayStrategyService payStrategyService;

    /**
     * 提交订单
     * @param jsonParam
     * @return
     */
    @Override
    public JSONObject submitPayOrder(String jsonParam) {
        JSONObject jsonResult=new JSONObject();

        PaymentOrderDO paymentOrderDO= JSON.parseObject(jsonParam,PaymentOrderDO.class);


        paymentOrderDO.setPayOrderId(SnowflakeUtils.getNextIdStr());


        jsonResult.put("payOrderId",paymentOrderDO.getPayOrderId());



        return jsonResult;
    }

    /**
     * 提交订单
     * @param submitOrderDTO
     * @return
     */
    @Override
    public JSONObject submitPayOrder(SubmitOrderDTO submitOrderDTO) {
        log.info("用户提交订单 ==> {}",submitOrderDTO.toString());
        JSONObject jsonResult=new JSONObject();

        try {
            PaymentOrderDO paymentOrderDO=new PaymentOrderDO();

            BeanUtils.copyProperties(submitOrderDTO,paymentOrderDO);

            paymentOrderDO.setPayOrderId(SnowflakeUtils.getNextIdStr());
                paymentOrderRepository.save(paymentOrderDO);
            jsonResult.put("payOrderId",paymentOrderDO.getPayOrderId());
            log.info("订单提交成功 ===> ",jsonResult.toJSONString());
            return jsonResult;
        } catch (Exception e) {
            jsonResult.put("error",e.getMessage());
            return jsonResult;
        }

    }
    @Override
    public SelectPayChannelResultDTO selectPayChannelRequest(String payOrderId) throws PayOrderException {
        SelectPayChannelResultDTO payRequestResultDTO=null;
        try {
            PaymentOrderDO paymentOrderDO=queryPayOrderRequest(payOrderId);
            payRequestResultDTO=new SelectPayChannelResultDTO();

            BeanCopyUtils.copyProperties(paymentOrderDO,payRequestResultDTO);

            payRequestResultDTO.setPayChannelList(paymentChannelService.queryOpenPaymentChannelIds());


        } catch (Exception e) {
            log.error(e.getMessage(),e);
             throw e;
        }
        return payRequestResultDTO;
    }

    @Override
    public JSONObject doPayRequestBefore(String jsonParam) {
        return null;
    }

    @Override
    public JSONObject doPayRequestBefore(PayRequestDTO payRequestDTO) throws PayOrderException {
        JSONObject jsonResult=null;
        PaymentOrderDO paymentOrderDO =queryPayOrderRequest(payRequestDTO.getPayOrderId());
            log.info("根据支付方式发起支付请求 ==> {} ",payRequestDTO.toString());
        try {

           switch (payRequestDTO.getChannelId()){
               case PayConstant.PAY_CHANNEL_BALANCE:
                   queryPayOrderRequest(payRequestDTO.getPayOrderId());
                   jsonResult=new JSONObject();
                   jsonResult.put("payOrderId",paymentOrderDO.getPayOrderId());
                   jsonResult.put("amount",paymentOrderDO.getAmount());
                   jsonResult.put("currency",paymentOrderDO.getAmount());
                   jsonResult.put("mchOrderNo",paymentOrderDO.getMchOrderNo());
                   jsonResult.put("parentOrderId",paymentOrderDO.getParentOrderId());

                   break;
               case PayConstant.PAY_CHANNEL_ALIPAY:
               case PayConstant.PAY_CHANNEL_WXPAY:
                   if(!paymentChannelService.paymentChannelIsEnable(payRequestDTO.getChannelId())){
                       /**
                        * 支付方式未启用
                        */
                       throw new PayOrderException(PayConstant.ERROR_PAY_CHANNEL_DISABLE);
                   }
                   PayRequestModel payRequestModel=new PayRequestModel(paymentOrderDO);

                  return payStrategyService.doPayReq(payRequestDTO.getChannelId(),payRequestModel);
               default:
                   break;
           }

        } catch (Exception e) {
            throw e;
        }
        return jsonResult;
    }

    private PaymentOrderDO queryPayOrderRequest(String payOrderId) throws PayOrderException{
        PaymentOrderDO paymentOrderDO = paymentOrderRepository.queryPayOrder(payOrderId);
        /**
         * 判断订单支付状态
         */
        if(null==paymentOrderDO){
            log.info("支付订单不存在 ==> {}",payOrderId);
            throw new PayOrderException(PayConstant.ERROR_ORDER_NOTEXIST);
        }if(PayConstant.PAY_STATUS_EXPIRED==paymentOrderDO.getStatus()){
            log.info("支付订单已过期 ==> {}",payOrderId);
            throw new PayOrderException(PayConstant.ERROR_STATUS_EXPIRED);
        }else if(PayConstant.PAY_STATUS_FAILED==paymentOrderDO.getStatus()){
            log.info("支付订单已失败 ==> {}",payOrderId);
            throw new PayOrderException(PayConstant.ERROR_STATUS_FAILED);
        }else if(PayConstant.PAY_STATUS_PAYING<paymentOrderDO.getStatus()){
            log.info("支付订单已支付 ==> {}",payOrderId);
            throw new PayOrderException(PayConstant.ERROR_STATUS_SUCCESS);
        }
        return paymentOrderDO;
    }

    public PaymentOrderDO queryPayOrderDO(String payOrderId) {
        PaymentOrderDO paymentOrderDO = paymentOrderRepository.queryPayOrder(payOrderId);
        /**
         * 判断订单支付状态
         */
        if(null==paymentOrderDO){
            log.info("支付订单不存在 ==> {}",payOrderId);
            throw new PayOrderException(PayConstant.ERROR_ORDER_NOTEXIST);
        }
        return paymentOrderDO;
    }

    public String queryPayOrderPayChannelId(String payOrderId) {
        PaymentOrderDO paymentOrderDO = paymentOrderRepository.queryPayOrder(payOrderId);
        /**
         * 判断订单支付状态
         */
        if(null==paymentOrderDO){
            log.info("支付订单不存在 ==> {}",payOrderId);
            throw new PayOrderException(PayConstant.ERROR_ORDER_NOTEXIST);
        }
        return paymentOrderDO.getChannelId();
    }


    @Override
    public String handlePayCallBackNotify(String channelId, Map<String, String> params) {
        return paymentNoticeApiService.handlePayCallBackNotify(channelId,params);
    }

    @Override
    public boolean paySuccess(PaySuccessDTO successDTO) {
        /*PaymentOrderDO paymentOrderDO = paymentOrderRepository.queryPayOrder(successDTO.getPayOrderId());
        if(null!=paymentOrderDO){
            successDTO.setMchOrderNo(paymentOrderDO.getMchOrderNo());
            successDTO.setAmount(paymentOrderDO.getAmount());
        }*/
        log.info("处理订单支付成功 ===> {} ",successDTO.toString());
        boolean success = paymentOrderRepository.paySuccess(successDTO);
        if(success){
            paymentNoticeApiService.paySuccessNotify(successDTO);
        }
        return success;
    }

    @Override
    public long queryPayOrderAmount(String payOrderId) {
            return paymentOrderRepository.queryPayOrderAmount(payOrderId);
    }

    @Override
    public JSONObject queryPayOrder(String payOrderId, String executeNotify) {
        PaymentOrderDO paymentOrderDO = paymentOrderRepository.queryPayOrder(payOrderId);
        if(null==paymentOrderDO){
            return null;
        }

        return JSON.parseObject(JSON.toJSONString(paymentOrderDO));
    }

    @Override
    public PaymentOrderDTO queryPayOrder(String payOrderId) {
        PaymentOrderDO paymentOrderDO = paymentOrderRepository.queryPayOrder(payOrderId);
        PaymentOrderDTO paymentOrderDTO=new PaymentOrderDTO();
        BeanCopyUtils.copyProperties(paymentOrderDO,paymentOrderDTO);
        return paymentOrderDTO;
    }

    private BaseModel setPayChannelId(BaseModel model){
        if(StringUtils.isEmpty(model.getChannelId())) {
            PaymentOrderDTO orderDTO = queryPayOrder(model.getPayOrderId());

            if(StringUtils.isEmpty(orderDTO.getChannelId())){
                log.info("支付渠道订单不存在 ==> {}",model.getPayOrderId());
                throw new PayOrderException(PayConstant.ERROR_ORDER_NOTEXIST);
            }
            model.setChannelId(orderDTO.getChannelId());
            model.setChannelOrderNo(orderDTO.getChannelOrderNo());
        }
        return model;
    }


    @Override
    public JSONObject queryPayChannelOrder(OrderQueryRequest request) {
        setPayChannelId(request);
        return payStrategyService.queryOrder(request);
    }

    @Override
    public JSONObject queryRefund(OrderRefundQueryRequest request) {
        setPayChannelId(request);
        return payStrategyService.queryRefund(request);
    }

    @Override
    public JSONObject closeOrder(OrderCloseRequest request) {
        setPayChannelId(request);
        return payStrategyService.closeOrder(request);
    }

    @Override
    public JSONObject refund(OrderRefundRequest request) {
        PaymentOrderDO paymentOrder = paymentOrderRepository.queryPayOrder(request.getPayOrderId());
        if(PayConstant.PAY_STATUS_CANCEL==paymentOrder.getStatus()){
            throw new PayOrderException(PayConstant.ERROR_STATUS_CANCEL);
        }else if(PayConstant.PAY_STATUS_REFUND==paymentOrder.getStatus()){
            if(paymentOrder.getRefundAmount()>=paymentOrder.getAmount()) {
                throw new PayOrderException(PayConstant.ERROR_STATUS_REFUND);
            }
        }
        if(StringUtils.isEmpty(request.getChannelId())) {
            request.setChannelId(paymentOrder.getChannelId());
        }
        request.setChannelOrderNo(paymentOrder.getChannelOrderNo());
        return payStrategyService.refund(request);
    }

    /**
     * 验证创建订单请求参数,参数通过返回JSONObject对象,否则返回错误文本信息
     * @param params
     * @return
     */
    private Object validateParams(JSONObject params, JSONObject payContext) {
        // 验证请求参数,参数有问题返回错误提示
        String errorMessage;
        // 支付参数


        // 商户ID
        String mchId = params.getString("mchId");
        // 商户订单号
        String mchOrderNo = params.getString("mchOrderNo");
        // 渠道ID
        String channelId = params.getString("channelId");
        // 支付金额（单位分）
        String amount = params.getString("amount");
        // 币种
        String currency = params.getString("currency");
        // 客户端IP
        String clientIp = params.getString("clientIp");
        // 设备
        String device = params.getString("device");
        // 特定渠道发起时额外参数
        String extra = params.getString("extra");
        // 扩展参数1
        String param1 = params.getString("param1");
        // 扩展参数2
        String param2 = params.getString("param2");
        // 支付结果回调URL
        String notifyUrl = params.getString("notifyUrl");
        // 签名
        String sign = params.getString("sign");
        // 商品主题
        String subject = params.getString("subject");
        // 商品描述信息
        String body = params.getString("body");
        // 验证请求参数有效性（必选项）
        if(StringUtil.isEmpty(mchId)) {
            errorMessage = "request params[mchId] error.";
            return errorMessage;
        }
        if(StringUtil.isEmpty(mchOrderNo)) {
            errorMessage = "request params[mchOrderNo] error.";
            return errorMessage;
        }
        if(StringUtil.isEmpty(channelId)) {
            errorMessage = "request params[channelId] error.";
            return errorMessage;
        }
        try {
            Long.valueOf(amount);
        } catch (NumberFormatException e) {
            errorMessage = "request params[amount] error.";
            return errorMessage;
        }
        if(StringUtil.isEmpty(currency)) {
            errorMessage = "request params[currency] error.";
            return errorMessage;
        }
        if(StringUtil.isEmpty(notifyUrl)) {
            errorMessage = "request params[notifyUrl] error.";
            return errorMessage;
        }
        if(StringUtil.isEmpty(subject)) {
            errorMessage = "request params[subject] error.";
            return errorMessage;
        }
        if(StringUtil.isEmpty(body)) {
            errorMessage = "request params[body] error.";
            return errorMessage;
        }


        // 签名信息
        if (StringUtil.isEmpty(sign)) {
            errorMessage = "request params[sign] error.";
            return errorMessage;
        }

        // 查询商户信息
        JSONObject mchInfo =null;
        //mchInfoService.getByMchId(mchId);
        if(mchInfo == null) {
            errorMessage = "Can't found mchInfo[mchId="+mchId+"] record in db.";
            return errorMessage;
        }
        if(mchInfo.getByte("state") != 1) {
            errorMessage = "mchInfo not available [mchId="+mchId+"] record in db.";
            return errorMessage;
        }

        String reqKey = mchInfo.getString("reqKey");
        if (StringUtil.isEmpty(reqKey)) {
            errorMessage = "reqKey is null[mchId="+mchId+"] record in db.";
            return errorMessage;
        }
        payContext.put("resKey", mchInfo.getString("resKey"));

        // 查询商户对应的支付渠道
        JSONObject payChannel =null;
        //payChannelService.getByMchIdAndChannelId(mchId, channelId);
        if(payChannel == null) {
            errorMessage = "Can't found payChannel[channelId="+channelId+",mchId="+mchId+"] record in db.";
            return errorMessage;
        }
        if(payChannel.getByte("state") != 1) {
            errorMessage = "channel not available [channelId="+channelId+",mchId="+mchId+"]";
            return errorMessage;
        }

        // 验证签名数据
        boolean verifyFlag = XXPayUtil.verifyPaySign(params, reqKey);
        if(!verifyFlag) {
            errorMessage = "Verify XX pay sign failed.";
            return errorMessage;
        }
        // 验证参数通过,返回JSONObject对象
        JSONObject payOrder = new JSONObject();
       // payOrder.put("payOrderId", SnowflakeUtils.getNextIdStr());
        payOrder.put("mchId", mchId);
        payOrder.put("mchOrderNo", mchOrderNo);
        payOrder.put("channelId", channelId);
        payOrder.put("amount", Long.parseLong(amount));
        payOrder.put("currency", currency);
        payOrder.put("clientIp", clientIp);
        payOrder.put("device", device);
        payOrder.put("subject", subject);
        payOrder.put("body", body);
        payOrder.put("extra", extra);
        //payOrder.put("channelMchId", payChannel.getString("channelMchId"));
        payOrder.put("param1", param1);
        payOrder.put("param2", param2);
        payOrder.put("notifyUrl", notifyUrl);
        return payOrder;
    }



    private Object validateParams_old(JSONObject params, JSONObject payContext) {
        // 验证请求参数,参数有问题返回错误提示
        String errorMessage;
        // 支付参数
        String mchId = params.getString("mchId"); 			    // 商户ID
        String mchOrderNo = params.getString("mchOrderNo"); 	// 商户订单号
        String channelId = params.getString("channelId"); 	    // 渠道ID
        String amount = params.getString("amount"); 		    // 支付金额（单位分）
        String currency = params.getString("currency");         // 币种
        String clientIp = params.getString("clientIp");	        // 客户端IP
        String device = params.getString("device"); 	        // 设备
        String extra = params.getString("extra");		        // 特定渠道发起时额外参数
        String param1 = params.getString("param1"); 		    // 扩展参数1
        String param2 = params.getString("param2"); 		    // 扩展参数2
        String notifyUrl = params.getString("notifyUrl"); 		// 支付结果回调URL
        String sign = params.getString("sign"); 				// 签名
        String subject = params.getString("subject");	        // 商品主题
        String body = params.getString("body");	                // 商品描述信息
        // 验证请求参数有效性（必选项）
        if(StringUtil.isEmpty(mchId)) {
            errorMessage = "request params[mchId] error.";
            return errorMessage;
        }
        if(StringUtil.isEmpty(mchOrderNo)) {
            errorMessage = "request params[mchOrderNo] error.";
            return errorMessage;
        }
        if(StringUtil.isEmpty(channelId)) {
            errorMessage = "request params[channelId] error.";
            return errorMessage;
        }
        try {
            Long.valueOf(amount);
        } catch (NumberFormatException e) {
            errorMessage = "request params[amount] error.";
            return errorMessage;
        }
        if(StringUtil.isEmpty(currency)) {
            errorMessage = "request params[currency] error.";
            return errorMessage;
        }
        if(StringUtil.isEmpty(notifyUrl)) {
            errorMessage = "request params[notifyUrl] error.";
            return errorMessage;
        }
        if(StringUtil.isEmpty(subject)) {
            errorMessage = "request params[subject] error.";
            return errorMessage;
        }
        if(StringUtil.isEmpty(body)) {
            errorMessage = "request params[body] error.";
            return errorMessage;
        }
        // 根据不同渠道,判断extra参数
        if(PayConstant.PAY_CHANNEL_WX_JSAPI.equalsIgnoreCase(channelId)) {
            if(StringUtil.isEmpty(extra)) {
                errorMessage = "request params[extra] error.";
                return errorMessage;
            }
            JSONObject extraObject = JSON.parseObject(extra);
            String openId = extraObject.getString("openId");
            if(StringUtil.isEmpty(openId)) {
                errorMessage = "request params[extra.openId] error.";
                return errorMessage;
            }
        }else if(PayConstant.PAY_CHANNEL_WX_NATIVE.equalsIgnoreCase(channelId)) {
            if(StringUtil.isEmpty(extra)) {
                errorMessage = "request params[extra] error.";
                return errorMessage;
            }
            JSONObject extraObject = JSON.parseObject(extra);
            String productId = extraObject.getString("productId");
            if(StringUtil.isEmpty(productId)) {
                errorMessage = "request params[extra.productId] error.";
                return errorMessage;
            }
        }else if(PayConstant.PAY_CHANNEL_WX_MWEB.equalsIgnoreCase(channelId)) {
            if(StringUtil.isEmpty(extra)) {
                errorMessage = "request params[extra] error.";
                return errorMessage;
            }
            JSONObject extraObject = JSON.parseObject(extra);
            String productId = extraObject.getString("sceneInfo");
            if(StringUtil.isEmpty(productId)) {
                errorMessage = "request params[extra.sceneInfo] error.";
                return errorMessage;
            }
            if(StringUtil.isEmpty(clientIp)) {
                errorMessage = "request params[clientIp] error.";
                return errorMessage;
            }
        }

        // 签名信息
        if (StringUtil.isEmpty(sign)) {
            errorMessage = "request params[sign] error.";
            return errorMessage;
        }

        // 查询商户信息
        JSONObject mchInfo =null;
        //mchInfoService.getByMchId(mchId);
        if(mchInfo == null) {
            errorMessage = "Can't found mchInfo[mchId="+mchId+"] record in db.";
            return errorMessage;
        }
        if(mchInfo.getByte("state") != 1) {
            errorMessage = "mchInfo not available [mchId="+mchId+"] record in db.";
            return errorMessage;
        }

        String reqKey = mchInfo.getString("reqKey");
        if (StringUtil.isEmpty(reqKey)) {
            errorMessage = "reqKey is null[mchId="+mchId+"] record in db.";
            return errorMessage;
        }
        payContext.put("resKey", mchInfo.getString("resKey"));

        // 查询商户对应的支付渠道
        JSONObject payChannel =null;
        //payChannelService.getByMchIdAndChannelId(mchId, channelId);
        if(payChannel == null) {
            errorMessage = "Can't found payChannel[channelId="+channelId+",mchId="+mchId+"] record in db.";
            return errorMessage;
        }
        if(payChannel.getByte("state") != 1) {
            errorMessage = "channel not available [channelId="+channelId+",mchId="+mchId+"]";
            return errorMessage;
        }

        // 验证签名数据
        boolean verifyFlag = XXPayUtil.verifyPaySign(params, reqKey);
        if(!verifyFlag) {
            errorMessage = "Verify XX pay sign failed.";
            return errorMessage;
        }
        // 验证参数通过,返回JSONObject对象
        JSONObject payOrder = new JSONObject();
        // payOrder.put("payOrderId", SnowflakeUtils.getNextIdStr());
        payOrder.put("mchId", mchId);
        payOrder.put("mchOrderNo", mchOrderNo);
        payOrder.put("channelId", channelId);
        payOrder.put("amount", Long.parseLong(amount));
        payOrder.put("currency", currency);
        payOrder.put("clientIp", clientIp);
        payOrder.put("device", device);
        payOrder.put("subject", subject);
        payOrder.put("body", body);
        payOrder.put("extra", extra);
        //payOrder.put("channelMchId", payChannel.getString("channelMchId"));
        payOrder.put("param1", param1);
        payOrder.put("param2", param2);
        payOrder.put("notifyUrl", notifyUrl);
        return payOrder;
    }
}
