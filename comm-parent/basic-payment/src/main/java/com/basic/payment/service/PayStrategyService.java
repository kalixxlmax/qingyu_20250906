package com.basic.payment.service;

import com.alibaba.fastjson.JSONObject;
import com.basic.payment.constant.PayConstant;
import com.basic.payment.dto.PayCallBackResultDTO;
import com.basic.payment.entity.PaymentOrderDO;
import com.basic.payment.ex.PayOrderException;
import com.basic.payment.model.*;
import com.basic.payment.strategy.PayStrategy;
import com.basic.payment.strategy.impl.AliPayStrategy;
import com.basic.payment.strategy.impl.WxPayStrategy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class PayStrategyService implements CommandLineRunner {


    @Autowired
    private AliPayStrategy aliPayStrategy;

    @Autowired
    private WxPayStrategy wxPayStrategy;


    private Map<String, PayStrategy> payStrategyMap=new HashMap<>();


    public PayStrategy getPayStrategyByChannel(String channelId){

        PayStrategy payStrategy = payStrategyMap.get(channelId);
        if(null==payStrategy){
            throw new PayOrderException(PayConstant.ERROR_PAY_CHANNEL_DISABLE);
        }
        return payStrategy;
    }

    /**
     * 向支付平台发起下单支付请求
     * @param channelId
     * @param payRequestModel
     * @return
     */
    public JSONObject doPayReq(String channelId, PayRequestModel payRequestModel) throws PayOrderException {
        PayStrategy payStrategy = getPayStrategyByChannel(channelId);

        return payStrategy.doPayReq(payRequestModel);
    }


    public PayCallBackResultDTO handlePayCallBackNotify(String channelId, String params){
        return getPayStrategyByChannel(channelId).handlePayCallBackNotify(params);
    }

    public PayCallBackResultDTO handlePayCallBackNotify(String channelId,Map params){
        return getPayStrategyByChannel(channelId).handlePayCallBackNotify(params);
    }


    private void requestLog(String method,BaseModel request){

        //mchOrderNo {}
        log.info(method+"  channel {}  channelOrderNo {}  payOrderId {} ",
                request.getChannelId(),request.getChannelOrderNo(),request.getPayOrderId());
    }
    private void resultLog(String method,BaseModel request,JSONObject result){
        log.info(method+"  channel {}  channelOrderNo {}  payOrderId {} \n result {} ",
                request.getChannelId(), request.getChannelOrderNo(), request.getPayOrderId(), result.toJSONString());
    }
    /**
     * 查询订单
     * @param request
     * @return
     */
    public JSONObject queryOrder(OrderQueryRequest request){

        requestLog("queryOrder",request);

        JSONObject result = getPayStrategyByChannel(request.getChannelId()).queryOrder(request);
        if(null!=result) {
            resultLog("queryOrder",request,result);
        }
        return result;
    }
    public JSONObject queryOrder(String channelId,String channelOrderNo,String payOrderId){
        log.info("queryOrder channel {}  channelOrderNo {}  payOrderId {} ",
                channelId,channelOrderNo,payOrderId);
        JSONObject result = getPayStrategyByChannel(channelOrderNo).queryOrder(channelOrderNo,payOrderId);
        if(null!=result) {
            log.info("queryOrder channel {}  channelOrderNo {}  payOrderId {} \n result {} ",
                    channelId, channelOrderNo, payOrderId, result.toJSONString());
        }
        return result;
    }

    public JSONObject closeOrder(String channelId,String channelOrderNo,String payOrderId){
        log.info("closeOrder channel {}  channelOrderNo {}  payOrderId {} ",
                channelId,channelOrderNo,payOrderId);
        JSONObject result = getPayStrategyByChannel(channelId).closeOrder(channelOrderNo, payOrderId);
        if(null!=result) {
            log.info("closeOrder channel {}  channelOrderNo {}  payOrderId {} \n result {} ",
                    channelId, channelOrderNo, payOrderId, result.toJSONString());
        }
        return result;
    }

    public JSONObject closeOrder(OrderCloseRequest request){
        requestLog("closeOrder",request);
        JSONObject result = getPayStrategyByChannel(request.getChannelId())
                .closeOrder(request.getChannelOrderNo(), request.getPayOrderId());
        if(null!=result) {
            resultLog("closeOrder",request,result);
        }
        return result;
    }



    /**
     * 订单退款
     * @param request
     * @return
     */
    public JSONObject refund(OrderRefundRequest request){
        requestLog("refund",request);


        JSONObject result = getPayStrategyByChannel(request.getChannelId()).refund(request);

        if(null!=result) {
            resultLog("refund",request,result);
        }
        return result;
    }

    /**
     * 退款查询
     * @param request
     * @return
     */
    public JSONObject queryRefund(OrderRefundQueryRequest request){
        requestLog("queryRefund",request);
        JSONObject result = getPayStrategyByChannel(request.getChannelId()).queryRefund(request);
        if(null!=result) {
            resultLog("queryRefund",request,result);
        }
        return result;
    }


    @Override
    public void run(String... args) throws Exception {
        payStrategyMap.put(PayConstant.PAY_CHANNEL_ALIPAY,aliPayStrategy);
        payStrategyMap.put(PayConstant.PAY_CHANNEL_WXPAY,wxPayStrategy);

    }

    public boolean addPaychannelStrategy(String channelId,PayStrategy payStrategy){
       return null!=payStrategyMap.put(channelId,payStrategy);
    }

}
