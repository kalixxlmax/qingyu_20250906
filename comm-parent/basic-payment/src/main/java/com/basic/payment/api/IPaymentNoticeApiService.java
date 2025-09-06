package com.basic.payment.api;


import com.alibaba.fastjson.JSONObject;
import com.basic.payment.dto.*;
import com.basic.payment.ex.PayOrderException;

import java.util.Map;

/**
 * 支付平台发送通知接口
 */
public interface IPaymentNoticeApiService {




    /**
     * 支付平台回调通知
     * @param params
     * @return
     */
   public String handlePayCallBackNotify(String channelId, Map<String, String> params);
    /**
     * 支付平台回调通知
     * @param params
     * @return
     */
   public String handlePayCallBackNotify(String channelId, String params);


   public boolean paySuccessNotify(PaySuccessDTO successDTO);







}
