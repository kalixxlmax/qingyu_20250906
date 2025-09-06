package com.basic.payment.repository.impl;

import com.mongodb.client.result.UpdateResult;
import com.basic.mongodb.springdata.BaseMongoRepository;
import com.basic.payment.constant.PayConstant;
import com.basic.payment.dto.PaySuccessDTO;
import com.basic.payment.entity.PaymentOrderDO;
import com.basic.payment.repository.PaymentOrderRepository;
import com.basic.utils.DateUtil;
import com.basic.utils.StringUtil;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

@Repository
public class PaymentOrderRepositoryImpl extends BaseMongoRepository<PaymentOrderDO, ObjectId> implements PaymentOrderRepository {

    @Override
    public PaymentOrderDO queryPayOrder(String payOrderId) {
        return queryOne("payOrderId",payOrderId);
    }

    @Override
    public long queryPayOrderAmount(String payOrderId) {
        Query query=createQuery("payOrderId",payOrderId);
         query.fields().include("amount");
        PaymentOrderDO paymentOrderDO = findOne(query);

        if(null==paymentOrderDO){
            return 0;
        }
        return paymentOrderDO.getAmount();
    }

    @Override
    public boolean paySuccess(PaySuccessDTO successDTO) {
        Query query=createQuery("payOrderId",successDTO.getPayOrderId());

        Update update=createUpdate();
        update.set("channelId",successDTO.getChannelId());
        update.set("paySuccessTime",successDTO.getPaySuccessTime());
        update.set("clientIp",successDTO.getClientIp());
        update.set("device",successDTO.getDevice());

        update.set("status", PayConstant.PAY_STATUS_SUCCESS);

        update.set("modifyTime", DateUtil.currentTimeSeconds());

        UpdateResult update1 = update(query, update);

        return 0<update1.getModifiedCount();
    }


    @Override
    public long updatePayOrderCallBackResult(String payOrderId,String resultMessage) {
        Query query=createQuery("payOrderId",payOrderId);
        Update update=createUpdate();
        update.set("channelCallBackMessage",resultMessage);
        UpdateResult updateResult = update(query, update);
        return updateResult.getModifiedCount();
    }

    @Override
    public long updatePayOrderCallBackResult(PaymentOrderDO paymentOrderDO) {
        Query query=createQuery("payOrderId",paymentOrderDO.getPayOrderId());
        Update update=createUpdate();
        if(!StringUtil.isEmpty(paymentOrderDO.getChannelCallBackMessage())){
            update.set("channelCallBackMessage",paymentOrderDO.getChannelCallBackMessage());
        }


        update.set("status",paymentOrderDO.getStatus());
        update.set("channelId",paymentOrderDO.getChannelId());
        update.set("channelOrderNo",paymentOrderDO.getChannelOrderNo());
        if(!StringUtil.isEmpty(paymentOrderDO.getErrCode())){
            update.set("errCode",paymentOrderDO.getErrCode());
        }
        if(!StringUtil.isEmpty(paymentOrderDO.getErrMsg())){
            update.set("errMsg",paymentOrderDO.getErrMsg());
        }
        if(!StringUtil.isEmpty(paymentOrderDO.getDevice())){
            update.set("device",paymentOrderDO.getDevice());
        }


        update.set("modifyTime", DateUtil.currentTimeSeconds());
        if(0!=paymentOrderDO.getLastNotifyTime()){
            update.set("lastNotifyTime", paymentOrderDO.getLastNotifyTime());
        }
        if(0!=paymentOrderDO.getPaySuccessTime()){
            update.set("paySuccessTime", paymentOrderDO.getPaySuccessTime());
        }
       /*clientIp
        device*/
        UpdateResult updateResult = update(query, update);
        return updateResult.getModifiedCount();
    }
}
