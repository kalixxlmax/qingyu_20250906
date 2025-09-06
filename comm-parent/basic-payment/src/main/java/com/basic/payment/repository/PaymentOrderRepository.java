package com.basic.payment.repository;

import com.basic.mongodb.springdata.IBaseMongoRepository;
import com.basic.payment.dto.PaySuccessDTO;
import com.basic.payment.dto.PaymentOrderDTO;
import com.basic.payment.entity.PaymentOrderDO;
import org.bson.types.ObjectId;

public interface PaymentOrderRepository extends IBaseMongoRepository<PaymentOrderDO, ObjectId> {

    /**
     * 查询支付订单
     * @param payOrderId
     * @return
     */
    PaymentOrderDO queryPayOrder(String payOrderId);

    /**
     * 查询支付订单金额
     * @param payOrderId
     * @return
     */
    long queryPayOrderAmount(String payOrderId);


    /**
     * 订单支付成功
     * @param successDTO
     * @return
     */
    boolean paySuccess(PaySuccessDTO successDTO);

    long updatePayOrderCallBackResult(String payOrderId, String resultMessage);

    long updatePayOrderCallBackResult(PaymentOrderDO paymentOrderDO);
}
