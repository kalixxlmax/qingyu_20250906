package com.basic.im.pay.service.impl;

import com.basic.im.pay.dao.ConsumeRecordDao;
import com.basic.common.model.PageResult;
import com.basic.im.pay.entity.BaseConsumeRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2019/12/2
 **/
@Slf4j
@Service
public class PaymentServerImpl {

    @Autowired
    private ConsumeRecordDao consumeRecordDao;

    public void addConsumRecord(BaseConsumeRecord consumeRecord) {
    }

    public PageResult<BaseConsumeRecord> recharge(int userId, int type, int page, int limit, String startDate, String endDate, String tradeNo){
        return  new PageResult<BaseConsumeRecord>();
    }

    public ConsumeRecordDao getConsumeRecordDao(){
        return consumeRecordDao;
    }

    public PageResult<BaseConsumeRecord> consumeRecordList(Integer userId, int page, int limit, byte state, String startDate, String endDate, int type){
        return new PageResult<BaseConsumeRecord>();
    }

    public PageResult<BaseConsumeRecord> getConsumeRecordByTradeNo(String tradeNo){
        return new PageResult<BaseConsumeRecord>();
    }

    public PageResult<BaseConsumeRecord> payment(int userId, int type, int page, int limit, String startDate, String endDate){
        return new PageResult<BaseConsumeRecord>();
    }

}
