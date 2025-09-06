package com.basic.im.manual.service;

import com.basic.common.model.PageResult;
import com.basic.im.manual.entity.Recharge;
import org.bson.types.ObjectId;

public interface RechargeService {

    void addRecharge(int userId,Double money,int type,String payName);

    Recharge getRecharge(ObjectId id);

    PageResult<Recharge> getRechargeList(int pageIndex, int pageSize, String keyword, long startTime, long endTime);

    Recharge checkRecharge(ObjectId id,int status);

    void deleteRecharge(ObjectId id);
}
