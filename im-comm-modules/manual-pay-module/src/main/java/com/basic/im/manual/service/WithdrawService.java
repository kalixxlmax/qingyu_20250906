package com.basic.im.manual.service;

import com.basic.common.model.PageResult;
import com.basic.im.manual.entity.Withdraw;
import org.bson.types.ObjectId;

public interface WithdrawService {

    void addWithdraw(int userId,String money,String withdrawAccountId);

    Withdraw getWithdraw(ObjectId id);

    PageResult<Withdraw> getWithdrawList(int pageIndex, int pageSize, String keyword, long startTime, long endTime);

    Withdraw checkWithdraw(ObjectId id,int status);

    void deleteWithdraw(ObjectId id);
}
