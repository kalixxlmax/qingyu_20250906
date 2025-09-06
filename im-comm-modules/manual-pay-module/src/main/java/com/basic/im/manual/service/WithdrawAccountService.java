package com.basic.im.manual.service;

import com.basic.common.model.PageResult;
import com.basic.im.manual.entity.WithdrawAccount;
import org.bson.types.ObjectId;

public interface WithdrawAccountService {

    void addWithdrawAccount(WithdrawAccount entity);

    void deleteWithdrawAccount(ObjectId id);

    void updateWithdrawAccount(WithdrawAccount withdrawAccount);

    PageResult<WithdrawAccount> getWithdrawAccountList(int userId, int pageIndex, int pageSize);

    WithdrawAccount getWithdrawAccount(ObjectId id);
}
