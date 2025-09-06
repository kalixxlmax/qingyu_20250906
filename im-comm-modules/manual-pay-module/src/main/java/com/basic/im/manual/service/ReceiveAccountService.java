package com.basic.im.manual.service;

import com.basic.common.model.PageResult;
import com.basic.im.manual.entity.ReceiveAccount;
import org.bson.types.ObjectId;

public interface ReceiveAccountService {

    void addReceiveAccount(ReceiveAccount receiveAccount);

    void updateReceiveAccount(ObjectId id, ReceiveAccount receiveAccount);

    void deleteReceiveAccount(ObjectId id);

    PageResult<ReceiveAccount> getReceiveAccountList(int pageIndex, int pageSize, int type, String keyword);

    ReceiveAccount getReceiveAccount(ObjectId id);
}
