package com.basic.im.manual.dao;

import com.basic.common.model.PageResult;
import com.basic.im.repository.IMongoDAO;
import com.basic.im.manual.entity.ReceiveAccount;
import org.bson.types.ObjectId;

import java.util.Map;

public interface ReceiveAccountDao extends IMongoDAO<ReceiveAccount,ObjectId> {

    void  addReceiveAccount(ReceiveAccount receiveAccount);

    void updateReceiveAccount(ObjectId id, Map<String,Object> map);

    void deleteReceiveAccount(ObjectId id);

    PageResult<ReceiveAccount> getReceiveAccountList(int pageIndex, int pageSize, int type, String keyword);

    ReceiveAccount getReceiveAccount(ObjectId id);
}
