package com.basic.im.manual.dao;

import com.basic.common.model.PageResult;
import com.basic.im.repository.IMongoDAO;
import com.basic.im.manual.entity.WithdrawAccount;
import org.bson.types.ObjectId;

import java.util.Map;

public interface WithdrawAccountDao extends IMongoDAO<WithdrawAccount, ObjectId> {

    void addWithdrawAccount(WithdrawAccount entity);

    void deleteWithdrawAccount(ObjectId id);

    void updateWithdrawAccount(ObjectId id, Map<String,Object> map);

    PageResult<WithdrawAccount> getWithdrawAccountList(int userId, int pageIndex, int pageSize);

    WithdrawAccount getWithdrawAccount(ObjectId id);
}
