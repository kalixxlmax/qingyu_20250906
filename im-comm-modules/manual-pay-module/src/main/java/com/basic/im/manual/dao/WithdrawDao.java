package com.basic.im.manual.dao;

import com.basic.common.model.PageResult;
import com.basic.im.manual.entity.Withdraw;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

import java.util.Map;

public interface WithdrawDao extends IMongoDAO<Withdraw, ObjectId> {

    void addWithdraw(Withdraw entity);

    Withdraw getWithdraw(ObjectId id);

    PageResult<Withdraw> getWithdrawList(int pageIndex, int pageSize, String keyword, long startTime, long endTime);

    Map<String,Object> queryWithdraw(int pageIndex,int pageSize,String keyWord,long startTime,long endTime);

    Withdraw updateWithdraw(ObjectId id, Map<String,Object> map);

    void deleteWithdraw(ObjectId id);
}
