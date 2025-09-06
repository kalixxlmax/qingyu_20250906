package com.basic.im.manual.dao;

import com.basic.common.model.PageResult;
import com.basic.im.manual.entity.Recharge;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

import java.util.Map;

public interface RechargeDao extends IMongoDAO<Recharge, ObjectId> {

    void addRecharge(Recharge entity);

    Recharge getRecharge(ObjectId id);

    PageResult<Recharge> getRechargeList(int pageIndex, int pageSize, String keyword, long startTime, long endTime);

    Map<String,Object> queryRecharge(int pageIndex, int pageSize,String keyword,long startTime,long endTime);

    Recharge updateRecharge(ObjectId id, Map<String,Object> map);

    void deleteRecharge(ObjectId id);

}
