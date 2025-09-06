package com.basic.im.msg.dao;
import com.basic.im.msg.entity.PlayAmount;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

import java.util.List;

public interface MsgPlayAmountDao extends IMongoDAO<PlayAmount, ObjectId> {

    void addPlayAmount(PlayAmount playAmount);

    boolean exists(int userId, String msgId);

    List<PlayAmount> find(ObjectId msgId, ObjectId playAmountId, int pageIndex, int pageSize);
}
