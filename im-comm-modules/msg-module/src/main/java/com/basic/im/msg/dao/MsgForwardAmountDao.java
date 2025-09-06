package com.basic.im.msg.dao;

import com.basic.im.msg.entity.ForwardAmount;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

import java.util.List;

public interface MsgForwardAmountDao extends IMongoDAO<ForwardAmount, ObjectId> {

    void addForwardAmount(ForwardAmount forwardAmount);

    boolean exists(int userId, String msgId);

    List<ForwardAmount> find(ObjectId msgId, ObjectId forwardId, int pageIndex, int pageSize);
}
