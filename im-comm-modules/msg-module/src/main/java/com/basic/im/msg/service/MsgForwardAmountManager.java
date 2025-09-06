package com.basic.im.msg.service;

import com.basic.im.msg.entity.ForwardAmount;
import org.bson.types.ObjectId;

import java.util.List;

public interface MsgForwardAmountManager {

    void addForwardAmount(int userId, String msgId);

    boolean exists(int userId, String msgId);

    List<ForwardAmount> find(ObjectId msgId, ObjectId forwardId, int pageIndex, int pageSize);
}
