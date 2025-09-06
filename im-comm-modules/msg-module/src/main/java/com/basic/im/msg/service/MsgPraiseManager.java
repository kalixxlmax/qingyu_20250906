package com.basic.im.msg.service;

import com.basic.common.model.PageResult;
import com.basic.im.msg.entity.Praise;
import org.bson.types.ObjectId;

import java.util.List;

public interface MsgPraiseManager {

    ObjectId add(int userId, ObjectId msgId);

    boolean delete(int userId, ObjectId msgId);

    List<Praise> getPraiseList(Integer userId, ObjectId msgId, ObjectId praiseId, int pageIndex, int pageSize);

    boolean exists(int userId, ObjectId msgId);

    boolean existsCollect(int userId, ObjectId msgId);

    List<Praise> find(Integer userId, ObjectId msgId, ObjectId praiseId, int pageIndex, int pageSize);

    PageResult<Praise> praiseListMsg(ObjectId msgId, Integer page, Integer limit);
}
