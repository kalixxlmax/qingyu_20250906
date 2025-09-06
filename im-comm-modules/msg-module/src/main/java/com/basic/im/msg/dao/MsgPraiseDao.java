package com.basic.im.msg.dao;

import com.basic.common.model.PageResult;
import com.basic.im.msg.entity.Praise;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;

public interface MsgPraiseDao extends IMongoDAO<Praise, ObjectId> {
    void add(Praise praise);

//    boolean delete(int userId, ObjectId msgId);

    List<Praise> find(List<Integer> userIds, ObjectId msgId, ObjectId praiseId, int pageIndex, int pageSize);

    boolean exists(int userId, ObjectId msgId);

    void update(int userid, Map<String, Object> map);

    List<ObjectId> getPraiseIds(Integer userId);

    Praise getPraise(int userId, ObjectId msgId);

    void deletePraise(int userId, ObjectId msgId);

    boolean existsCollect(int userId, ObjectId msgId);

    PageResult<Praise> praiseListMsg(ObjectId msgId, Integer page, Integer limit);
}
