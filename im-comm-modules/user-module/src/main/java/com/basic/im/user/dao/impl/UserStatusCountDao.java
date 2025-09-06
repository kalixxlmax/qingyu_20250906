package com.basic.im.user.dao.impl;

import com.basic.im.repository.IMongoDAO;
import com.basic.im.user.entity.UserStatusCount;
import org.bson.types.ObjectId;

public interface UserStatusCountDao extends IMongoDAO<UserStatusCount, ObjectId> {

    void addUserStatusCount(UserStatusCount userStatusCount);

    UserStatusCount getUserStatusCount(long startTime,long endTime,int type);

    /**
     * 用户最高在线数量
     **/
    long maxOnlineCount();

}
