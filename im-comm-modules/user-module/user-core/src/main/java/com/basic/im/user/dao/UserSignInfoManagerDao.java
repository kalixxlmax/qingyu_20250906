package com.basic.im.user.dao;

import com.basic.im.repository.IMongoDAO;
import com.basic.im.user.entity.UserSignInfo;
import org.bson.types.ObjectId;

import java.util.List;

public interface UserSignInfoManagerDao extends IMongoDAO<UserSignInfo, ObjectId> {

    List<UserSignInfo> getUserSignInfoByUserId(Integer userId);

    void saveUserSignInfo(UserSignInfo userSignInfo);

    void updateUserSignInfo(UserSignInfo userSignInfo);

    void updateUserSignInfoCount(UserSignInfo userSignInfo);
}
