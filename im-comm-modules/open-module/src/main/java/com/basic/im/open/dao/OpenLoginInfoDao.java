package com.basic.im.open.dao;

import com.basic.im.open.opensdk.entity.OpenLoginInfo;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

public interface OpenLoginInfoDao extends IMongoDAO<OpenLoginInfo, ObjectId> {

    void addOpenLoginInfo(OpenLoginInfo openLoginInfo);

    OpenLoginInfo getOpenLoginInfo(int userId);
}
