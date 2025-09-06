package com.basic.im.open.dao;

import com.basic.im.open.entity.Helper;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

import java.util.List;

public interface HelperDao extends IMongoDAO<Helper, ObjectId> {

    Helper getHelper(ObjectId Id);

    List<Helper> getHelperList(String openAppId,int pageIndex,int pageSize);

    void deleteHelper(String openAppId);

    void updateHelper(Helper helper);

     void deleteHelper(Integer userId,ObjectId id);
}
