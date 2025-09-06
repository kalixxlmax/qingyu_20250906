package com.basic.im.live.dao;

import com.basic.im.live.entity.Black;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

public interface BlackDao extends IMongoDAO<Black, ObjectId> {

    void addBlack(Black entity);

    Black getBlack(ObjectId roomId, int userId);

    void deleteBlack(ObjectId roomId);


}
