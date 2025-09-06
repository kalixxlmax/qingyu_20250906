package com.basic.im.room.dao;

import com.basic.im.repository.IMongoDAO;
import com.basic.im.room.entity.Room;
import org.bson.types.ObjectId;

import java.util.List;

public interface ShareDao extends IMongoDAO<Room.Share, ObjectId> {

    void addShare(Room.Share share);

    Room.Share getShare(ObjectId roomId, ObjectId shareId);

    List<Room.Share> getShareList(ObjectId roomId,int userId,int pageIndex,int pageSize,long time);

    void deleteShare(ObjectId roomId, ObjectId shareId);

    List<String> getShareUrlList(ObjectId roomId);

}
