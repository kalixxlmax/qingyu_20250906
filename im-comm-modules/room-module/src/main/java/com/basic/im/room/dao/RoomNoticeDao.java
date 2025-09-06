package com.basic.im.room.dao;

import com.basic.im.repository.IMongoDAO;
import com.basic.im.room.entity.Room;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;

public interface RoomNoticeDao extends IMongoDAO<Room.Notice, ObjectId> {

    void addNotice(Room.Notice entity);

    void deleteNotice(ObjectId roomId,ObjectId noticeId);

    Room.Notice getNotice(ObjectId noticeId,ObjectId roomId);

    List<Room.Notice> getNoticList(ObjectId roomId,int pageIndex,int pageSize);

    List<Room.Notice> getNoticList(ObjectId roomId, long startTime,int limit);

    void updateNotic(ObjectId roomId, ObjectId noticeId, Map<String,Object> map);
}
