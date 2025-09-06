package com.basic.im.room.dao;

import com.basic.im.repository.IMongoDAO;
import com.basic.im.room.entity.Room;
import org.bson.types.ObjectId;

import java.util.List;

public interface RoomCoreDao extends IMongoDAO<Room,ObjectId> {


    Room getRoomById(ObjectId roomId);


    Room getRoomByJid(String roomJid);

    Room getRoomByDesc(String  desc);

    ObjectId getRoomId(String jid);

    String queryRoomJid(ObjectId roomId);

    String getRoomNameByJid(String jid);

    String getRoomNameByRoomId(ObjectId roomId);


    List<String> queryUserRoomsJidList(List<ObjectId> list);


    List<Room> getRoomByNameInIds(String keyword, List<ObjectId> roomIdList, int pageIndex, int pageSize);
}
