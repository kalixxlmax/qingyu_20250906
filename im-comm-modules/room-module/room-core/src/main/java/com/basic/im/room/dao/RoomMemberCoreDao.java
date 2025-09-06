package com.basic.im.room.dao;

import com.basic.im.repository.IMongoDAO;
import com.basic.im.room.entity.Room;
import org.bson.types.ObjectId;

import java.util.List;

public interface RoomMemberCoreDao extends IMongoDAO<Room.Member, ObjectId> {

    List<Integer> getRoomPushUserIdList(ObjectId roomId);

    List<String> queryUserNoPushJidList(int userId);

    List<Integer> queryRoomMemberUserIdList(ObjectId roomId);

    List<ObjectId> getRoomIdListByUserId(Integer userId);

    List<ObjectId> getRoomIdListByType(Integer userId, int type);

    List<ObjectId> getRoomIdListByUser(Integer userId);

    Object getMemberIsNoPushMsg(ObjectId roomId, Integer userId);

    Room.Member getMember(ObjectId roomId, Integer userId);

    List<String> queryUserRoomsJidListByDB(int userId);

    void deleteRoomJidsUserId(Integer userId);

    void updateRoomJidsUserId(Integer userId, List<String> jids);

    void saveJidsByUserId(Integer userId, String jid, ObjectId roomId);

    void delJidsByUserId(Integer userId, String jid);

    List<String> getListByNotInBlack(List<String> objectIdList,ObjectId roomId, int isBlack);

    List<ObjectId> getRoomIdListLessType(Integer userId, int type);
}
