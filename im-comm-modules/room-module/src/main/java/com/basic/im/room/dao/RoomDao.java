package com.basic.im.room.dao;

import com.basic.common.model.PageResult;
import com.basic.im.repository.IMongoDAO;
import com.basic.im.room.entity.Room;
import com.basic.im.room.vo.NearByRoom;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;

public interface RoomDao extends IMongoDAO<Room, ObjectId> {

    void addRoom(Room room);


    Room getRoom(String roomname, ObjectId roomId);

    long getAllRoomNums();

    void updateRoomUserSize(ObjectId roomId, int userSize);

    void updateRoom(ObjectId roomId, Map<String,Object> map);

    void updateRoomByUserId(int userId,Map<String,Object> map);

    void deleteRoom(ObjectId roomId);

    Integer getCreateUserId(ObjectId roomId);



    Integer queryRoomStatus(ObjectId roomId);



    Integer getRoomStatus(ObjectId roomId);


    List<Room> getRoomList(List<ObjectId> list,int s,int pageIndex,int pageSize);

    List<Room> getRoomList(NearByRoom nearByRoom);

    List<Room> getRoomListOrName(int pageIndex, int pageSize, String roomName,int isSearchAllJoinGroup);

    List<Object> getAddRoomsCount(long startTime,long endTime,String mapStr,String reduce);


    PageResult<Room> adminQueryRoomList(int page, int limit, int leastNumbers, String keyWorld, String isSecretGroup);

    PageResult<Room> queryActiveRoomList(int page, int limit);

    String getRoomIdByJid(String roomJid);

    //---------------------------------------以下是没有实体且数据操作不多的--------------------------------------------------
    List<String> queryRoomHistoryFileType(String roomJid);

    void dropRoomChatHistory(String roomJid);

    Room getRoom(ObjectId roomId);


    void setRoomFlag(Integer userId, ObjectId objectId, int flag);


    List<Room> queryFlagRoomList(Integer userId);

    List<ObjectId> queryFlagRoomIdList(Integer userId);

    List<Room> queryFlagRoomList(List<ObjectId> roomIdList);

    List<Room> getRoomListByCreateTime(long endTime);

}
