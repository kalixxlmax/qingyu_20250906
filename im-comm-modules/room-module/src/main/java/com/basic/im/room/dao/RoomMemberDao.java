package com.basic.im.room.dao;

import com.basic.common.model.PageResult;
import com.basic.im.repository.IMongoDAO;
import com.basic.im.room.entity.Room;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;

public interface RoomMemberDao extends IMongoDAO<Room.Member, ObjectId> {

    void addMember(Room.Member entity);

    void addMemberList(List<Room.Member> memberList);

    Room.Member getMember(ObjectId roomId, int userId);



    List<Room.Member> getMemberList(ObjectId roomId,int pageIndex,int pageSize);

    List<Integer> getMemberUserIdList(ObjectId roomId);

    PageResult<Room.Member> getMemberListResult(ObjectId roomId, int pageIndex, int pageSize);

    List<Integer> getRoomAvatarUserIdList(ObjectId roomId);

    List<Room.Member> getMemberListLessThanOrEq(ObjectId roomId, int role, int pageIndex, int pageSize);

    List<Room.Member> getMemberListLessThan(ObjectId roomId,int role,int pageIndex,int pageSize);

    List<Room.Member> getMemberListGreaterThan(ObjectId roomId,int role,int pageIndex,int pageSize);

    List<Room.Member> getMemberListByTime(ObjectId roomId,int role,long createTime,int pageSize);

    List<Room.Member> getMemberListByNickname(ObjectId roomId,String nickName);

    void readNotice(ObjectId roomId, Integer userId);

    List<Room.Member> getMemberListByBlack(ObjectId roomId, Integer status);

    List<Room.Member> getMemberListOrder(ObjectId roomId);

    Map<String,Object> getMemberListOr(ObjectId roomId,int role,int userId,int pageIndex,int pageSize);

    List<Room.Member> getMemberListAdminRole(ObjectId roomId,int role,int pageSize);

    Object getMemberOneFile(ObjectId roomId,int userId,int offlineNoPushMsg);




    List<Integer>  getMemberUserIdList(ObjectId roomId,int role);

    Object findMemberRole(ObjectId roomId, int userId);

    void deleteRoomMember(ObjectId roomId,Integer userId);

    void updateRoomMember(ObjectId roomId,long talkTime);

    void updateRoomMemberDeadLine(ObjectId roomId,int userId,long deadLine);

    void updateRoomMemberRole(ObjectId roomId,int userId,int role);

    void updateRoomMember(ObjectId roomId, int userId, Map<String,Object> map);

    void updateRoomMemberAttribute(ObjectId roomId, int userId,String key,Object value);

    void updateRoomMemberNickName(ObjectId roomId, int userId,String key,Object value);

    long getMemberNumGreaterThanOrEq(ObjectId roomId,byte role,int pageIndex,int pageSize);

    long getMemberNumLessThan(ObjectId roomId,byte role,int userId);

    void setBeginMsgTime(ObjectId roomId, int userId,long clearMaxNo);

    long getAdminMemberNum(ObjectId roomId, byte role);

    List<String> joinGroupList(Integer userId);

    boolean exists(ObjectId roomId, Integer userId);

    void setHideChatSwitch(ObjectId roomId, Integer userId, byte hideChatSwitch);

    List<Room.Member> findByNotInUserIds(String roomIdByJid, List<Integer> notIn, int pageIndex, int pageSize);

    List<Room.Member> findByUserIds(int userId,String roomIdByJid, List<Integer> userIds, int pageIndex, int pageSize);

    List<Room.Member> findMemberByRoomId(int userId,String roomIdByJid, int pageIndex, int pageSize);

    List<Room.Member> getMemberListByTime(ObjectId roomId,int role,long createTime,int pageSize,String keyword);

    List<Room.Member> getMemberListByNickname(ObjectId roomId, String keyword, List<Integer> userIds);

    void resetReadNotice(ObjectId id, Integer userId);

}
