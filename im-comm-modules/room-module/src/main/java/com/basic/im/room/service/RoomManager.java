package com.basic.im.room.service;

import com.alibaba.fastjson.JSONObject;
import com.basic.im.room.entity.Room;
import com.basic.im.room.entity.Room.Share;
import com.basic.im.room.vo.RoomPayVO;
import com.basic.im.user.entity.User;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;


public interface RoomManager {
	public static final String BEAN_ID = "RoomManagerImpl";

	Room add(User user, Room room, List<Integer> memberUserIdList, JSONObject userKeys);


	void delete( ObjectId roomId,Integer userId);


	Room get(ObjectId roomId,Integer pageIndex,Integer pageSize);

	Room getRoomByJid(String roomJid);

	Room exisname(Object roomname,ObjectId roomId);

	List<Room> selectList(int pageIndex, int pageSize, String roomName, int isJoinGroup, Integer userId,int isSearchAllJoinGroup);

	Object selectHistoryList(int userId, int type);

	Object selectHistoryList(int userId, int type, int pageIndex, int pageSize);

	void deleteMember(User user, ObjectId roomId, int userId,boolean deleteUser);

	void updateMember(User user, ObjectId roomId, Room.Member member, int operationType, int inviteUserId);

	void updateRoomAvatarUserIds(Room room);

	//	List<Integer> updateMember(User user, ObjectId roomId, List<Integer> idList,JSONObject userKeys);
Map<String, List<Integer>> updateMember(User user, ObjectId roomId, List<Integer> idList, JSONObject userKeys,int operationType,int inviteUserId);

	void Memberset(Integer offlineNoPushMsg,ObjectId roomId,int userId,int type);

	Room.Member getMember(ObjectId roomId, int userId);

	List<Room.Member> getMemberList(ObjectId roomId,String keyword);

    Room.Notice updateNotice(ObjectId roomId, ObjectId noticeId, String noticeContent, Integer userId);

    void join(int userId, ObjectId roomId, int type, int operationType,int periods);

    void paySuccess(int userId, ObjectId roomId, int type, int operationType,int periods);

	void setAdmin(ObjectId roomId,int touserId,int type,int userId);

	Share Addshare(ObjectId roomId,float size,int type ,int userId, String url,String name);
	
	List<Room.Share> findShare(ObjectId roomId,long time,int userId,int pageIndex,int pageSize);
	
	Room.Share getShare(ObjectId roomId, ObjectId shareId);
	
	void deleteShare(ObjectId roomId,ObjectId shareId,int userId);
	
	String getCall(ObjectId roomId);
	
	String getVideoMeetingNo(ObjectId roomId);

	Long countRoomNum();

    Room getRoom(ObjectId objRoomId, Integer adminUserId);

	void joinRoom(Integer userId, String name, ObjectId objRoomId, long currentTime, Integer adminUserId);

	List<ObjectId> getRoomIdList(Integer userId);

	String getRoomName(ObjectId objectId);

	Integer getRoomStatus(ObjectId objectId);

	void deleteRedisRoom(String toString);


	void editBlackList(Integer userId, ObjectId objectId, Integer status,Integer... toUserIds);

	List<Room.Member> getBlackMemberList(String roomId,Integer status);

    void setHideChatSwitch(ObjectId roomId, Integer userId, byte hideChatSwitch);

	List<Room.Member> findByUserIds(int userId,String roomJid, List<Integer> userIds, int pageIndex, int pageSize);
	List<Room.Member> findByNotInUserIds(String roomJid, List<Integer> notIn,int pageIndex, int pageSize);
	List<Room.Member> findMemberByRoomJid(int userId,String roomJid, int pageIndex, int pageSize);

	Object setPay(User user, RoomPayVO roomPayVO);

	/**
	 * 过期的付费群聊成员的删掉
	 */
	void deleteDealLine();

	/**
	 * 设置成员过期时间
	 * @param objectId
	 * @param userId
	 * @param time
	 */
	void setDealLine(ObjectId objectId, String userId, long time);
}
