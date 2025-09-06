package com.basic.im.friends.dao;

import com.basic.common.model.PageResult;
import com.basic.im.friends.entity.Friends;
import com.basic.im.friends.entity.NewFriends;
import com.basic.im.repository.IMongoDAO;
import com.basic.im.user.event.KeyPairChageEvent;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;

public interface FriendsDao extends IMongoDAO<Friends, ObjectId> {

    Friends deleteFriends(int userId, int toUserId);

    void deleteFriends(int userId);

    Friends getFriends(int userId, int toUserId);

    List<Friends> queryBlacklist(int userId, int pageIndex, int pageSize);

    List<Friends> queryFriendsList(Integer userId, Integer status, Integer userType, List<Integer> notInUserIds,String keyword,int pageIndex, int pageSize,Boolean isOrder);
    List<Friends> queryFriendsList(int userId, int status, int pageIndex, int pageSize);

    /**
     * 查询好友的用户ID 列表 排除拉黑关系
     */
    List<Integer> queryFriendUserIdList(int userId);

    PageResult<Friends> queryFollowByKeyWord(int userId, int status, String keyWord, int pageIndex, int pageSize);

    List<Integer> queryFollowId(int userId);

    List<Friends> queryFriends(int userId);

    List<Friends> queryAllFriends(Integer userId);

    long queryFriendsCount(Integer userId);

    List<Friends> friendsOrBlackList(int userId, String type);

    List<Integer> friendsOrBlackUserIdList(int userId, String type);

    Object saveFriends(Friends friends);

    Friends updateFriends(Friends friends);

    void updateFriends(int userId, int toUserId, Map<String, Object> map);

    void updateFriendsAttribute(int userId, int toUserId, String key, Object value);

    void updateMsgKeyPair(KeyPairChageEvent event);

    Friends updateFriendsReturn(int userId, int toUserId, Map<String, Object> map);

    void updateFriendsEncryptType(int userId, int toUserId, byte type);

    /**
     * 修改备注手机号
     */
    void updatePhoneRemark(int userId,int toUserId,String phoneRemark);

    List<Friends> queryBlacklistWeb(int userId, int pageIndex, int pageSize);


    PageResult<Friends> consoleQueryFollow(int userId, int toUserId, int status, int page, int limit);


    Friends updateFriendRemarkName(Integer userId, Integer toUserId, String remarkName, String describe);

    List<Object> getAddFriendsCount(long startTime, long endTime, String mapStr, String reduce);

    List<NewFriends> getNewFriendsList(int userId, int pageIndex, int pageSize);

    List<NewFriends> getLastNewFriendsList(int userId);

    NewFriends getNewFriendLast(int userId, int toUserId);

    long getFriendsCount(int userId);

    long queryAllFriendsCount();

    boolean getFriendIsNoPushMsg(int to, int from);

    void setHideChatSwitch(int userId, int toUserId, byte hideChatSwitch);

    void updateHiding(int userId, int hiding);

    List<Friends> queryBlackList(Integer userId);

    List<Friends> queryFriendsByToUserId(int userId);

    void updateFriend(ObjectId id,int userId, Map<String, Object> map);

    void setFriendsSendMsgState(Integer userId, Integer toUserId, Byte isSendMsgState);


    List<Integer> queryEncryptFriendUserIdList(Integer userId);

    void cleanAndupdateOldData();


}
