package com.basic.im.friends.service;

import com.basic.common.model.PageResult;
import com.basic.im.friends.entity.Friends;
import com.basic.im.friends.entity.NewFriends;
import com.basic.im.vo.JSONMessage;

import java.util.List;


public interface FriendsManager {

	Friends addBlacklist(Integer userId, Integer toUserId,Integer isNotify);


	void deleteFansAndFriends(int userId);

	boolean addFriends(Integer userId, Integer toUserId);

	Friends deleteBlacklist(Integer userId, Integer toUserId,Integer isNotify);

	boolean deleteFriends(Integer userId, Integer toUserId);

	JSONMessage followUser(Integer userId, Integer toUserId, Integer fromAddType);

	JSONMessage batchFollowUser(Integer userId, String toUserId);


	Friends getFriends(Friends friends);

	Friends getFriends(int userId, int toUserId);

	List<Integer> queryFriendUserIdList(int userId);


	List<Friends> queryBlacklist(Integer userId, int pageIndex, int pageSize);


	List<Friends> queryFollow(Integer userId, int status);
	List<Friends> queryFollow(Integer userId, int status,int searchType,List<Integer> notInUserIds,String keyword,int pageIndex,int pageSize);

	List<Integer> queryFollowId(Integer userId);

	List<Friends> queryFriends(Integer userId);

	PageResult<Friends> queryFriends(Integer userId, int status, String keyword, int pageIndex, int pageSize);

	boolean unfollowUser(Integer userId, Integer toUserId);

	Friends updateRemark(int userId, int toUserId, String remarkName, String describe);

	List<NewFriends> newFriendList(int userId, int pageIndex, int pageSize);

	List<Integer> friendsAndAttentionUserId(Integer userId, String type);

	// 消息免打扰、阅后即焚、聊天置顶
	Friends updateOfflineNoPushMsg(int userId, int toUserId, int offlineNoPushMsg, int type);

    boolean getFriendIsNoPushMsg(int to, int from);

	/**
	 * 设置隐藏会话
	 */
	void setHideChatSwitch(int userId,int toUserId,byte hideChatSwitch);

    void cleanFriendsSecretMsg(List<Integer> userIdList, Integer userId);

    void cleanFriendsAllMsg(List<Integer> userIdList, Integer userId);

}
