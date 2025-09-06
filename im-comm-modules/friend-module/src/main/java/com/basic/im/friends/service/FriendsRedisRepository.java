package com.basic.im.friends.service;

import cn.hutool.core.util.ObjectUtil;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.friends.entity.Friends;
import com.basic.redisson.AbstractRedisson;
import org.redisson.api.RList;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class FriendsRedisRepository extends AbstractRedisson {

    @Autowired(required=false)
    private RedissonClient redissonClient;

    @Override
    public RedissonClient getRedissonClient() {
        return redissonClient;
    }
    
    /**
     * 用户的好友userId列表
     */
    public static final String FRIENDS_USERIDS="friends:toUserIds:%s";

    /**
     * 用户通讯录好友userId列表
     */
    public static final String ADDRESSBOOK_USERIDS="addressBook:userIds";
    
    /**
     * 用户的好友列表
     */
    public static final String FRIENDS_USERS="friends:toUsers:%s";


    /**
     * 好友排序 Key 全量更新标志位
     */
    public static final String ORDER_KEY_ALL_UPDATE_FLAG="friends:orderKey:flag:%s";

    /**
     * 删除通讯录好友userIds列表
     **/
    public void delAddressBookFriendsUserIds(Integer userId){
        String key = String.format(ADDRESSBOOK_USERIDS,userId);
        redissonClient.getBucket(key).delete();
    }

    /**
     *  获取通讯录好友列表userIds
     **/
    public List<Integer> getAddressBookFriendsUserIds(Integer userId){
        String key = String.format(ADDRESSBOOK_USERIDS,userId);
        RList<Integer> list = redissonClient.getList(key);
        return list.readAll();
    }

    /**
     * 维护用户通讯录好友列表userIds
     **/
    public void saveAddressBookFriendsUserIds(Integer userId,List<Integer> friendIds){
        String key = String.format(ADDRESSBOOK_USERIDS,userId);
        RList<Object> list = redissonClient.getList(key);
        list.clear();
        list.addAll(friendIds);
        list.expire(KConstants.Expire.DAY7, TimeUnit.SECONDS);
    }

    /** 
     * 删除用户userIds
     **/
    public void deleteFriendsUserIdsList(Integer userId){
        String key = String.format(FRIENDS_USERIDS,userId);
        redissonClient.getBucket(key).delete();
    }

    /**
     * 获取好友列表userIds
     **/
    public List<Integer> getFriendsUserIdsList(Integer userId){
        String key = String.format(FRIENDS_USERIDS,userId);
        RList<Integer> list = redissonClient.getList(key);
        return list.readAll();
    }

    /** 
     *  维护用户好友列表
     **/
    public void saveFriendsList(Integer userId,List<Friends> friends){

        String key = String.format(FRIENDS_USERS,userId);
        RList<Object> bucket = redissonClient.getList(key);
        bucket.clear();
        bucket.addAll(friends);
        bucket.expire(KConstants.Expire.DAY7, TimeUnit.SECONDS);
    }

    public void addFriendsList(Integer userId,List<Friends> friends){

        String key = String.format(FRIENDS_USERS,userId);
        RList<Object> bucket = redissonClient.getList(key);
        bucket.addAll(friends);
        bucket.expire(KConstants.Expire.DAY7, TimeUnit.SECONDS);
    }


    /** 
     *  删除用户好友列表
     **/
    public void deleteFriends(Integer userId){
        String key = String.format(FRIENDS_USERS,userId);
        redissonClient.getBucket(key).delete();
    }

    /** 
     * 获取好友列表
     **/
    public List<Friends> getFriendsList(Integer userId){
        String key = String.format(FRIENDS_USERS,userId);
        RList<Friends> friendList = redissonClient.getList(key);
        return friendList.readAll();
    }


    /** 
     *  维护用户好友列表userIds
     **/
    public void saveFriendsUserIdsList(Integer userId,List<Integer> friendIds){
        String key = String.format(FRIENDS_USERIDS,userId);
        RList<Object> list = redissonClient.getList(key);
        list.clear();
        list.addAll(friendIds);
        list.expire(KConstants.Expire.DAY7, TimeUnit.SECONDS);
    }


    /**
     * 获取用户好友排序键全量更新字段
     */
    public boolean getOrderKeyAllUpdateFlag(Integer userId) {
        Object flag = redissonClient.getBucket(String.format(ORDER_KEY_ALL_UPDATE_FLAG, userId)).get();
        return !ObjectUtil.isNotNull(flag) || !((boolean) flag);
    }

    /**
     * 更新用户好友排序键全量更新字段
     */
    public void updateOrderKeyAllUpdateFlag(Integer userId,boolean flag) {
        redissonClient.getBucket(String.format(ORDER_KEY_ALL_UPDATE_FLAG,userId)).set(flag);
    }
}
