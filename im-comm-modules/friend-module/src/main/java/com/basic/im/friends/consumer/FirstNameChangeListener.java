package com.basic.im.friends.consumer;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import com.basic.commons.thread.ThreadUtils;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.constant.TopicConstant;
import com.basic.im.friends.dao.FriendsDao;
import com.basic.im.friends.entity.Friends;
import com.basic.im.friends.service.FriendsRedisRepository;
import com.basic.im.friends.service.impl.FriendsManagerImpl;
import com.basic.im.utils.FirstCharUtil;
import com.basic.redisson.LockCallBack;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
@RocketMQMessageListener(topic = TopicConstant.FIRST_NAME_CHANGE_TOPIC, consumerGroup = "first_name_change_group")
public class FirstNameChangeListener implements RocketMQListener<JSONObject> {


    @Autowired
    private FriendsManagerImpl friendsManager;

    @Autowired
    private FriendsDao friendsDao;

    @Autowired
    private FriendsRedisRepository friendsRedisRepository;

    @Override
    @SneakyThrows
    public void onMessage(JSONObject obj) {
        try {
            Integer userId = obj.getInteger("userId");
            Integer toUserId = obj.getInteger("toUserId");
            String FIRST_NAME_CHANGE_LOCK_KEY = "fiends:first:name:change:lock:%s";
            friendsRedisRepository.executeOnLock(String.format(FIRST_NAME_CHANGE_LOCK_KEY, userId), KConstants.ZERO, KConstants.Expire.FIVE_MINUTES, lockCallback -> {
                // 更新单个好友
                if (ObjectUtil.isAllNotEmpty(userId, toUserId)) {
                    Friends friends = friendsManager.getFriends(userId, toUserId);
                    friends.setOrderKey(FirstCharUtil.first(StrUtil.isNotBlank(friends.getRemarkName()) ? friends.getRemarkName() : friends.getToNickname()));
                    friendsManager.updateFriends(friends);
                    friendsRedisRepository.deleteFriends(userId);
                    return true;
                }

                // 更新全部好友
                if (ObjectUtil.isNotNull(userId) && friendsRedisRepository.getOrderKeyAllUpdateFlag(userId)) {
                    ThreadUtils.executeInThread(thread -> {
                        log.info("start update orderKey,userId is {} ", userId);
                        try {
                            List<Friends> friends = friendsManager.queryFollow(userId, KConstants.ZERO);
                            if (CollectionUtil.isNotEmpty(friends)) {
                                friends.forEach(friend -> {
                                    friend.setOrderKey(FirstCharUtil.first(StrUtil.isNotBlank(friend.getRemarkName()) ? friend.getRemarkName() : friend.getToNickname()));
                                    friendsDao.updateFriends(friend);
                                });
                                friendsRedisRepository.deleteFriends(userId);
                                friendsRedisRepository.updateOrderKeyAllUpdateFlag(userId, true);
                            }
                        } catch (Exception e) {
                            friendsRedisRepository.updateOrderKeyAllUpdateFlag(userId, false);
                        }
                        log.info("end update orderKey,userId is {} ", userId);
                    });
                }
                return true;
            });
        } catch (NumberFormatException e) {
            log.info("format failure message is {}",e.getMessage());
        }
    }
}
