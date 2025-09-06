package com.basic.im.friends.service.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import com.basic.common.model.PageResult;
import com.basic.common.model.PageVO;
import com.basic.commons.thread.ThreadUtils;
import com.basic.im.comm.constants.MsgType;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.common.MultipointSyncUtil;
import com.basic.im.constant.TopicConstant;
import com.basic.im.friends.dao.FriendsDao;
import com.basic.im.friends.entity.Friends;
import com.basic.im.friends.entity.NewFriends;
import com.basic.im.friends.service.FriendsManager;
import com.basic.im.friends.service.FriendsRedisRepository;
import com.basic.im.message.IMessageRepository;
import com.basic.im.message.MessageService;
import com.basic.im.message.MessageType;
import com.basic.im.support.Callback;
import com.basic.im.user.dao.OfflineOperationDao;
import com.basic.im.user.entity.OfflineOperation;
import com.basic.im.user.entity.User;
import com.basic.im.user.event.KeyPairChageEvent;
import com.basic.im.user.event.UserChageNameEvent;
import com.basic.im.user.service.*;
import com.basic.im.utils.MqMessageSendUtil;
import com.basic.im.utils.SKBeanUtils;
import com.basic.im.vo.JSONMessage;
import com.basic.utils.MapUtil;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.basic.im.comm.constants.KConstants.*;

@Service
@Slf4j
public class FriendsManagerImpl implements FriendsManager, CommandLineRunner {

    private static final String groupCode = "110";

    private static Logger Log = LoggerFactory.getLogger(FriendsManager.class);

    @Autowired
    private FriendsDao friendsDao;

    public FriendsDao getFriendsDao() {
        return friendsDao;
    }

    @Autowired
    private OfflineOperationDao offlineOperationDao;

    @Autowired
    private AuthKeysService authKeysService;

    @Autowired
    private AddressBookManagerImpl addressBookManager;

    @Autowired
    private FriendGroupManagerImpl friendGroupManager;

    @Autowired
    private FriendsRedisRepository friendsRedisRepository;

    @Autowired
    private MessageService messageService;

    @Autowired(required = false)
    private IMessageRepository messageRepository;

    @Autowired
    private RoleCoreService roleCoreService;

    @Autowired
    private UserCoreService userManager;

    @Autowired(required = false)
    private UserHandler userHandler;

    @Autowired
    private UserCoreRedisRepository userCoreRedis;

    @Autowired
    private UserCoreService userCoreService;

    public FriendsRedisRepository getFriendsRedisRepository() {
        return friendsRedisRepository;
    }


    private UserCoreService getUserManager() {
        return userManager;
    }

    /**
     * 维护用户通讯录好友缓存
     **/
    private void deleteAddressFriendsInfo(Integer userId, Integer toUserId) {
        // 通讯录好友id
        friendsRedisRepository.delAddressBookFriendsUserIds(userId);
        friendsRedisRepository.delAddressBookFriendsUserIds(toUserId);
        deleteFriendsInfo(userId, toUserId);
    }

    /**
     * 维护用户好友缓存
     **/
    public void deleteFriendsInfo(Integer userId, Integer toUserId) {
        // 好友userIdsList
        friendsRedisRepository.deleteFriendsUserIdsList(userId);
        friendsRedisRepository.deleteFriendsUserIdsList(toUserId);
        // 好友列表
        friendsRedisRepository.deleteFriends(userId);
        friendsRedisRepository.deleteFriends(toUserId);
    }

    // 后台加入黑名单（后台可以互相拉黑）
    public Friends consoleAddBlacklist(Integer userId, Integer toUserId, Integer adminUserId) {
        // 是否存在AB关系
        Friends friendsAB = getFriendsDao().getFriends(userId, toUserId);
        Friends friendsBA = getFriendsDao().getFriends(toUserId, userId);
        if (null == friendsAB) {
            Friends friends = new Friends(userId, toUserId, getUserManager().getNickName(toUserId), Friends.Status.Stranger, Friends.Blacklist.Yes, 0);
            getFriendsDao().saveFriends(friends);
        } else {
            // 更新关系
            getFriendsDao().updateFriends(new Friends(userId, toUserId, null, -1, Friends.Blacklist.Yes, friendsAB.getIsBeenBlack()));
            getFriendsDao().updateFriends(new Friends(toUserId, userId, null, null, friendsBA.getBlacklist(), 1));
        }
        messageRepository.deleteLastMsg(userId.toString(), toUserId.toString());
        ThreadUtils.executeInThread((Callback) obj -> {
            //xmpp推送消息
            MessageBean messageBean = new MessageBean();
            messageBean.setType(MessageType.joinBlacklist);
            messageBean.setFromUserId(adminUserId + "");
            messageBean.setFromUserName("后台管理员");
           // messageBean.setFromUserName("System administrator");
            MessageBean beanVo = new MessageBean();
            beanVo.setFromUserId(userId + "");
            beanVo.setFromUserName(getUserManager().getNickName(userId));
            beanVo.setToUserId(toUserId + "");
            beanVo.setToUserName(getUserManager().getNickName(toUserId));
            messageBean.setObjectId(JSONObject.toJSONString(beanVo));
            messageBean.setMessageId(StringUtil.randomUUID());
            try {
                List<Integer> userIdlist = new ArrayList<Integer>();
                userIdlist.add(userId);
                userIdlist.add(toUserId);
                messageService.send(messageBean, userIdlist);
            } catch (Exception e) {
            }

        });
        // 维护好友数据
        deleteFriendsInfo(userId, toUserId);
        // 更新好友设置操作时间
        updateOfflineOperation(userId, toUserId);
        return getFriendsDao().getFriends(userId, toUserId);
    }

    public Friends updateFriends(Friends friends) {
        return getFriendsDao().updateFriends(friends);
    }

    public boolean isBlack(Integer toUserId) {
        Friends friends = getFriends(ReqUtil.getUserId(), toUserId);
        if (friends == null) {
            return false;
        }
        return friends.getBlacklist() == Friends.Blacklist.Yes;
    }

    public boolean isBlack(Integer userId, Integer toUserId) {
        Friends friends = getFriends(userId, toUserId);
        if (friends == null) {
            return false;
        }
        return friends.getBlacklist() == Friends.Blacklist.Yes;
    }

    private void saveFansCount(int userId) {
		/*BasicDBObject q = new BasicDBObject("_id", userId);
		DBCollection dbCollection = SKBeanUtils.getTigaseDatastore().getDB().getCollection("chat_msgs_count");
		if (0 == dbCollection.count(q)) {
			BasicDBObject jo = new BasicDBObject("_id", userId);
			jo.put("count", 0);// 消息数
			jo.put("fansCount", 1);// 粉丝数
			dbCollection.insert(jo);
		} else {
			dbCollection.update(q, new BasicDBObject("$inc", new BasicDBObject("fansCount", 1)));
		}*/
    }

    @Override
    public boolean addFriends(Integer userId, Integer toUserId) {

        User toUser= userCoreService.getUser(toUserId);
        if(null==toUser){
            return false;
        }
        int toUserType = toUser.getUserType();

        User user = userCoreService.getUser(userId);

        int userType = null==user?0:null==user.getUserType()?0:user.getUserType();

        List<Integer> toUserRoles = roleCoreService.getUserRoles(toUserId);
        List<Integer> userRoles = roleCoreService.getUserRoles(userId);
        Friends friends = getFriends(userId, toUserId);
        if (null == friends) {
            getFriendsDao().saveFriends(new Friends(userId, toUserId, getUserManager().getNickName(toUserId),
                    Friends.Status.Friends, 0, 0, toUserRoles, toUserType, 4));
            saveFansCount(toUserId);
        } else {
            saveFansCount(toUserId);


            Map<String, Object> map = new HashMap<>(6);
            map.put("modifyTime", DateUtil.currentTimeSeconds());
            map.put("status", Friends.Status.Friends);
            map.put("toUserType", toUserType);
            map.put("toFriendsRole", toUserRoles);
            if(null==friends.getRsaMsgPublicKey()){
                Document document = authKeysService.queryMsgAndDHPublicKey(friends.getToUserId());
                if(null!=document&&null!=document.getString("msgRsaKeyPair.publicKey")){
                    friends.setRsaMsgPublicKey(document.getString("msgRsaKeyPair.publicKey"));

                    friends.setDhMsgPublicKey(document.getString("dhMsgPublicKey.publicKey"));
                    map.put("msgRsaKeyPair.publicKey", friends.getRsaMsgPublicKey());
                    map.put("dhMsgPublicKey.publicKey", friends.getDhMsgPublicKey());
                }

            }

            friendsDao.updateFriends(userId, toUserId, map);
        }
        Friends toFriends = getFriends(toUserId, userId);
        if (null == toFriends) {
            getFriendsDao().saveFriends(new Friends(toUserId, userId, getUserManager().getNickName(userId),
                    Friends.Status.Friends, 0, 0, userRoles, userType, 4));
            saveFansCount(toUserId);
        } else {
            saveFansCount(toUserId);
            Map<String, Object> map = new HashMap<>(6);
            map.put("modifyTime", DateUtil.currentTimeSeconds());
            map.put("status", Friends.Status.Friends);
            map.put("toUserType", userType);
            map.put("toFriendsRole", userRoles);
            if(null==toFriends.getRsaMsgPublicKey()){
                Document document = authKeysService.queryMsgAndDHPublicKey(toFriends.getToUserId());
                if(null!=document&&null!=document.getString("msgRsaKeyPair.publicKey")){
                    friends.setRsaMsgPublicKey(document.getString("msgRsaKeyPair.publicKey"));

                    friends.setDhMsgPublicKey(document.getString("dhMsgPublicKey.publicKey"));
                    map.put("msgRsaKeyPair.publicKey", toFriends.getRsaMsgPublicKey());
                    map.put("dhMsgPublicKey.publicKey", toFriends.getDhMsgPublicKey());
                }

            }
            friendsDao.updateFriends(toUserId, userId, map);
        }
        // 更新好友设置操作时间
        updateOfflineOperation(userId, toUserId);
        // 维护好友数据
        deleteFriendsInfo(userId, toUserId);
        // 内部定制版本，严禁修改
        checkAddFriend(userId, toUserId, 2);
        return true;
    }



    @Override
    public boolean deleteFriends(Integer userId, Integer toUserId) {
        getFriendsDao().deleteFriends(userId, toUserId);
        getFriendsDao().deleteFriends(toUserId, userId);
        messageRepository.deleteLastMsg(userId.toString(), toUserId.toString());
        messageRepository.deleteLastMsg(toUserId.toString(), userId.toString());
        // 删除好友间消息记录
        messageRepository.delFriendsChatMsg(userId, toUserId);
        messageRepository.delFriendsChatMsg(toUserId, userId);
        // 维护通讯录好友
        addressBookManager.deleteAddressBook(userId, toUserId);
        addressBookManager.deleteAddressBook(toUserId, userId);
        // 维护好友标签数据
        friendGroupManager.deleteFriendToFriendGroup(userId, toUserId);
        // 维护好友数据
        deleteFriendsInfo(userId, toUserId);
        // 更新好友设置操作时间
        updateOfflineOperation(userId, toUserId);
        // 更新对方的好友设置操作事件
        updateOfflineOperation(toUserId, userId);
        return true;
    }

    /**
     * 后台删除好友-xmpp发通知
     **/
    public boolean consoleDeleteFriends(Integer userId, Integer adminUserId, String... toUserIds) {
        for (String strToUserId : toUserIds) {
            int toUserId = Integer.parseInt(strToUserId);

            getFriendsDao().deleteFriends(userId, toUserId);
            getFriendsDao().deleteFriends(toUserId, userId);

            messageRepository.deleteLastMsg(userId.toString(), Integer.toString(toUserId));
            messageRepository.deleteLastMsg(Integer.toString(toUserId), userId.toString());

            ThreadUtils.executeInThread((Callback) obj -> {
                //以系统号发送删除好友通知
                MessageBean messageBean = new MessageBean();
                messageBean.setType(MessageType.deleteFriends);
                messageBean.setFromUserId(adminUserId + StrUtil.EMPTY);
                messageBean.setFromUserName("后台管理员");
               // messageBean.setFromUserName("System administrator");
                MessageBean beanVo = new MessageBean();
                beanVo.setFromUserId(userId + "");
                beanVo.setFromUserName(getUserManager().getNickName(userId));
                beanVo.setToUserId(toUserId + "");
                beanVo.setToUserName(getUserManager().getNickName(toUserId));
                messageBean.setObjectId(JSONObject.toJSONString(beanVo));
                messageBean.setMessageId(StringUtil.randomUUID());
                messageBean.setContent("管理员已解除双方好友关系");
                messageBean.setMessageId(StringUtil.randomUUID());
                try {
                    List<Integer> userIdList = new ArrayList<>();
                    userIdList.add(userId);
                    userIdList.add(toUserId);
                    messageService.send(messageBean, userIdList);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                // 维护好友缓存
                deleteFriendsInfo(userId, toUserId);
            });
        }
        return true;
    }


    @Override
    public JSONMessage followUser(Integer userId, Integer toUserId, Integer fromAddType) {
        final String serviceCode = "08";
        JSONMessage jMessage = null;
        User toUser = getUserManager().getUser(toUserId);
        int toUserType = 0;
        List<Integer> toUserRoles = roleCoreService.getUserRoles(toUserId);
        if (toUserRoles.size() > 0) {
            if (toUserRoles.contains(2)) {
                toUserType = 2;
            }
        }
        //好友不存在
        if (null == toUser) {
            if (10000 == toUserId) {
                return null;
            } else {
                return JSONMessage.failureByErrCode(ResultCode.UserNotExist);
            }
        }
        int resultType = 0;

        try {
            User user = getUserManager().getUser(userId);
            int userType = 0;
            List<Integer> userRoles = roleCoreService.getUserRoles(userId);
            if (null != userRoles && userRoles.size() > 0) {
                if (userRoles.contains(2)) {
                    userType = 2;
                }
            }

            // 是否存在AB关系
            Friends friendsAB = getFriendsDao().getFriends(userId, toUserId);
            // 是否存在BA关系
            Friends friendsBA = getFriendsDao().getFriends(toUserId, userId);
            // 获取目标用户设置
            User.UserSettings userSettingsB = getUserManager().getSettings(toUserId);

            // ----------------------------
            // 0 0 0 0 无记录 执行关注逻辑
            // A B 1 0 非正常 执行关注逻辑
            // A B 1 1 拉黑陌生人 执行关注逻辑
            // A B 2 0 关注 重复关注
            // A B 3 0 好友 重复关注
            // A B 2 1 拉黑关注 恢复关系
            // A B 3 1 拉黑好友 恢复关系
            // ----------------------------
            // 无AB关系或陌生人黑名单关系，加关注
            if (null != friendsAB && friendsAB.getIsBeenBlack() == 1) {
                return JSONMessage.failureByErrCode(ResultCode.AddFriendsFailure);
            }
            if (null == friendsAB || Friends.Status.Stranger == friendsAB.getStatus()) {
                // 目标用户拒绝关注
                if (0 == userSettingsB.getAllowAtt()) {
                    jMessage = new JSONMessage(groupCode, serviceCode, "01", "关注失败，目标用户拒绝关注");
                } else {
                    // 目标用户允许关注
                    int statusA;
                    // 目标用户加好需验证，执行加关注。过滤公众号开启好友验证
                    if (1 == userSettingsB.getFriendsVerify() && 2 != toUserType) {
                        // ----------------------------
                        // 0 0 0 0 无记录 执行单向关注
                        // B A 1 0 非正常 执行单向关注
                        // B A 1 1 拉黑陌生人 执行单向关注
                        // B A 2 0 关注 加好友
                        // B A 3 0 好友 加好友
                        // B A 2 1 拉黑关注 加好友
                        // B A 3 1 拉黑好友 加好友
                        // ----------------------------
                        // 无BA关系或陌生人黑名单关系，单向关注
                        if (null == friendsBA || Friends.Status.Stranger == friendsBA.getStatus()) {
                            statusA = Friends.Status.Attention;
                        } else {
                            statusA = Friends.Status.Friends;
                            getFriendsDao().updateFriends(new Friends(toUserId, user.getUserId(), user.getNickname(), Friends.Status.Friends));
                        }
                    } else {
                        // 目标用户加好友无需验证，执行加好友
                        statusA = Friends.Status.Friends;
                        if (null == friendsBA) {
                            getFriendsDao().saveFriends(new Friends(toUserId, user.getUserId(), user.getNickname(), Friends.Status.Friends, Friends.Blacklist.No, 0, userRoles, userType, fromAddType));
                            saveFansCount(toUserId);
                        } else {
                            getFriendsDao().updateFriends(new Friends(toUserId, user.getUserId(), user.getNickname(), Friends.Status.Friends, userType, userRoles));//改变usertype
                        }
                    }
                    if (null == friendsAB) {
                        getFriendsDao().saveFriends(new Friends(userId, toUserId, toUser.getNickname(), statusA, Friends.Blacklist.No, 0, toUserRoles, toUserType, fromAddType));
                        saveFansCount(toUserId);
                    } else {
                        getFriendsDao().updateFriends(new Friends(userId, toUserId, toUser.getNickname(), statusA, Friends.Blacklist.No, 0));
                    }

                    if (statusA == Friends.Status.Attention) {
                        HashMap<String, Object> newMap = MapUtil.newMap("type", 1);
                        newMap.put("fromAddType", fromAddType);
                        resultType = 1;
                        jMessage = JSONMessage.success(ResultCode.AttentionSuccess, newMap);
                    } else {
                        HashMap<String, Object> newMap = MapUtil.newMap("type", 2);
                        newMap.put("fromAddType", fromAddType);
                        resultType = 2;
                        jMessage = JSONMessage.success(ResultCode.AttentionSuccessAndFriends, newMap);
                    }

                }
            } else if (Friends.Blacklist.No == friendsAB.getBlacklist()) {   // 有关注或好友关系，重复关注
                if (Friends.Status.Attention == friendsAB.getStatus()) {
                    // 开启好友验证后关闭
                    if (0 == userSettingsB.getFriendsVerify()) {
                        Integer statusA = Friends.Status.Friends;
                        if (null == friendsBA) {
                            getFriendsDao().saveFriends(new Friends(toUserId, user.getUserId(), user.getNickname(), Friends.Status.Friends, Friends.Blacklist.No, 0, userRoles, userType, fromAddType));
                            saveFansCount(toUserId);
                        } else {
                            getFriendsDao().updateFriends(new Friends(toUserId, user.getUserId(), user.getNickname(), Friends.Status.Friends));
                        }
                        getFriendsDao().updateFriends(new Friends(userId, toUserId, toUser.getNickname(), statusA, Friends.Blacklist.No, 0));
                        HashMap<String, Object> newMap = MapUtil.newMap("type", 2);
                        newMap.put("fromAddType", fromAddType);
                        resultType = 2;
                        jMessage = JSONMessage.success(ResultCode.AttentionSuccessAndFriends, newMap);
                    } else if (1 == userSettingsB.getFriendsVerify()) {
                        HashMap<String, Object> newMap = MapUtil.newMap("type", 1);
                        newMap.put("fromAddType", fromAddType);
                        resultType = 1;
                        jMessage = JSONMessage.success(ResultCode.AttentionSuccess, newMap);
                    }
                } else {
                    HashMap<String, Object> newMap = MapUtil.newMap("type", 2);
                    newMap.put("fromAddType", fromAddType);
                    resultType = 2;
                    jMessage = JSONMessage.success(ResultCode.AttentionSuccessAndFriends, newMap);
                }
            } else {
                // 有关注黑名单或好友黑名单关系，恢复关系
                getFriendsDao().updateFriends(new Friends(userId, toUserId, toUser.getNickname(), Friends.Blacklist.No));
                jMessage = null;
            }
            // 维护好友数据
            deleteFriendsInfo(userId, toUserId);
            // 更新好友设置操作时间
            updateOfflineOperation(userId, toUserId);

            // 内部定制版本，严禁修改
            checkAddFriend(userId, toUserId, resultType);

        } catch (Exception e) {
            Log.error("关注失败", e);
            throw e;
        }
        return jMessage;
    }

    /**
     * 更新好友设置操作时间
     **/
    public void updateOfflineOperation(Integer userId, Integer toUserId) {
        OfflineOperation offlineOperation = offlineOperationDao.queryOfflineOperation(userId, MultipointSyncUtil.MultipointLogin.TAG_FRIEND, String.valueOf(toUserId));
        if (null == offlineOperation) {
            offlineOperationDao.addOfflineOperation(userId, MultipointSyncUtil.MultipointLogin.TAG_FRIEND, String.valueOf(toUserId), DateUtil.currentTimeSeconds());
        } else {
            Map<String, Object> map = new HashMap<>(1);
            map.put("operationTime", DateUtil.currentTimeSeconds());
            offlineOperationDao.updateOfflineOperation(userId, toUserId.toString(), map);
        }
    }

    // 批量添加好友
    @Override
    public JSONMessage batchFollowUser(Integer userId, String toUserIds) {
        JSONMessage jMessage = null;
        if (StringUtil.isEmpty(toUserIds)) {
            return null;
        }
        int[] toUserId = StringUtil.getIntArray(toUserIds, ",");
        for (int value : toUserId) {
            //好友不存在
            if (userId == value || 10000 == value) {
                continue;
            }
            User toUser = getUserManager().getUser(value);
            if (null == toUser) {
                continue;
            }
            int toUserType = 0;
            List<Integer> toUserRoles = roleCoreService.getUserRoles(value);
            if (toUserRoles.size() > 0) {
                if (toUserRoles.contains(2)) {
                    toUserType = 2;
                }
            }

            try {
                User user = getUserManager().getUser(userId);
                int userType = 0;
                List<Integer> userRoles = roleCoreService.getUserRoles(userId);
                if (userRoles.size() > 0) {
                    if (userRoles.contains(2)) {
                        userType = 2;
                    }
                }

                // 是否存在AB关系
                Friends friendsAB = getFriendsDao().getFriends(userId, value);
                // 是否存在BA关系
                Friends friendsBA = getFriendsDao().getFriends(value, userId);
                // 获取目标用户设置
                User.UserSettings userSettingsB = getUserManager().getSettings(value);

                if (null != friendsAB && friendsAB.getIsBeenBlack() == 1) {
                    continue;
                    // throw new ServiceException(KConstants.ResultCode.WasAddBlacklist);
                }
                if (null == friendsAB || Friends.Status.Stranger == friendsAB.getStatus()) {
                    // 目标用户拒绝关注
                    if (0 == userSettingsB.getAllowAtt()) {
                        continue;
                    } else {// 目标用户允许关注
                        int statusA = Friends.Status.Friends;
                        if (null == friendsBA) {
                            getFriendsDao().saveFriends(new Friends(value, user.getUserId(), user.getNickname(),
                                    Friends.Status.Friends, Friends.Blacklist.No, 0, userRoles, userType, 4));
                            saveFansCount(value);
                        } else {
                            getFriendsDao()
                                    .updateFriends(new Friends(value, user.getUserId(), user.getNickname(), Friends.Status.Friends));
                        }
                        if (null == friendsAB) {
                            getFriendsDao().saveFriends(new Friends(userId, value, toUser.getNickname(), statusA, Friends.Blacklist.No, 0, toUserRoles, toUserType, 4));
                            saveFansCount(value);
                        } else {
                            getFriendsDao().updateFriends(new Friends(userId, value, toUser.getNickname(), statusA, Friends.Blacklist.No, 0));
                        }
                    }
                } else if (Friends.Blacklist.No == friendsAB.getBlacklist()) { // 有关注或好友关系，重复关注
                    if (Friends.Status.Attention == friendsAB.getStatus()) {
                        // 已关注的修改为好友状态
                        getFriendsDao().updateFriends(new Friends(userId, value, toUser.getNickname(), Friends.Status.Friends, toUserType, toUserRoles));
                        // 添加成为好友
                        getFriendsDao().saveFriends(new Friends(value, user.getUserId(), user.getNickname(),
                                Friends.Status.Friends, Friends.Blacklist.No, 0, userRoles, "", userType));
                    }
                } else {
                    // 有关注黑名单或好友黑名单关系，恢复关系
                    getFriendsDao().updateFriends(new Friends(userId, value, toUser.getNickname(), Friends.Blacklist.No));
                }
                notify(userId, value);
                jMessage = JSONMessage.success();
                // 维护好友数据
                deleteAddressFriendsInfo(userId, value);
                // 更新好友设置操作时间
                updateOfflineOperation(userId, value);
            } catch (Exception e) {
                Log.error("通讯录添加好友失败,{}", e.getMessage());
                throw e;
            }
        }
        return jMessage;
    }


    /**
     * 通讯录自动添加好友
     **/
    public JSONMessage autofollowUser(Integer userId, Map<String, String> addressBook) {
        int toUserId = Integer.parseInt(addressBook.get("toUserId"));
        String toRemark = addressBook.get("toRemark");
        JSONMessage jMessage;
        User toUser = getUserManager().getUser(toUserId);
        int toUserType = 0;
        List<Integer> toUserRoles = roleCoreService.getUserRoles(toUserId);
        if (toUserRoles.size() > 0) {
            if (toUserRoles.contains(2)) {
                toUserType = 2;
            }
        }
        //好友不存在
        if (10000 == toUser.getUserId()) {
            return null;
        }
        try {
            User user = getUserManager().getUser(userId);
            int userType = 0;
            List<Integer> userRoles = roleCoreService.getUserRoles(userId);
            if (userRoles.size() > 0) {
                if (userRoles.contains(2)) {
                    userType = 2;
                }
            }

            // 是否存在AB关系
            Friends friendsAB = getFriendsDao().getFriends(userId, toUserId);
            // 是否存在BA关系
            Friends friendsBA = getFriendsDao().getFriends(toUserId, userId);
            // 获取目标用户设置
            User.UserSettings userSettingsB = getUserManager().getSettings(toUserId);

            if (null != friendsAB && friendsAB.getIsBeenBlack() == 1) {
                return JSONMessage.failureByErrCode(ResultCode.AddFriendsFailure);
            }
            if (null == friendsAB || Friends.Status.Stranger == friendsAB.getStatus()) {
                // 目标用户拒绝关注
                if (0 == userSettingsB.getAllowAtt()) {
                    return JSONMessage.failureByErrCode(ResultCode.AttentionFailure);
                }
                // 目标用户允许关注
                else {
                    int statusA;
                    // 目标用户加好友无需验证，执行加好友
//						else {
                    statusA = Friends.Status.Friends;

                    if (null == friendsBA) {
                        getFriendsDao().saveFriends(new Friends(toUserId, user.getUserId(), user.getNickname(),
                                Friends.Status.Friends, Friends.Blacklist.No, 0, userRoles, "", userType));

                        saveFansCount(toUserId);
                    } else {
                        getFriendsDao()
                                .updateFriends(new Friends(toUserId, user.getUserId(), user.getNickname(), Friends.Status.Friends));
                    }
//						}

                    if (null == friendsAB) {
                        getFriendsDao().saveFriends(new Friends(userId, toUserId, toUser.getNickname(), statusA, Friends.Blacklist.No, 0, toUserRoles, toRemark, toUserType));
                        saveFansCount(toUserId);
                    } else {
                        getFriendsDao().updateFriends(new Friends(userId, toUserId, toUser.getNickname(), statusA, Friends.Blacklist.No, 0));
                    }

                }
            }
            // 有关注或好友关系，重复关注
            else if (Friends.Blacklist.No == friendsAB.getBlacklist()) {
                if (Friends.Status.Attention == friendsAB.getStatus()) {
                    // 已关注的修改为好友状态
                    getFriendsDao().updateFriends(new Friends(userId, toUserId, toUser.getNickname(), Friends.Status.Friends, toUserType, toUserRoles));
                    // 添加成为好友
                    getFriendsDao().saveFriends(new Friends(toUserId, user.getUserId(), user.getNickname(),
                            Friends.Status.Friends, Friends.Blacklist.No, 0, userRoles, "", userType));
                }
            } else {
                // 有关注黑名单或好友黑名单关系，恢复关系
                getFriendsDao().updateFriends(new Friends(userId, toUserId, toUser.getNickname(), Friends.Blacklist.No));
            }
            notify(userId, toUserId);
            // 维护好友数据
            deleteFriendsInfo(userId, toUserId);
            jMessage = JSONMessage.success();
        } catch (Exception e) {
            Log.error("关注失败", e);
            jMessage = JSONMessage.failureByErrCode(ResultCode.AttentionFailure);
        }
        return jMessage;
    }

    public void notify(Integer userId, Integer toUserId) {
        ThreadUtils.executeInThread((Callback) obj -> {
            MessageBean messageBean = new MessageBean();
            messageBean.setType(MessageType.batchAddFriend);
            messageBean.setFromUserId(String.valueOf(userId));
            messageBean.setFromUserName(getUserManager().getNickName(userId));
            messageBean.setToUserId(String.valueOf(toUserId));
            messageBean.setToUserName(getUserManager().getNickName(toUserId));
            messageBean.setContent(toUserId);
            messageBean.setMsgType(0);// 单聊消息
            messageBean.setMessageId(StringUtil.randomUUID());
            try {
                messageService.send(messageBean);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public Friends getFriends(int userId, int toUserId) {
        return getFriendsDao().getFriends(userId, toUserId);
    }

    public void getFriends(int userId, String... toUserIds) {
        for (String strToUserId : toUserIds) {
            int toUserId = Integer.parseInt(strToUserId);
            Friends friends = getFriendsDao().getFriends(userId, toUserId);
            if (null == friends) {
                throw new ServiceException(ResultCode.NotYourFriends);
            }
        }
    }

    public List<Friends> getFansList(Integer userId) {
        return getFriendsDao().queryAllFriends(userId);
    }


    @Override
    public Friends getFriends(Friends p) {
        return getFriendsDao().getFriends(p.getUserId(), p.getToUserId());
    }

    @Override
    public List<Integer> queryFriendUserIdList(int userId) {
        List<Integer> result;
        try {
            result = friendsRedisRepository.getFriendsUserIdsList(userId);
            if (null != result && result.size() > 0) {

                return result;
            } else {
                result = friendsDao.queryFriendUserIdList(userId);

                friendsRedisRepository.saveFriendsUserIdsList(userId, result);
            }
            return result;
        } catch (Exception e) {
            Log.error(e.getMessage(), e);
            throw e;
        }
    }


    @Override
    public List<Friends> queryBlacklist(Integer userId, int pageIndex, int pageSize) {
        return getFriendsDao().queryBlacklist(userId, pageIndex, pageSize);
    }

    public PageVO queryBlacklistWeb(Integer userId, int pageIndex, int pageSize) {
        List<Friends> data = getFriendsDao().queryBlacklistWeb(userId, pageIndex, pageSize);
        return new PageVO(data, (long) data.size(), pageIndex, pageSize);
    }


    /**
     * 查询好友是否开启 免打扰
     */
    @Override
    public boolean getFriendIsNoPushMsg(int userId, int toUserId) {
        return getFriendsDao().getFriendIsNoPushMsg(userId, toUserId);
    }

    @Override
    public List<Friends> queryFollow(Integer userId, int status) {
        List<Friends> userfriends = friendsRedisRepository.getFriendsList(userId);
        if (null == userfriends || userfriends.size() == 0) {
            if (0 == status) {
                status = 2;  //好友
            }
            userfriends = getFriendsDao().queryFriendsList(userId, status, null,null,null,0, 0,true);
            //userfriends = getFriendsDao().queryFriendsList(userId, status, 0, 0);
            /*Document document=null;
            for (Friends friends : userfriends) {
                if(!StrUtil.isEmpty(friends.getDhMsgPublicKey())){
                    continue;
                }
                document=authKeysService.queryMsgAndDHPublicKey(friends.getToUserId());
                if(null==document){
                    continue;
                }
                log.info(document.toJson());
            }*/
            if (10000 < userfriends.size()) {
                List<Friends> pageFriends;
                int pageNo = 0;
                int pageSize = 5000;
                while (true) {
                    pageFriends = ListUtil.page(pageNo, pageSize, userfriends);
                    pageNo++;
                    if (0 == pageFriends.size()) {
                        break;
                    }
                    friendsRedisRepository.addFriendsList(userId, pageFriends);
                    if (pageSize > pageFriends.size()) {
                        break;
                    }
                }
            } else {
                friendsRedisRepository.saveFriendsList(userId, userfriends);
            }
        }


        // 判断是否需要更新通讯号，兼容旧版本好友关系不具备通讯号的情况

        /*if (CollectionUtil.isNotEmpty(userfriends)){
            if (StrUtil.isBlank(userfriends.get(0).getAccount())
                    || userfriends.stream().allMatch(obj -> StrUtil.isNotBlank(obj.getAccount()))){
                userfriends.forEach(obj->{
                    User user = userCoreService.getUser(obj.getToUserId());
                    if (ObjectUtil.isNotNull(user)){
                        obj.setAccount(user.getAccount());
                        friendsDao.updateFriendsAttribute(obj.getUserId(),obj.getToUserId(),"account",user.getAccount());
                    }
                    //this.markerConsumer(obj);
                });
            }
        }*/
        /*else{
            userfriends.forEach(this::markerConsumer);
        }*/
        return userfriends;
    }

    /**
     * 修改水印内容
     */
    private void markerConsumer(Friends friends){
        String showMarker = friends.getShowMarker();
        String toShowMarker = friends.getToShowMarker();
        friends.setToShowMarker(showMarker == null ? StrUtil.EMPTY : showMarker);
        friends.setShowMarker(toShowMarker == null ? StrUtil.EMPTY : toShowMarker);
    }

    @Override
    public List<Friends> queryFollow(Integer userId, int status, int userType, List<Integer> notInUserIds,String keyword,int pageIndex, int pageSize) {
        List<Friends> friends = getFriendsDao().queryFriendsList(userId, status, userType,notInUserIds,keyword,pageIndex, pageSize,true);
        if (CollectionUtil.isNotEmpty(friends) && friendsRedisRepository.getOrderKeyAllUpdateFlag(userId)){
            sendFriendsNameChangeTopic(userId,null);
        }
        return friends;
    }


    public PageResult<Friends> consoleQueryFollow(Integer userId, Integer toUserId, int status, int page, int limit) {
        PageResult<Friends> result;
        result = getFriendsDao().consoleQueryFollow(userId, toUserId, status, page, limit);
        return result;
    }


    @Override
    public List<Integer> queryFollowId(Integer userId) {
        return getFriendsDao().queryFollowId(userId);
    }

    @Override
    public List<Friends> queryFriends(Integer userId) {
        return getFriendsDao().queryFriends(userId);
    }


    /**
     * 返回好友的userId 和单向关注的userId
     */
    @Override
    public List<Integer> friendsAndAttentionUserId(Integer userId, String type) {
        if ("friendList".equals(type) || "blackList".equals(type)) {  //返回好友的userId 和单向关注的userId
            return getFriendsDao().friendsOrBlackUserIdList(userId, type);
        } else {
            throw new ServiceException(ResultCode.ParamsAuthFail);
        }

    }

    @Override
    public PageResult<Friends> queryFriends(Integer userId, int status, String keyword, int pageIndex, int pageSize) {
        PageResult<Friends> pageData = friendsDao.queryFollowByKeyWord(userId, status, keyword, pageIndex, pageSize);

        return pageData;
    }

    public List<Friends> queryFriendsList(Integer userId, int status, int pageIndex, int pageSize) {
        return friendsDao.queryFriendsList(userId, status, pageIndex, pageSize);
    }


    /**
     * 取消关注
     */
    @Override
    public boolean unfollowUser(Integer userId, Integer toUserId) {
        // 删除用户关注
        getFriendsDao().deleteFriends(userId, toUserId);
        // 更新好友设置操作时间
        updateOfflineOperation(userId, toUserId);
        return true;
    }

    @Override
    public Friends updateRemark(int userId, int toUserId, String remarkName, String describe) {
        return getFriendsDao().updateFriendRemarkName(userId, toUserId, remarkName, describe);
    }


    @Override
    public void deleteFansAndFriends(int userId) {
        getFriendsDao().deleteFriends(userId);
    }

    @Override
    public List<NewFriends> newFriendList(int userId, int pageIndex, int pageSize) {

        List<NewFriends> pageData = friendsDao.getNewFriendsList(userId, pageIndex, pageSize);
        Friends friends;
        for (NewFriends newFriends : pageData) {
            friends = getFriends(newFriends.getUserId(), newFriends.getToUserId());
            newFriends.setToNickname(getUserManager().getNickName(newFriends.getToUserId()));

            if (null != friends) {
                newFriends.setStatus(friends.getStatus());
            }
        }
        return pageData;

    }

    public PageVO newFriendListWeb(int userId, int pageIndex, int pageSize) {
        List<NewFriends> pageData = friendsDao.getNewFriendsList(userId, pageIndex, pageSize);
        Friends friends;
        for (NewFriends newFriends : pageData) {
            friends = getFriends(newFriends.getUserId(), newFriends.getToUserId());
            newFriends.setToNickname(getUserManager().getNickName(newFriends.getToUserId()));
            if (null != friends) {
                newFriends.setStatus(friends.getStatus());
            }
        }
        return new PageVO(pageData, (long) pageData.size(), pageIndex, pageSize);
    }

    public List<NewFriends> lastNewFriendListWeb(int userId) {
        List<NewFriends> result = friendsDao.getLastNewFriendsList(userId);
        Friends friends = null;
        for (NewFriends newFriends : result) {
            friends = getFriends(newFriends.getUserId(), newFriends.getToUserId());
            newFriends.setToNickname(getUserManager().getNickName(newFriends.getToUserId()));
            if (null != friends) {
                newFriends.setStatus(friends.getStatus());
            }
        }
        return result;
    }


    public NewFriends newFriendLast(int userId, int toUserId) {
        NewFriends newFriend = friendsDao.getNewFriendLast(userId, toUserId);
        newFriend.setToNickname(getUserManager().getNickName(newFriend.getToUserId()));
        return newFriend;
    }

    /* 消息免打扰、阅后即焚、聊天置顶相关修改
     * type = 0  消息免打扰 ,type = 1  阅后即焚 ,type = 2  聊天置顶
     */
    @Override
    public Friends updateOfflineNoPushMsg(int userId, int toUserId, int offlineNoPushMsg, int type) {
        Map<String, Object> map = new HashMap<>(4);
        switch (type) {
            case 0:
                map.put("offlineNoPushMsg", offlineNoPushMsg);
                break;
            case 1:
                map.put("isOpenSnapchat", offlineNoPushMsg);
                break;
            case 2:
                map.put("openTopChatTime", (offlineNoPushMsg == 0 ? 0 : DateUtil.currentTimeSeconds()));
                break;
            default:
                break;
        }
        // 多点登录下消息免打扰xmpp通知
        if (getUserManager().isOpenMultipleDevices(userId)) {
            MultipointSyncUtil.multipointLoginUpdateUserInfo(userId, getUserManager().getNickName(userId), toUserId, getUserManager().getNickName(toUserId), 1);
        }
        friendsRedisRepository.deleteFriends(userId);
        return friendsDao.updateFriendsReturn(userId, toUserId, map);
    }


    /**
     * 添加好友统计      时间单位每日，最好可选择：每日、每月、每分钟、每小时
     */
    public List<Object> getAddFriendsCount(String startDate, String endDate, short timeUnit) {

        List<Object> countData;

        long startTime; //开始时间（秒）

        long endTime; //结束时间（秒）,默认为当前时间

        /**
         * 如时间单位为月和天，默认开始时间为当前时间的一年前 ; 时间单位为小时，默认开始时间为当前时间的一个月前;
         * 时间单位为分钟，则默认开始时间为当前这一天的0点
         */
        long defStartTime = timeUnit == 4 ? DateUtil.getTodayMorning().getTime() / 1000
                : timeUnit == 3 ? DateUtil.getLastMonth().getTime() / 1000 : DateUtil.getLastYear().getTime() / 1000;

        startTime = StringUtil.isEmpty(startDate) ? defStartTime : DateUtil.toDate(startDate).getTime() / 1000;
        endTime = StringUtil.isEmpty(endDate) ? DateUtil.currentTimeSeconds() : DateUtil.toDate(endDate).getTime() / 1000;


        String mapStr = "function Map() { "
                + "var date = new Date(this.createTime*1000);"
                + "var year = date.getFullYear();"
                + "var month = (\"0\" + (date.getMonth()+1)).slice(-2);"  //month 从0开始，此处要加1
                + "var day = (\"0\" + date.getDate()).slice(-2);"
                + "var hour = (\"0\" + date.getHours()).slice(-2);"
                + "var minute = (\"0\" + date.getMinutes()).slice(-2);"
                + "var dateStr = date.getFullYear()" + "+'-'+" + "(parseInt(date.getMonth())+1)" + "+'-'+" + "date.getDate();";

        if (timeUnit == 1) { // counType=1: 每个月的数据
            mapStr += "var key= year + '-'+ month;";
        } else if (timeUnit == 2) { // counType=2:每天的数据
            mapStr += "var key= year + '-'+ month + '-' + day;";
        } else if (timeUnit == 3) { //counType=3 :每小时数据
            mapStr += "var key= year + '-'+ month + '-' + day + '  ' + hour +' : 00';";
        } else if (timeUnit == 4) { //counType=4 :每分钟的数据
            mapStr += "var key= year + '-'+ month + '-' + day + '  ' + hour + ':'+ minute;";
        }

        mapStr += "emit(key,1);}";

        String reduce = "function Reduce(key, values) {" +
                "return Array.sum(values);" +
                "}";
        countData = friendsDao.getAddFriendsCount(startTime, endTime, mapStr, reduce);
        return countData;
    }

    // 好友之间的聊天记录
    public PageResult<Document> chardRecord(Integer sender, Integer receiver, Integer page, Integer limit) {
        return messageRepository.queryFirendMsgRecord(sender, receiver, page, limit);
    }

    /**
     * 删除好友间的聊天记录
     **/
    public void delFriendsChatRecord(String... messageIds) {
        messageRepository.delFriendsChatRecord(messageIds);
    }


    /**
     * 校验是否为好友或通讯录好友
     *
     * @param type -1 所有人不显示 1所有人显示  2 所有好友显示   3 手机联系人显示
     **/
    public boolean isAddressBookOrFriends(Integer userId, Integer toUserId, int type) {
        boolean flag = false;
        switch (type) {
            case -1:
                break;
            case 1:
                flag = true;
                break;
            case 2:
                List<Integer> friendsUserIdsList = queryFriendUserIdList(userId);
                if (null != friendsUserIdsList && friendsUserIdsList.size() > 0) {
                    flag = friendsUserIdsList.contains(toUserId);
                }

                break;
            case 3:
                List<Integer> addressBookUserIdsList;
                List<Integer> allAddressBookUserIdsList = friendsRedisRepository.getAddressBookFriendsUserIds(userId);
                if (null != allAddressBookUserIdsList && allAddressBookUserIdsList.size() > 0) {
                    addressBookUserIdsList = allAddressBookUserIdsList;
                } else {
                    addressBookUserIdsList = addressBookManager.getAddressBookUserIds(userId);
                    friendsRedisRepository.saveAddressBookFriendsUserIds(userId, addressBookUserIdsList);
                }
                flag = addressBookUserIdsList.contains(toUserId);
                break;
            default:
                break;
        }
        return flag;
    }

    /**
     * 修改和某个好友的消息加密方式
     */
    public void modifyEncryptType(int userId, int toUserId, byte type) {
        //修改自己和好友的加密方式
        friendsDao.updateFriendsEncryptType(userId, toUserId, type);
        //修改好友和自己的加密方式
        friendsDao.updateFriendsEncryptType(toUserId,userId,type);

        friendsRedisRepository.deleteFriends(userId);
        friendsRedisRepository.deleteFriends(toUserId);

        MessageBean messageBean = new MessageBean();
        messageBean.setContent(type);
        messageBean.setFromUserId(userId + "");
        messageBean.setFromUserName(userCoreService.getNickName(userId));
        messageBean.setMessageId(UUID.randomUUID().toString());
        messageBean.setMsgType(0);// 单聊消息
        messageBean.setType(MessageType.updateFriendEncryptType);
        messageBean.setTo(toUserId+"");
        messageBean.setToUserId(toUserId+"");
        messageService.send(messageBean);

        if (getUserManager().isOpenMultipleDevices(userId)) {
            MultipointSyncUtil.multipointLoginUpdateUserInfo(userId, getUserManager().getNickName(userId), toUserId, getUserManager().getNickName(toUserId), 1);
        }
    }

    /**
     * 修改和某个好友的手机号备注
     */
    public void updatePhoneRemark(int userId, int toUserId, String phoneRemark) {
        friendsDao.updatePhoneRemark(userId, toUserId, phoneRemark);
        friendsRedisRepository.deleteFriends(userId);
    }

    public void sendUpdatePublicKeyMsgToFriends(String dhPublicKey, String rsaPublicKey, int userId) {
        ThreadUtils.executeInThread(call -> {
            try {
                List<Integer> friendIds = queryFriendUserIdList(userId);
                // 删除好友的好友缓存
                friendIds.forEach(this::deleteRedisUserFriends);
                MessageBean mb = new MessageBean();
                mb.setContent(dhPublicKey + "," + rsaPublicKey);
                mb.setFromUserId(userId + "");
                mb.setMessageId(UUID.randomUUID().toString());
                mb.setMsgType(0);// 单聊消息
                mb.setType(MessageType.updateFriendsEncryptKey);
                messageService.send(mb, friendIds);
            } catch (Exception e) {
                Log.error(e.getMessage(), e);
            }
        });

    }

    public void deleteRedisUserFriends(int userId) {
        friendsRedisRepository.deleteFriends(userId);
    }

    public void deleteFriendsCache(Integer userId) {
        // 好友名称(维护自己好友列表的数据)
        List<Integer> toUserIds = friendsDao.queryFriendUserIdList(userId);
        toUserIds.forEach(this::deleteRedisUserFriends);
    }

    @EventListener
    public void handlerUserChangeNameEvent(UserChageNameEvent event) {
        friendsDao.updateFriendsAttribute(0, event.getUserId(), "toNickname", event.getNickName()); //修改好友昵称
        // 修改好友昵称排序 Key
        List<Friends> friends = friendsDao.queryFriendsByToUserId(event.getUserId());
        if (CollectionUtil.isNotEmpty(friends)){
            friends.forEach(friend-> sendFriendsNameChangeTopic(friend.getUserId(),friend.getToUserId()));
        }
    }

    @EventListener
    public void handlerKeyPairChageEvent(KeyPairChageEvent event) {
        friendsDao.updateMsgKeyPair(event); // 更新好友密钥对
        deleteFriendsCache(event.getUserId());
    }


    @Override
    public void setHideChatSwitch(int userId, int toUserId, byte hideChatSwitch) {
        friendsDao.setHideChatSwitch(userId, toUserId, hideChatSwitch);
        friendsRedisRepository.deleteFriends(userId);
        userCoreRedis.deleteUserByUserId(userId);
        // 数据同步
        MultipointSyncUtil.multipointLoginDataSync(userId, MultipointSyncUtil.MultipointLogin.HIDE_CHAT_SWITCH);
    }


    /**
     * 建立邀请关系
     */
    public void addInvite(Integer userId, Integer inviteUserId) {
        userHandler.addFriendsHandler(userId, inviteUserId);
    }

    public void updateHiding(int userId, int hiding) {
        friendsDao.updateHiding(userId, hiding);
    }


    public void checkAddFriend(int userId, int toUserId, int type) {
        if (SKBeanUtils.getImCoreService().getAppConfig().getServiceNoList().isEmpty()) {
            return;
        }
        if (SKBeanUtils.getImCoreService().getAppConfig().getServiceNoList().contains(userId)) {
            User user = userCoreService.getUserByDB((SKBeanUtils.getImCoreService().getAppConfig().getPhone()));
            if (null != user) {
                sendAddFriendNotice(userId, userCoreService.getNickName(userId),
                        toUserId, userCoreService.getNickName(toUserId), user.getUserId(), type);
            }
        } else if (SKBeanUtils.getImCoreService().getAppConfig().getServiceNoList().contains(toUserId)) {
            User user = userCoreService.getUserByDB(SKBeanUtils.getImCoreService().getAppConfig().getPhone());
            if (null != user) {
                sendAddFriendNotice(userId, userCoreService.getNickName(userId), toUserId, userCoreService.getNickName(toUserId), user.getUserId(), type);
            }
        }
    }

    public void sendAddFriendNotice(int userId, String userName, int toUserId, String toUserName, int noticeUserId, int type) {
        MessageBean messageBean = new MessageBean();
        if (2 == type) {
            messageBean.setContent(String.format("加好友成功通知: %s [%s] ====> %s [%s] \n 时间: %s",
                    userId, userName, toUserId, toUserName, DateUtil.getFullString()));
        } else {
            messageBean.setContent(String.format("申请加好友通知: %s [%s] ====> %s [%s] \n 时间: %s",
                    userId, userName, toUserId, toUserName, DateUtil.getFullString()));
        }
        messageBean.setFromUserId(10000 + "");
        messageBean.setToUserId(noticeUserId + "");
        messageBean.setMessageId(UUID.randomUUID().toString());
        messageBean.setMsgType(0);// 单聊消息
        messageBean.setType(1);
        messageService.send(messageBean);
    }


    @Override
    public Friends addBlacklist(Integer userId, Integer toUserId,Integer isNotify) {
        // 是否存在AB关系
        Friends friendsAB = getFriendsDao().getFriends(userId, toUserId);
        Friends friendsBA = getFriendsDao().getFriends(toUserId, userId);
        if (null == friendsAB) {
            // 拉黑陌生人
            friendsAB = new Friends(userId, toUserId, getUserManager().getNickName(toUserId), Friends.Status.Stranger, Friends.Blacklist.Yes, ZERO);
            friendsBA = new Friends(toUserId,userId , getUserManager().getNickName(toUserId), Friends.Status.Stranger, Friends.Blacklist.No, ONE);
            getFriendsDao().saveFriends(friendsAB);
            getFriendsDao().saveFriends(friendsBA);
            notifyBlack(userId, toUserId, ZERO,isNotify);
        } else {
            // 拉黑好友
            getFriendsDao().updateFriends(new Friends(userId, toUserId, null, -1, Friends.Blacklist.Yes, friendsAB.getIsBeenBlack(),ZERO));
            if (null == friendsBA) {
                Friends friends = new Friends(toUserId, userId, getUserManager().getNickName(userId), Friends.Status.Stranger, Friends.Blacklist.No, 1);
                getFriendsDao().saveFriends(friends);
            } else {
                getFriendsDao().updateFriends(new Friends(toUserId, userId, null, null, friendsBA.getBlacklist(), 1, ZERO));
            }
            notifyBlack(userId, toUserId, ONE,isNotify);
        }
        messageRepository.deleteLastMsg(userId.toString(), toUserId.toString());
        // 维护好友标签数据
        friendGroupManager.deleteFriendToFriendGroup(userId, toUserId);
        // 维护好友数据
        deleteFriendsInfo(userId, toUserId);
        // 更新好友设置操作时间
        updateOfflineOperation(userId, toUserId);
        return getFriendsDao().getFriends(userId, toUserId);
    }

    @Override
    public Friends deleteBlacklist(Integer userId, Integer toUserId,Integer isNotify) {
        // 是否存在AB关系
        Friends friendsAB = getFriendsDao().getFriends(userId, toUserId);
        Friends friendsBA = getFriendsDao().getFriends(toUserId, userId);

        if (ObjectUtil.isNotNull(friendsAB)) {
            // 陌生人黑名单
            if ((Friends.Blacklist.Yes == friendsAB.getBlacklist() && Friends.Status.Stranger == friendsAB.getStatus()) || ObjectUtil.isNull(friendsBA)) {
                getFriendsDao().deleteFriends(userId, toUserId);
                getFriendsDao().deleteFriends(toUserId, userId);
                notifyBlack(userId, toUserId, TWO,isNotify);
            } else {
                friendsAB.setStatus(friendsAB.getStatus()==LOSE? Friends.Status.Attention : friendsAB.getStatus());
                getFriendsDao().updateFriends(new Friends(userId, toUserId, null, friendsBA.getStatus(), Friends.Blacklist.No, friendsAB.getIsBeenBlack(),ZERO));
                getFriendsDao().updateFriends(new Friends(toUserId, userId, null, friendsBA.getStatus(), friendsBA.getBlacklist(), ZERO,ZERO));
                notifyBlack(userId, toUserId, THREE,isNotify);
            }
            // 是否存在AB关系
            friendsAB = getFriendsDao().getFriends(userId, toUserId);
            // 维护好友数据
            deleteFriendsInfo(userId, toUserId);
            // 更新好友设置操作时间
            updateOfflineOperation(userId, toUserId);
        }
        return friendsAB;
    }

    /**
     * 后台移除黑名单
     **/
    public Friends consoleDeleteBlacklist(Integer userId, Integer toUserId, Integer adminUserId) {
        // 是否存在AB关系
        Friends friendsAB = getFriendsDao().getFriends(userId, toUserId);
        Friends friendsBA = getFriendsDao().getFriends(toUserId, userId);

        if (ObjectUtil.isNotNull(friendsAB)) {
            // 陌生人黑名单
            if (Friends.Blacklist.Yes == friendsAB.getBlacklist() && Friends.Status.Stranger == friendsAB.getStatus()) {
                // 物理删除
                getFriendsDao().deleteFriends(userId, toUserId);
            } else {
                // 恢复关系
                getFriendsDao().updateFriends(new Friends(userId, toUserId, null, 2, Friends.Blacklist.No, friendsAB.getIsBeenBlack()));
                getFriendsDao().updateFriends(new Friends(toUserId, userId, null, friendsBA.getStatus(), friendsBA.getBlacklist(), 0));
            }
            // 是否存在AB关系
            friendsAB = getFriendsDao().getFriends(userId, toUserId);
        }

        ThreadUtils.executeInThread((Callback) obj -> {
            //xmpp推送消息
            MessageBean messageBean = new MessageBean();
            messageBean.setType(MessageType.moveBlacklist);
            messageBean.setFromUserId(adminUserId + "");
           // messageBean.setFromUserName("System administrator");
            messageBean.setFromUserName("后台管理员");

            MessageBean beanVo = new MessageBean();
            beanVo.setFromUserId(userId + "");
            beanVo.setFromUserName(getUserManager().getNickName(userId));
            beanVo.setToUserId(toUserId + "");
            beanVo.setToUserName(getUserManager().getNickName(toUserId));
            messageBean.setObjectId(JSONObject.toJSONString(beanVo));
            messageBean.setMessageId(StringUtil.randomUUID());
            try {
                List<Integer> userIdList = new ArrayList<>();
                userIdList.add(userId);
                userIdList.add(toUserId);
                messageService.send(messageBean, userIdList);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        // 维护好友数据
        deleteFriendsInfo(userId, toUserId);
        return friendsAB;
    }

    /**
     * 查询拉黑指定用户的所有用户
     */
    public List<Integer> queryBlackList(Integer userId) {
        List<Friends> friends = friendsDao.queryBlackList(userId);
        if (CollectionUtil.isNotEmpty(friends)) {
            return friends.stream().map(Friends::getToUserId).collect(Collectors.toList());
        }
        return null;
    }

    /**
     * 拉黑消息通知
     * @param type 0 拉黑陌生人 1 拉黑好友  2 取消拉黑陌生人  3 取消拉黑好友
     */
    private void notifyBlack(Integer userId, Integer toUserId, Integer type,Integer isNotify) {
        if (ONE == isNotify){
            ThreadUtils.executeInThread(obj -> {
                MessageBean messageBean = new MessageBean();
                messageBean.setFromUserId(String.valueOf(userId));
                messageBean.setFromUserName(userCoreService.getNickName(userId));
                messageBean.setToUserId(String.valueOf(toUserId));
                messageBean.setToUserName(userCoreService.getNickName(toUserId));
                int messageType = type.equals(ZERO) || type.equals(ONE) ? MessageType.ADD_FRIENDS_BLACK_USER : MessageType.CANCEL_FRIENDS_BLACK_USER;
                messageBean.setMessageId(UUID.randomUUID().toString());
                messageBean.setMsgType(0);// 单聊消息
                messageBean.setType(messageType);
                messageService.send(messageBean);
            });
        }
    }

    private final Predicate<Friends> friendsPredicate = friends -> StrUtil.isNotBlank(friends.getRemarkName());

    /**
     * 查询所有好友备注信息
     */
    public Map<String, String> getAllRemarkName(Integer userId, Integer status) {
        List<Friends> friends = queryFollow(userId,status);
        if (CollectionUtil.isNotEmpty(friends)) {
            return friends.stream().filter(friendsPredicate).collect(Collectors.toMap(friends1 -> String.valueOf(friends1.getToUserId()), Friends::getRemarkName, (key1, key2) -> key2));
        }
        return new HashMap<>();
    }


    private void sendFriendsNameChangeTopic(Integer userId,Integer toUserId){
        JSONObject obj = new JSONObject();
        obj.put("userId",userId);
        if (ObjectUtil.isNotNull(toUserId)){
            obj.put("toUserId",toUserId);
        }
        MqMessageSendUtil.sendMessage(TopicConstant.FIRST_NAME_CHANGE_TOPIC,obj);
    }

    /**
     * 推送type = 945单聊控制消息通知
     **/
    public void sendModityShowMarker_single(User user, Friends friends,String markContent){
        MessageBean messageBean=new MessageBean();
        if(StringUtil.isEmpty(markContent)){
            messageBean.setContent(0);
        }else{
            messageBean.setContent(1);
        }
        messageBean.setFromUserId(user.getUserId() + "");
        messageBean.setFromUserName(user.getNickname());
        messageBean.setToUserId(String.valueOf(friends.getToUserId()));
        messageBean.setToUserName(friends.getToNickname());
        messageBean.setTimeSend(com.basic.utils.DateUtil.currentTimeSeconds());
        messageBean.setType(MsgType.TYPE_MODITY_SHOWMARKER);
        messageBean.setMsgType(0);
        messageBean.setFileName(markContent);
        messageBean.setObjectId(user.getUserId());
        messageBean.setTimeLen(0);
        messageBean.setMessageId(com.basic.utils.StringUtil.randomUUID());
        System.out.println(messageBean.toString());
        try {
            //群控制消息
            messageService.send(messageBean);
        } catch (Exception e) {
            System.out.println(user.getUserId() + "：推送失败");
            log.info(e.getMessage());
        }
    }

    public void updateFriends(ObjectId id,Map<String,Object> map,Integer userId) {
        log.info("修改的:"+userId);
         getFriendsDao().updateFriend(id,userId,map);
         friendsRedisRepository.deleteFriends(userId);
    }

    public void setFriendsSendMsgState(Integer userId,Integer toUserId,Byte isSendMsgState) {
        getFriendsDao().setFriendsSendMsgState(userId,toUserId,isSendMsgState);
        friendsRedisRepository.deleteFriends(userId);
        MultipointSyncUtil.multipointLoginDataSync(userId, MultipointSyncUtil.MultipointLogin.SEND_MSG_STATE);
    }




    @Override
    public void cleanFriendsSecretMsg(List<Integer> userIdList, Integer userId) {
        ThreadUtils.executeInThread(obj -> {
            List<Integer> friendUserIdList=null;
            try {
                MessageBean messageBean=null;
                for (Integer integer : userIdList) {
                    messageRepository.delFriendsChatMsg(userId,integer);
                    messageRepository.delFriendsChatMsg(integer,userId);

                    messageBean = new MessageBean();
                    messageBean.setType(MessageType.TYPE_DESTROY_MESSAGE);
                    messageBean.setFromUserId(userId + "");
                    messageBean.setFromUserName(SystemNo.CONSOLE_SYSTEM_NAME);
                    messageBean.setToUserId(integer + "");
                    messageBean.setMsgType(0);
                    messageBean.setMessageId(StringUtil.randomUUID());
                    messageService.send(messageBean);

                }

                friendUserIdList = friendsDao.queryEncryptFriendUserIdList(userId);
                /**
                 * 移除同事列表
                 */
                friendUserIdList.removeAll(userIdList);
                for (Integer toUserId : friendUserIdList) {
                    if(toUserId.equals(userId)){
                        continue;
                    }
                    messageRepository.delFriendsChatMsg(userId,toUserId);

                    messageRepository.delFriendsChatMsg(toUserId,userId);

                    messageBean = new MessageBean();
                    messageBean.setType(MessageType.TYPE_DESTROY_MESSAGE);
                    messageBean.setFromUserId(userId + "");
                    messageBean.setFromUserName(SystemNo.CONSOLE_SYSTEM_NAME);
                    messageBean.setToUserId(toUserId + "");
                    messageBean.setMsgType(0);
                    messageBean.setMessageId(StringUtil.randomUUID());
                    messageService.send(messageBean);

                }

            }catch (Exception e){

            }
        });

    }



    @Override
    public void cleanFriendsAllMsg(List<Integer> userIdList, Integer userId) {
        ThreadUtils.executeInThread(obj -> {
            List<Integer> friendUserIdList=null;
            try {
                MessageBean messageBean=null;
                for (Integer integer : userIdList) {

                    friendUserIdList = friendsDao.queryFriendUserIdList(integer);
                    for (Integer toUserId : friendUserIdList) {
                        messageRepository.delFriendsChatMsg(integer,toUserId);
                        if(!toUserId.equals(userId)) {
                            messageBean = new MessageBean();
                            messageBean.setType(MessageType.TYPE_DESTROY_MESSAGE);
                            messageBean.setFromUserId(userId + "");
                            messageBean.setFromUserName(SystemNo.CONSOLE_SYSTEM_NAME);
                            messageBean.setToUserId(toUserId + "");
                            messageBean.setMsgType(0);
                            messageBean.setMessageId(StringUtil.randomUUID());
                            messageService.send(messageBean);
                        }

                    }
                }


            }catch (Exception e){

            }
        });

    }

    /**
     * 清除同步老版本的数据
     */
    public void cleanAndupdateOldData(){
        long start=System.currentTimeMillis();
        log.info("cleanAndupdateOldDate start ==================>");

        friendsDao.cleanAndupdateOldData();

        log.info("cleanAndupdateOldDate end  ==================> {} 秒 ",(System.currentTimeMillis()-start)/1000);
    }

    @Override
    public void run(String... args) throws Exception {

        if(1==SKBeanUtils.getImCoreService().getAppConfig().getVersion()){
            ThreadUtils.executeInThread(obj -> {
                cleanAndupdateOldData();
            },3);
        }

    }



    /**
     * 删除用户和全部好友之间的双向聊天记录
     */
    public  void  deleteUserAllFriendsMsgRecord(int userId){


        ThreadUtils.executeInThread(obj -> {

            try {

                //获得用户的全部好友id 列表
                List<Integer> friendUserIdList = queryFriendUserIdList(userId);

                MessageBean messageBean = null;
                for (Integer friendUserId : friendUserIdList){

                    log.info("DeleteUserAllFriendsMsgRecord =========> {}",userId+" --> "+friendUserId);
                    messageRepository.delFriendsChatMsg(userId,friendUserId);

                    if(!friendUserId.equals(userId)) {
                        messageBean = new MessageBean();
                        messageBean.setType(MessageType.TYPE_DESTROY_MESSAGE);
                        messageBean.setFromUserId(userId + "");
                        messageBean.setFromUserName(SystemNo.CONSOLE_SYSTEM_NAME);
                        messageBean.setToUserId(friendUserId + "");
                        messageBean.setMsgType(0);
                        messageBean.setMessageId(StringUtil.randomUUID());
                        messageService.send(messageBean);
                    }

                    messageRepository.delFriendsChatMsg(friendUserId,userId);

                    if(!friendUserId.equals(userId)) {
                        messageBean = new MessageBean();
                        messageBean.setType(MessageType.TYPE_DESTROY_MESSAGE);
                        messageBean.setFromUserId(friendUserId + "");
                        messageBean.setFromUserName(SystemNo.CONSOLE_SYSTEM_NAME);
                        messageBean.setToUserId(userId + "");
                        messageBean.setMsgType(0);
                        messageBean.setMessageId(StringUtil.randomUUID());
                        messageService.send(messageBean);
                    }
                }


            }catch (Exception e){

            }
        });



    }



    //强制开启好友之间端到端加密，同时删除一天前的聊天记录
    public void ModifyFriendsChatSercetData(){

        //=== 不具备加密条件的用户 =>> 10003910

        /*int[] targetUserIds = {10042317, 10008291, 10041361, 10004476, 10003505,10027047, 10033990 ,
                10017332, 10039886, 10022588, 10040740, 10039592, 10027282, 10002092, 10040708, 10007042,
                10002984, 10004541, 10008295, 10010521, 10026994, 10022918, 10037273, 10040704, 10003910, 10031316};*/

        int[] targetUserIds = {10042317, 10008291, 10041361, 10004476, 10003505,10027047, 10033990 ,
                10017332, 10039886, 10022588, 10040740, 10039592, 10027282, 10002092, 10040708, 10007042,
                10002984, 10004541, 10008295, 10010521, 10026994, 10022918, 10037273, 10040704, 10031316};

        double recordTimeOut = 1;

        for (int i = 0; i < targetUserIds.length; i++) {
            int userId = targetUserIds[i];

            for (int j = 0; j < targetUserIds.length; j++) {
                int toUserId = targetUserIds[j];

                if(userId == toUserId){
                    continue;
                }

                System.out.println("== ## start modify chat msg passwd ==>"+ userId + "-->" + toUserId);


                Friends friends = getFriends(userId, toUserId);

                if(null==friends){
                    continue;
                }
                //设置好友之间聊天记录销毁时间
                friends.setChatRecordTimeOut(recordTimeOut);
                //修改和某个好友的消息加密方式   3 : 端到端加密
                //friends.setEncryptType((byte) 3);
                updateFriends(friends);

                /**
                 * 修改和某个好友的消息加密方式   3 : 端到端加密
                 */
                friendsDao.updateFriendsEncryptType(userId, toUserId, (byte) 3);
                //清除好友关系缓存
                friendsRedisRepository.deleteFriends(userId);
                if (getUserManager().isOpenMultipleDevices(userId)) {
                    MultipointSyncUtil.multipointLoginUpdateUserInfo(userId, getUserManager().getNickName(userId), toUserId, getUserManager().getNickName(toUserId), 1);
                }
            }
        }

        /*for (int i = 0; i < targetUserIds.length; i++) {
           int sourceUserId = targetUserIds[i];

            //查找用户公私钥
            Optional<AuthKeys> authKeys = Optional.ofNullable(authKeysService.getAuthKeys(sourceUserId));

            if (authKeys.isPresent()) {

                if( authKeys.get().getMsgDHKeyPair() == null || authKeys.get().getMsgRsaKeyPair() == null  ) {
                    System.out.println("=== TestModifyData === 不具备加密条件的用户 =>> " + sourceUserId );
                }
            }
        }*/

    }



}
