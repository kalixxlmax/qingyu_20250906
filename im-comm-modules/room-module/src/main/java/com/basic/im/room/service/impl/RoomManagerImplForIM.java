package com.basic.im.room.service.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import com.basic.im.common.service.PaymentManager;
import com.basic.im.pay.dao.ConsumeRecordDao;
import com.basic.im.pay.entity.BaseConsumeRecord;
import com.basic.im.room.vo.RoomPayVO;
import com.basic.im.user.constants.MoneyLogConstants;
import com.basic.im.user.entity.UserMoneyLog;
import com.basic.im.vo.JSONMessage;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.basic.common.model.PageResult;
import com.basic.common.model.PageVO;
import com.basic.commons.thread.ThreadUtils;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.constants.MsgType;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.ex.VerifyUtil;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.common.MultipointSyncUtil;
import com.basic.im.entity.Config;
import com.basic.im.event.MemberExitLiveRoomEvent;
import com.basic.im.i18n.LocaleMessageConstant;
import com.basic.im.i18n.LocaleMessageUtils;
import com.basic.im.message.IMessageRepository;
import com.basic.im.message.MessageService;
import com.basic.im.message.MessageType;
import com.basic.im.message.dao.TigaseMsgDao;
import com.basic.im.room.dao.*;
import com.basic.im.room.dto.MemberNameDTO;
import com.basic.im.room.entity.Room;
import com.basic.im.room.entity.Room.Member;
import com.basic.im.room.entity.Room.Notice;
import com.basic.im.room.entity.Room.Share;
import com.basic.im.room.service.RoomCoreRedisRepository;
import com.basic.im.room.service.RoomManager;
import com.basic.im.room.service.RoomRedisRepository;
import com.basic.im.room.vo.NearByRoom;
import com.basic.im.room.vo.RoomVO;
import com.basic.im.support.Callback;
import com.basic.im.user.dao.OfflineOperationDao;
import com.basic.im.user.dao.RoleDao;
import com.basic.im.user.entity.OfflineOperation;
import com.basic.im.user.entity.Role;
import com.basic.im.user.entity.User;
import com.basic.im.user.event.DeleteUserEvent;
import com.basic.im.user.event.UserChageNameEvent;
import com.basic.im.user.service.RoleCoreService;
import com.basic.im.user.service.UserCoreService;
import com.basic.im.utils.*;
import com.mongodb.client.result.UpdateResult;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;
import java.util.Arrays;

@Service(RoomManager.BEAN_ID)
public class RoomManagerImplForIM implements RoomManager {

    private Logger log = LoggerFactory.getLogger(RoomManager.class);

    @Autowired
    private RoomDao roomDao;
    @Autowired
    @Lazy
    private ConsumeRecordDao consumeRecordDao;

    public RoomDao getRoomDao() {
        return roomDao;
    }

    @Autowired
    private RoomCoreDao roomCoreDao;

    @Autowired
    private RoomMemberDao roomMemberDao;

    @Autowired
    private RoomMemberCoreDao roomMemberCoreDao;

    @Autowired
    private OfflineOperationDao offlineOperationDao;

    @Autowired
    private RoomNoticeDao roomNoticeDao;

    @Autowired
    private RoomRedisRepository roomRedisRepository;

    @Autowired
    private RoomCoreRedisRepository roomCoreRedisRepository;

    @Autowired
    private ShareDao shareDao;

    @Autowired
    private RoleDao roleDao;

    @Autowired
    private RoleCoreService roleCoreService;

    @Autowired
    @Lazy
    private MessageService messageService;

    @Autowired
    private IMessageRepository messageRepository;

    @Autowired
    private UserCoreService userCoreService;

    @Autowired
    @Lazy
    private TigaseMsgDao tigaseMsgDao;


    @Override
    public Room add(User user, Room entity, List<Integer> memberUserIdList, JSONObject userKeys) {

        Config config = SKBeanUtils.getSystemConfig();
        user.setNum(user.getNum() + 1);
        if (1 == entity.getIsSecretGroup()) {
            entity.setEncryptType((byte) 3);
        }
        entity.initRoomConfig(user.getUserId(), user.getNickname(), config); // 初始化群组配置

        List<Role> userRoles = roleCoreService.getUserRoles(user.getUserId(), null, 0);
        if (null != userRoles && userRoles.size() > 0) {
            for (Role role : userRoles) {
                if (role.getRole() == 4) {
                    entity.setPromotionUrl(role.getPromotionUrl());
                }
            }
        }

        if (null == entity.getName()) {
            entity.setName("我的群组");
        }
        if (null == entity.getDesc()) {
            entity.setDesc("");
        }

		/*if (null == entity.getLongitude()) {
            entity.setLongitude(0d);
        }
		if (null == entity.getLatitude()) {
            entity.setLatitude(0d);
        }*/

        if (null == entity.getJid()) {
            entity.setJid(StringUtil.randomUUID());
            messageService.createMucRoomToIMServer(entity.getJid(), user.getPassword(), user.getUserId().toString(),
                    entity.getName());

        }
        if(null!=user.getLoc()) {
            // 群组的经纬度即创建用户的经纬度
            entity.setLoc(new Room.Loc().setLat(user.getLoc().getLat()).setLng(user.getLoc().getLng()));
        }

        //初始化是否开启水印
        Config config_ = SKBeanUtils.getImCoreService().getConfig();
        entity.setShowMarker(config_.getShowMarker());

        // 保存房间配置
        roomDao.addRoom(entity);
        // 创建者
        Member member = new Member();
        member.setActive(DateUtil.currentTimeSeconds());
        member.setCreateTime(member.getActive());
        member.setModifyTime(0L);
        member.setNickname(user.getNickname());
        member.setRole(1);
        member.setRoomId(entity.getId());
        member.setSub(1);
        member.setTalkTime(0L);
        member.setCall(entity.getCall());
        member.setVideoMeetingNo(entity.getVideoMeetingNo());
        member.setUserId(user.getUserId());
        if (userKeys != null) {
            member.setChatKeyGroup(userKeys.getString(user.getUserId() + ""));
        }

        // 初始成员列表
        List<Member> memberList = Lists.newArrayList(member);

        //没有邀请群成员
        sendToChatNewMemberMessage(user.getUserId(), entity, member);

        /**
         * 删除 用户加入的群组 jid  缓存
         */
        if (0 == userCoreService.getOnlinestateByUserId(user.getUserId())) {
            roomCoreRedisRepository.addRoomPushMember(entity.getJid(), user.getUserId());
        }
        roomRedisRepository.deleteUserRoomJidList(user.getUserId());

        // 保存成员列表
        roomMemberDao.addMemberList(memberList);

        updateUserSize(entity.getId(), memberList.size());
        memberList.clear();
		/*// 用户加入的群组
		roomCoreRedisRepository.saveJidsByUserId(user.getUserId(), entity.getJid(), entity.getId());
		// 更新群组相关设置操作时间
		updateOfflineOperation(user.getUserId(), entity.getId(), null);
		roomCoreRedisRepository.deleteRoom(entity.getId().toString());
		roomCoreRedisRepository.deleteMemberList(entity.getId().toString());*/
//		}else
        if (null != memberUserIdList && !memberUserIdList.isEmpty()) {
            // 初始成员列表不为空

            ObjectId roomId = entity.getId();
            Long currentTimeSeconds = DateUtil.currentTimeSeconds();
			/*//添加群主
			memberUserIdList.add(user.getUserId());
			sendToChatNewMemberMessage(user.getUserId(), entity, member);*/

			/*ThreadUtils.executeInThread(new Callback() {
				@Override
				public void execute(Object obj) {*/
            Member _member = null;
            long messageSeqNo = roomCoreRedisRepository.queryGroupMessageSeqNo(entity.getJid());
            for (int userId : memberUserIdList) {
                User _user = userCoreService.getUser(userId);
                if(null==user){
                    continue;
                }
                currentTimeSeconds++;
                //群主在上面已经添加了
                if (userId != entity.getUserId()) {
                    //成员
                    _member = new Member();
                    _member.setActive(currentTimeSeconds);
                    _member.setCreateTime(currentTimeSeconds);
                    _member.setJoinSeqNo(messageSeqNo);
                    _member.setModifyTime(0L);
                    _member.setNickname(_user.getNickname());
                    _member.setRole(3);
                    _member.setRoomId(roomId);
                    _member.setSub(1);
                    _member.setCall(entity.getCall());
                    _member.setVideoMeetingNo(entity.getVideoMeetingNo());
                    _member.setTalkTime(0L);
                    _member.setUserId(_user.getUserId());
                    if (userKeys != null) {
                        _member.setChatKeyGroup(userKeys.getString(userId + ""));
                    }
                    memberList.add(_member);

                    // 发送单聊通知到被邀请人， 群聊
                    sendNewMemberMessage(user.getUserId(), entity, _member);

                    updateOfflineOperation(_member.getUserId(), entity.getId(), null);
                }
                /**
                 * 删除 用户加入的群组 jid  缓存
                 */

                if (0 == userCoreService.getOnlinestateByUserId(userId)) {
                    roomCoreRedisRepository.addRoomPushMember(entity.getJid(), userId);
                }
                roomCoreRedisRepository.saveJidsByUserId(userId, entity.getJid(), entity.getId());
            }

            // 保存成员列表
            roomMemberDao.addMemberList(memberList);
            updateUserSize(entity.getId(), memberList.size());

			/*	}
			});*/
        }
        // 用户加入的群组
        roomCoreRedisRepository.saveJidsByUserId(user.getUserId(), entity.getJid(), entity.getId());
        // 更新群组相关设置操作时间
        updateOfflineOperation(user.getUserId(), entity.getId(), null);
        roomCoreRedisRepository.deleteRoom(entity.getId().toString());
        roomCoreRedisRepository.deleteMemberList(entity.getId().toString());

        return entity;
    }

    /**
     * @param userId
     * @param roomId
     * @Description:更新群组相关设置操作时间 群组多点登录数据同步需要同步双方
     **/
    public void updateOfflineOperation(Integer userId, ObjectId roomId, String toUserIds) {
        long currentTime = DateUtil.currentTimeSeconds();
        OfflineOperation offlineOperation = offlineOperationDao.queryOfflineOperation(userId, null, String.valueOf(roomId));
        if (null == offlineOperation) {
            offlineOperationDao.addOfflineOperation(userId, MultipointSyncUtil.MultipointLogin.TAG_ROOM, String.valueOf(roomId), DateUtil.currentTimeSeconds());
        } else {
            OfflineOperation updateEntity = new OfflineOperation();
            updateEntity.setOperationTime(currentTime);
            offlineOperationDao.updateOfflineOperation(userId, String.valueOf(roomId), updateEntity);
        }
        if (!StringUtil.isEmpty(toUserIds)) {
            List<Integer> toUserIdList = StringUtil.getIntList(toUserIds, ",");
            toUserIdList.forEach(toUserId -> {
                OfflineOperation toOfflineOperation = offlineOperationDao.queryOfflineOperation(userId, null, String.valueOf(roomId));
                if (null == toOfflineOperation) {
                    offlineOperationDao.addOfflineOperation(toUserId, MultipointSyncUtil.MultipointLogin.TAG_ROOM, String.valueOf(roomId), currentTime);
                } else {
                    OfflineOperation updateEntity = new OfflineOperation();
                    updateEntity.setOperationTime(currentTime);
                    offlineOperationDao.updateOfflineOperation(userId, String.valueOf(roomId), updateEntity);
                }
            });
        }
    }


    @Override
    public void delete(ObjectId roomId, Integer userId) {
        Room room = roomCoreDao.getRoomById(roomId);
        if (null == room) {
            System.out.println("====> RoomManagerImplForIM > delete room is null ");
            return;
        }
        Member member = getMember(roomId, userId);

        List<Integer> userRoles = roleCoreService.getUserRoles(userId);
        if (null != member) {
            if (!userRoles.contains(5) && !userRoles.contains(6)) {
                if (1 != member.getRole()) {
                    throw new ServiceException(KConstants.ResultCode.NO_PERMISSION);
                }
            }
        } else {
            if (!userRoles.contains(5) && !userRoles.contains(6)) {
                throw new ServiceException(KConstants.ResultCode.NO_PERMISSION);
            }
        }

        destroyRoom(room,userId);


    }

    /**
     * 销毁解散房间逻辑执行
     * @param room
     * @param userId
     */
    public void destroyRoom(Room room,Integer userId){
        ObjectId roomId = room.getId();
        String roomJid = room.getJid();
        if (room.getUserSize() > 0) {
            MessageBean messageBean = new MessageBean();
            messageBean.setFromUserId(room.getUserId() + "");
            messageBean.setFromUserName(getMemberNickname(roomId, room.getUserId()));
            messageBean.setType(MessageType.DELETE_ROOM);
            messageBean.setFileSize(room.getEncryptType());
            messageBean.setObjectId(room.getJid());
            messageBean.setContent(room.getName());
            messageBean.setMessageId(StringUtil.randomUUID());
            // 发送单聊群解散通知
            sendChatGroupMsg(roomId, room.getJid(), messageBean);
        }

        ThreadUtils.executeInThread((Callback) obj -> {
            roomMemberDao.deleteById(roomId);
            List<Integer> memberIdList = getMemberIdList(roomId);
            for (Integer id : memberIdList) {
                // 维护用户加入群组 Jids 缓存
                roomCoreRedisRepository.delJidsByUserId(id, roomJid);
            }
            //删除群组 清除 群组成员
            roomMemberDao.deleteRoomMember(roomId, null);
            //删除公告
            roomNoticeDao.deleteNotice(roomId, null);

            // 删除群组相关的举报信息
            userCoreService.delReport(null, roomId.toString());
            roomDao.deleteRoom(roomId);
            // 维护群组、群成员缓存
            updateRoomInfoByRedis(roomId.toString());
            roomRedisRepository.deleteNoticeList(roomId.toString());
            // 处理面对面建群
            if (null != roomRedisRepository.queryLocationRoom(roomJid)) {
                String jid = roomRedisRepository.queryLocationRoomJid(room.getLoc().getLng(), room.getLoc().getLat(), room.getLocalRoomKey());
                //log.info(" ======  getRoomInfo jid ======" + jid);
                if (!StringUtil.isEmpty(jid)) {
                    roomRedisRepository.deleteLocalRoomJid(room.getLoc().getLng(), room.getLoc().getLat(), room.getLocalRoomKey());
                    roomRedisRepository.deleteLocalRoom(roomJid);
                }
            }
            // 删除 群共享的文件 和 删除群组离线消息记录
            destroyRoomMsgFileAndShare(roomId, roomJid);

        });
        // 更新群组相关设置操作时间
        updateOfflineOperation(userId, roomId, null);

    }



    /**
     * @param @param roomId  群主ID
     * @param @param talkTime   禁言到期时间   0 取消禁言
     * @Description:
     */
    public void roomAllBanned(ObjectId roomId, long talkTime) {
        log.info("[群全员禁言] 开始执行群全员禁言操作 - 群组ID: {}, 禁言到期时间: {}", roomId, talkTime);
        
        if (roomId == null) {
            log.warn("[群全员禁言] 群组ID为空，无法执行禁言操作");
            return;
        }
        
        if (talkTime == 0) {
            log.info("[群全员禁言] 禁言时间为0，执行取消全员禁言操作 - 群组ID: {}", roomId);
        } else {
            log.info("[群全员禁言] 设置全员禁言，禁言截止时间: {} ({})", talkTime, new java.util.Date(talkTime));
        }
        
        try {
            ThreadUtils.executeInThread((Callback) obj -> {
                log.debug("[群全员禁言] 异步执行群成员禁言更新 - 群组ID: {}, 禁言时间: {}", roomId, talkTime);
                try {
                    roomMemberDao.updateRoomMember(roomId, talkTime);
                    log.info("[群全员禁言] 成功更新群成员禁言状态 - 群组ID: {}, 禁言时间: {}", roomId, talkTime);
                } catch (Exception e) {
                    log.error("[群全员禁言] 更新群成员禁言状态失败 - 群组ID: {}, 错误: {}", roomId, e.getMessage(), e);
                }
            });
            log.info("[群全员禁言] 群全员禁言操作提交成功 - 群组ID: {}", roomId);
        } catch (Exception e) {
            log.error("[群全员禁言] 群全员禁言操作失败 - 群组ID: {}, 错误: {}", roomId, e.getMessage(), e);
            throw e;
        }
    }


    public synchronized Object update(User user, RoomVO roomVO, int isAdmin, int isConsole) {

//		Query<Room> query = getRoomDatastore().createQuery(getEntityClass());
//		query.filter("_id", roomVO.getRoomId());
//
//		UpdateOperations<Room> operations = getRoomDatastore().createUpdateOperations(getEntityClass());
        Map<String, Object> map = new HashMap<>();

        Room room = getRoom(roomVO.getRoomId());
        if (0 == isConsole) {
            if (null != room && room.getS() == -1) {
                throw new ServiceException(KConstants.ResultCode.RoomIsLock);
            }
        }
        if (!StringUtil.isEmpty(roomVO.getRoomName()) && (!room.getName().equals(roomVO.getRoomName()))) {
            UpdateGroupNickname(user, roomVO, isAdmin, room);
            return null;
        }
        /*全员禁言*/
        if (-2 < roomVO.getTalkTime()) {
            allBannedSpeak(user.getUserId(),room,roomVO.getTalkTime());
            return null;
        }

        if (!StringUtil.isEmpty(roomVO.getDesc())) {
//			operations.set("desc", roomVO.getDesc());
            map.put("desc", roomVO.getDesc());
        }
        if (!StringUtil.isEmpty(roomVO.getSubject())) {
//			operations.set("subject", roomVO.getSubject());
            map.put("subject", roomVO.getSubject());
        }
        try {
            if (!StringUtil.isEmpty(roomVO.getNotice())) {
                if (getMember(room.getId(), ReqUtil.getUserId()).getRole() == 3) {
                    throw new ServiceException(KConstants.ResultCode.NO_PERMISSION);
                }
                String noticeId = newNotice(user, roomVO, isAdmin, room);
                Map data = new HashMap();
                data.put("noticeId", noticeId);
                return data;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (-1 < roomVO.getShowRead() && room.getShowRead() != roomVO.getShowRead()) {
            alreadyReadNums(user, roomVO, isAdmin, room);
            return null;
        }
        if (-1 != roomVO.getIsNeedVerify()) {
            groupVerification(user, roomVO, isAdmin, room);
            return null;
        }
        if (-1 != roomVO.getIsLook()) {
            roomIsPublic(user, roomVO, isAdmin, room);
            return null;
        }
        if (-1 != roomVO.getAllowOpenLive()) {
            allowOpenLive(user, String.valueOf(roomVO.getAllowOpenLive()), room);
            return null;
        }
        if (null != roomVO.getMaxUserSize() && roomVO.getMaxUserSize() >= 0) {
            if (roomVO.getMaxUserSize() < room.getUserSize()) {
                throw new ServiceException(KConstants.ResultCode.NotLowerGroupMember);
            }
            int maxUserSize = SKBeanUtils.getImCoreService().getConfig().getMaxCrowdNumber();
            if (roomVO.getMaxUserSize() > maxUserSize) {
                throw new ServiceException(KConstants.ResultCode.RoomMemberAchieveMax);
            }
            map.put("maxUserSize", roomVO.getMaxUserSize());
        }
        // 锁定、取消锁定群组
        if (null != roomVO.getS() && 0 != roomVO.getS()) {
            roomIsLocking(user, roomVO, isAdmin, room);
            return null;
        }

        if (-1 != roomVO.getShowMember()) {
            showMember(user, roomVO, isAdmin, room);
            return null;
        }
        if (-1 != roomVO.getAllowSendCard()) {
            roomAllowSendCard(user, roomVO, isAdmin, room);
            return null;
        }

        if (-1 != roomVO.getAllowInviteFriend()) {
            roomAllowInviteFriend(user, roomVO.getAllowInviteFriend(), room);
            return null;
        }

        if (-1 != roomVO.getAllowUploadFile()) {
            roomAllowUploadFile(user, roomVO.getAllowUploadFile(), room);
            return null;
        }

        if (-1 != roomVO.getAllowConference()) {
            roomAllowConference(user, roomVO.getAllowConference(), room);
            return null;
        }

        if (-1 != roomVO.getAllowSpeakCourse()) {
            roomAllowSpeakCourse(user, roomVO.getAllowSpeakCourse(), room);
            return null;
        }


        if (-1 != roomVO.getAllowHostUpdate()) {
            map.put("allowHostUpdate", roomVO.getAllowHostUpdate());
        }

        // 聊天记录超时
        if (0 != roomVO.getChatRecordTimeOut()) {
            ChatRecordTimeOut(user, roomVO.getChatRecordTimeOut(), room);
            return null;
        }

        if (-1 != roomVO.getIsAttritionNotice()) {
            map.put("isAttritionNotice", roomVO.getIsAttritionNotice());
        }

        if (!StringUtil.isEmpty(roomVO.getRoomTitleUrl())) {
            map.put("roomTitleUrl", roomVO.getRoomTitleUrl());
        }
        if (-1 != roomVO.getWithdrawTime()) {
            map.put("withdrawTime", roomVO.getWithdrawTime());
        }
        if (-1 != roomVO.getAdminMaxNumber()) {
            map.put("adminMaxNumber", roomVO.getAdminMaxNumber());
        }
        if (-1 != roomVO.getAllowModifyCard()){
            map.put("allowModifyCard", roomVO.getAllowModifyCard());
            sendModityAllowModifyCard(user,roomVO.getAllowModifyCard(),room);
        }
        if (-1 != roomVO.getShowMarker()){
            map.put("showMarker", roomVO.getShowMarker());
            // 多点登录维护数据
            if (userCoreService.isOpenMultipleDevices(user.getUserId())) {
                offlineOperationDao.addOfflineOperation(user.getUserId(), MultipointSyncUtil.MultipointLogin.TAG_ROOM, String.valueOf(roomVO.getRoomId()), DateUtil.currentTimeSeconds());
            }
            sendModityShowMarker_room(user,roomVO.getShowMarker(),room);
        }
        map.put("modifyTime", DateUtil.currentTimeSeconds());


        synchronized (this) {
            roomDao.updateRoom(room.getId(), map);
        }
        // 维护群组相关缓存
        roomRedisRepository.deleteRoom(roomVO.getRoomId().toString());
        return null;
    }

    /**
     * 推送type = 944消息通知客户端刷新标志位
     **/
    public void sendModityAllowModifyCard(User user,byte allowModifyCard,Room room){
        MessageBean messageBean=new MessageBean();
        messageBean.setContent(allowModifyCard);
        messageBean.setObjectId(room.getJid());
        messageBean.setFromUserId(user.getUserId() + "");
        messageBean.setFromUserName(user.getNickname());
        messageBean.setToUserId(room.getJid());
        messageBean.setToUserName(room.getName());
        messageBean.setTimeSend(com.basic.utils.DateUtil.currentTimeSeconds());
        messageBean.setType(MsgType.TYPE_MODITY_ROOM_ALLOWMODIFYCARD);
        messageBean.setMsgType(1);
        messageBean.setMessageId(com.basic.utils.StringUtil.randomUUID());

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("isAttritionNotice", room.getIsAttritionNotice());
        messageBean.setOther(jsonObject.toJSONString());
        System.out.println(messageBean.toString());
        try {
            messageService.sendMsgToGroupByJid(room.getJid(),messageBean);
        } catch (Exception e) {
            System.out.println(user.getUserId() + "：推送失败");
            log.info(e.getMessage());
        }
    }

    /**
     * 推送type = 945群控制消息通知
     **/
    public void sendModityShowMarker_room(User user,byte showMarker,Room room){
        MessageBean messageBean=new MessageBean();
        messageBean.setContent(showMarker);
        messageBean.setObjectId(room.getJid());
        messageBean.setFromUserId(user.getUserId() + "");
        messageBean.setFromUserName(user.getNickname());
        messageBean.setToUserId(room.getJid());
        messageBean.setToUserName(room.getName());
        messageBean.setTimeSend(com.basic.utils.DateUtil.currentTimeSeconds());
        messageBean.setType(MsgType.TYPE_MODITY_SHOWMARKER);
        messageBean.setMsgType(1);
        messageBean.setMessageId(com.basic.utils.StringUtil.randomUUID());

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("isAttritionNotice", room.getIsAttritionNotice());
        messageBean.setOther(jsonObject.toJSONString());
        try {
            //群控制消息
            messageService.sendMsgToGroupByJid(room.getJid(),messageBean);
        } catch (Exception e) {
            System.out.println(user.getUserId() + "：推送失败");
            log.info(e.getMessage());
        }
    }



    public void updateEncryptType(Room room, int encryptType) {
//		updateAttribute(room.getId(),"encryptType",encryptType);
        Map<String, Object> map = new HashMap<>();
        map.put("encryptType", encryptType);
        roomDao.updateRoom(room.getId(), map);
//		roomDao.updateRoomEncryptType(room.getId(),encryptType);
        roomRedisRepository.deleteRoom(room.getId().toString());
        // 多点登录数据同步、消息通知
        sendNoticeAndMultipoint(room, MessageType.ModifyEncryptType, String.valueOf(encryptType));
		/*MessageBean messageBean = new MessageBean();
		try {
			// IMPORTANT 1-2、改房间名推送-已改
			messageBean.setFromUserId(room.getUserId() + "");
			messageBean.setFromUserName(userCoreService.getNickName(room.getUserId()));
			messageBean.setType(MessageType.ModifyEncryptType);
			messageBean.setObjectId(room.getJid());
			messageBean.setContent(encryptType);
			messageBean.setMessageId(StringUtil.randomUUID());
			messageBean.setToUserId(room.getJid());
			// 发送群聊
			sendGroupMsg(room.getJid(), messageBean);

			// 多点登录维护数据
			if(userCoreService.isOpenMultipleDevices(room.getUserId())){
				String nickName = userCoreService.getNickName(room.getUserId());
				multipointLoginUpdateUserInfo(room.getUserId(), nickName, room.getUserId(), nickName, room.getId());
			}
		} catch (Exception e) {
			log.error(e.getMessage());
		}*/

    }

    /**
     * 群组内创建或关闭直播间消息通知和数据维护
     *
     * @param room
     * @param roomMap  修改条件如：发起直播的群成员id
     * @param msgType  消息类型
     * @param userName 操作人的昵称
     */
    public void startOrCloseRoomLive(Room room, Map<String, Object> roomMap, int msgType, String userName) {
		/*Map<String,Object> map = new HashMap<>();
		map.put("liveUserId",liveUserId);*/
        roomDao.updateRoom(room.getId(), roomMap);
        roomRedisRepository.deleteRoom(room.getId().toString());
        // 多点登录数据同步、消息通知
        sendNoticeAndMultipoint(room, msgType, userName);
    }

    /**
     * 多点登录下修改群组信息并发送消息通知
     *
     * @param room
     */
    private void sendNoticeAndMultipoint(Room room, int msgType, String msgContent) {
        try {
            MessageBean messageBean = new MessageBean();
            // IMPORTANT 1-2、改房间名推送-已改
            messageBean.setFromUserId(room.getUserId() + "");
            messageBean.setFromUserName(userCoreService.getNickName(room.getUserId()));
            messageBean.setType(msgType);
            messageBean.setObjectId(room.getJid());
            if (!StringUtil.isEmpty(msgContent)) {
                messageBean.setContent(msgContent);
            }
            messageBean.setMessageId(StringUtil.randomUUID());
            messageBean.setToUserId(room.getJid());
            // 发送群聊
            sendGroupMsg(room.getJid(), messageBean);
            // 多点登录维护数据
            if (userCoreService.isOpenMultipleDevices(room.getUserId())) {
                String nickName = userCoreService.getNickName(room.getUserId());
                multipointLoginUpdateUserInfo(room.getUserId(), nickName, room.getUserId(), nickName, room.getId());
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    public void resetGroupChatKey(Room room, final JSONObject jsonGroupKeys) {

        ThreadUtils.executeInThread(obj -> {
            try {
                roomDao.dropRoomChatHistory(room.getJid());
                MessageBean messageBean = new MessageBean();
                messageBean.setFromUserId(room.getUserId() + "");
                messageBean.setFromUserName(userCoreService.getNickName(room.getUserId()));
                messageBean.setType(806);
                messageBean.setObjectId(room.getJid());
                messageBean.setToUserId(room.getJid());
                messageBean.setContent(room.getName());
                messageBean.setMessageId(StringUtil.randomUUID());
                dropRoomChatHistory(room.getJid());
                jsonGroupKeys.entrySet().stream().forEach(entny -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put("chatKeyGroup", entny.getValue());
                    roomMemberDao.updateRoomMember(room.getId(), Integer.valueOf(entny.getKey()), map);
                });
                sendGroupMsg(room.getJid(), messageBean);


            } catch (Exception e) {
                e.printStackTrace();
            }

        });


    }

    public void updateGroupChatKey(Room room, int userId, final String key) {

        try {
//				final DBCollection dbCollection = getRoomDatastore().getCollection(Room.Member.class);
				/*MessageBean messageBean = new MessageBean();
				messageBean.setFromUserId(room.getUserId() + "");
				messageBean.setFromUserName(userCoreService.getNickName(room.getUserId()));
				messageBean.setType(806);
				messageBean.setObjectId(room.getJid());
				messageBean.setToUserId(room.getJid());
				messageBean.setContent(room.getName());
				messageBean.setMessageId(StringUtil.randomUUID());
				sendGroupMsg(room.getJid(),messageBean);*/

            DBObject query = new BasicDBObject().append("roomId", room.getId()).append("userId", userId);
            BasicDBObject values = new BasicDBObject(com.basic.common.core.MongoOperator.SET, new BasicDBObject("chatKeyGroup", key));
//				dbCollection.update(query,values);
            Map<String, Object> map = new HashMap<>();
            map.put("chatKeyGroup", key);
            roomMemberDao.updateRoomMember(room.getId(), userId, map);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    /**
     * @param roomId
     * @Description:维护群组、群成员 缓存
     **/
    protected void updateRoomInfoByRedis(String roomId) {
        roomRedisRepository.deleteRoom(roomId);
        roomRedisRepository.deleteMemberList(roomId);
    }

    // 修改群昵称
    public synchronized void UpdateGroupNickname(User user, RoomVO roomVO, int isAdmin, Room room) {
        Map<String, Object> map = new HashMap<>();
        map.put("name", roomVO.getRoomName());
        roomDao.updateRoom(roomVO.getRoomId(), map);
        /**
         * 维护群组缓存
         */
        roomRedisRepository.deleteRoom(room.getId().toString());
        MessageBean messageBean = new MessageBean();
        if (1 == isAdmin) {
            // IMPORTANT 1-2、改房间名推送-已改
            messageBean.setFromUserId(user.getUserId() + "");
            //messageBean.setFromUserName(("10005".equals(user.getUserId().toString()) ? "Background system administrator" : getMemberNickname(room.getId(), user.getUserId())));
            messageBean.setFromUserName(("10005".equals(user.getUserId().toString()) ? "后台管理员" : getMemberNickname(room.getId(), user.getUserId())));
            messageBean.setType(MessageType.CHANGE_ROOM_NAME);
            messageBean.setObjectId(room.getJid());
            messageBean.setContent(roomVO.getRoomName());
            messageBean.setMessageId(StringUtil.randomUUID());

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("isAttritionNotice", room.getIsAttritionNotice());
            messageBean.setOther(jsonObject.toJSONString());
        }
        // 发送群聊
        sendGroupMsg(room.getJid(), messageBean);

    }

    // 全员禁言
    public void allBannedSpeak(Integer userId, Room room,long talkTime) {
        log.info("[全员禁言设置] 开始执行全员禁言设置 - 操作用户ID: {}, 群组ID: {}, 群组名称: {}, 禁言时间: {}", 
                userId, room.getId(), room.getName(), talkTime);
        
        if (userId == null || room == null) {
            log.warn("[全员禁言设置] 参数验证失败 - 用户ID: {}, 群组: {}", userId, room != null ? room.getId() : "null");
            return;
        }
        
        if (talkTime == 0) {
            log.info("[全员禁言设置] 取消全员禁言 - 群组: {} ({})", room.getId(), room.getName());
        } else {
            log.info("[全员禁言设置] 设置全员禁言，截止时间: {} ({})", talkTime, new java.util.Date(talkTime));
        }
        
        try {
            // 更新群组禁言时间
            Map<String, Object> map = new HashMap<>();
            map.put("talkTime", talkTime);
            log.debug("[全员禁言设置] 准备更新群组禁言时间 - 群组ID: {}, 禁言时间: {}", room.getId(), talkTime);
            
            roomDao.updateRoom(room.getId(), map);
            log.info("[全员禁言设置] 成功更新群组禁言时间 - 群组ID: {}", room.getId());
            
            //roomAllBanned(roomVO.getRoomId(), roomVO.getTalkTime());
            
            // 更新Redis缓存
            log.debug("[全员禁言设置] 更新群组Redis缓存 - 群组ID: {}", room.getId());
            updateRoomInfoByRedis(room.getId().toString());
            
            // 构建禁言通知消息
            String operatorNickname = getMemberNickname(room.getId(), userId);
            log.debug("[全员禁言设置] 获取操作者昵称: {} - 用户ID: {}", operatorNickname, userId);
            
            MessageBean messageBean = new MessageBean();
            messageBean.setType(MessageType.RoomAllBanned);
            messageBean.setFromUserId(userId.toString());
            messageBean.setFromUserName(operatorNickname);
            messageBean.setContent(String.valueOf(talkTime));
            messageBean.setObjectId(room.getJid());
            messageBean.setMessageId(StringUtil.randomUUID());

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("isAttritionNotice", room.getIsAttritionNotice());
            messageBean.setOther(jsonObject.toJSONString());
            
            log.info("[全员禁言设置] 准备发送群聊禁言通知 - 群组JID: {}, 消息ID: {}, 操作者: {}", 
                    room.getJid(), messageBean.getMessageId(), operatorNickname);
            
            // 发送群聊通知
            sendGroupMsg(room.getJid(), messageBean);
            log.info("[全员禁言设置] 成功发送群聊禁言通知 - 群组: {} ({})", room.getId(), room.getName());
            
            //sendChatGroupMsg(room.getId(), room.getJid(), messageBean);
            
            log.info("[全员禁言设置] 全员禁言设置完成 - 群组: {} ({}), 禁言状态: {}", 
                    room.getId(), room.getName(), talkTime == 0 ? "已取消" : "已设置");
                    
        } catch (Exception e) {
            log.error("[全员禁言设置] 全员禁言设置失败 - 群组ID: {}, 操作用户: {}, 错误: {}", 
                    room.getId(), userId, e.getMessage(), e);
            throw e;
        }

    }

    // 新公告
    public String newNotice(User user, RoomVO roomVO, int isAdmin, Room room) {
        Notice notice = new Notice(new ObjectId(), roomVO.getRoomId(), roomVO.getNotice(), user.getUserId(), user.getNickname());
        // 更新最新公告
        Map<String, Object> map = new HashMap<>();
        map.put("notice", notice);
        roomDao.updateRoom(roomVO.getRoomId(), map);
        // 新增历史公告记录
        roomNoticeDao.addNotice(notice);
        /**
         * 维护公告
         */
        roomRedisRepository.deleteNoticeList(room.getId());
        /**
         * 维护群组缓存
         */
        roomRedisRepository.deleteRoom(room.getId().toString());

        /*roomMemberDao.resetReadNotice(room.getId(),user.getUserId());*/
        roomMemberDao.readNotice(room.getId(),user.getUserId());

        MessageBean messageBean = new MessageBean();
        if (1 == isAdmin) {
            // IMPORTANT 1-5、改公告推送-已改
            messageBean.setFromUserId(user.getUserId() + "");
            messageBean.setFromUserName(getMemberNickname(room.getId(), user.getUserId()));
            messageBean.setType(MessageType.NEW_NOTICE);
            messageBean.setObjectId(room.getJid());
            messageBean.setContent(roomVO.getNotice());
            messageBean.setMessageId(StringUtil.randomUUID());
            messageBean.setFileName(notice.getId().toString());
        }
        // 发送群聊
        sendGroupMsg(room.getJid(), messageBean);
        return notice.getId().toString();
    }

    public void readNotice(ObjectId roomId, Integer userId){

        roomMemberDao.readNotice(roomId,userId);

    }
    // 显示已读人数
    public void alreadyReadNums(User user, RoomVO roomVO, int isAdmin, Room room) {
        Map<String, Object> map = new HashMap<>();
        map.put("showRead", roomVO.getShowRead());
        roomDao.updateRoom(roomVO.getRoomId(), map);
        /**
         * 维护群组缓存
         */
        roomRedisRepository.deleteRoom(room.getId().toString());
        MessageBean messageBean = new MessageBean();
        if (1 == isAdmin) {
            messageBean.setType(MessageType.SHOWREAD);
            messageBean.setFromUserId(user.getUserId().toString());
            messageBean.setFromUserName(getMemberNickname(room.getId(), user.getUserId()));
            messageBean.setContent(String.valueOf(roomVO.getShowRead()));
            messageBean.setObjectId(room.getJid());
            messageBean.setMessageId(StringUtil.randomUUID());

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("isAttritionNotice", room.getIsAttritionNotice());
            messageBean.setOther(jsonObject.toJSONString());
        }
        // 发送群聊
        sendGroupMsg(room.getJid(), messageBean);
    }

    // 群组验证
    public void groupVerification(User user, RoomVO roomVO, int isAdmin, Room room) {
        Map<String, Object> map = new HashMap<>();
        map.put("isNeedVerify", roomVO.getIsNeedVerify());
        roomDao.updateRoom(roomVO.getRoomId(), map);
        /**
         * 维护群组缓存
         */
        roomRedisRepository.deleteRoom(room.getId().toString());
        MessageBean messageBean = new MessageBean();
        if (1 == isAdmin) {
            messageBean.setType(MessageType.RoomNeedVerify);
            messageBean.setFromUserId(user.getUserId().toString());
            messageBean.setFromUserName(getMemberNickname(room.getId(), user.getUserId()));
            messageBean.setContent(String.valueOf(roomVO.getIsNeedVerify()));
            messageBean.setObjectId(room.getJid());
            messageBean.setMessageId(StringUtil.randomUUID());

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("isAttritionNotice", room.getIsAttritionNotice());
            messageBean.setOther(jsonObject.toJSONString());
        }
        // 发送群聊
        sendGroupMsg(room.getJid(), messageBean);
    }

    // 是否公开群组
    public void roomIsPublic(User user, RoomVO roomVO, int isAdmin, Room room) {
        Map<String, Object> map = new HashMap<>();
        map.put("isLook", roomVO.getIsLook());
        roomDao.updateRoom(roomVO.getRoomId(), map);
        /**
         * 维护群组缓存
         */
        roomRedisRepository.deleteRoom(room.getId().toString());
        MessageBean messageBean = new MessageBean();
        if (1 == isAdmin) {
            messageBean.setType(MessageType.RoomIsPublic);
            messageBean.setFromUserId(user.getUserId().toString());
            messageBean.setFromUserName(getMemberNickname(room.getId(), user.getUserId()));
            messageBean.setContent(String.valueOf(roomVO.getIsLook()));
            messageBean.setObjectId(room.getJid());
            messageBean.setMessageId(StringUtil.randomUUID());

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("isAttritionNotice", room.getIsAttritionNotice());
            messageBean.setOther(jsonObject.toJSONString());
        }
        // 发送群聊
        sendGroupMsg(room.getJid(), messageBean);
    }

    // 群组是否被锁定
    public void roomIsLocking(User user, RoomVO roomVO, int isAdmin, Room room) {
        Map<String, Object> map = new HashMap<>();
        map.put("s", roomVO.getS());
        roomDao.updateRoom(roomVO.getRoomId(), map);
        /**
         * 维护群组缓存
         */
        roomRedisRepository.deleteRoom(room.getId().toString());
        MessageBean messageBean = new MessageBean();
        if (1 == isAdmin) {
            messageBean.setType(MessageType.consoleProhibitRoom);
            messageBean.setFromUserId(user.getUserId().toString());
            messageBean.setFromUserName(getMemberNickname(room.getId(), user.getUserId()));
            messageBean.setContent(roomVO.getS());
            messageBean.setObjectId(room.getJid());
            messageBean.setMessageId(StringUtil.randomUUID());
        }
        // 发送群聊
        sendChatGroupMsg(roomVO.getRoomId(), room.getJid(), messageBean);
    }


    // 是否允许发送名片
    public void roomAllowSendCard(User user, RoomVO roomVO, int isAdmin, Room room) {
        Map<String, Object> map = new HashMap<>();
        map.put("allowSendCard", roomVO.getAllowSendCard());
        roomDao.updateRoom(roomVO.getRoomId(), map);
        /**
         * 维护群组缓存
         */
        roomRedisRepository.deleteRoom(room.getId().toString());
        MessageBean messageBean = new MessageBean();
        if (1 == isAdmin) {
            messageBean.setType(MessageType.RoomAllowSendCard);
            messageBean.setFromUserId(user.getUserId().toString());
            messageBean.setFromUserName(getMemberNickname(room.getId(), user.getUserId()));
            messageBean.setContent(String.valueOf(roomVO.getAllowSendCard()));
            messageBean.setObjectId(room.getJid());
            messageBean.setMessageId(StringUtil.randomUUID());

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("isAttritionNotice", room.getIsAttritionNotice());
            messageBean.setOther(jsonObject.toJSONString());
        }
        // 发送群聊
        sendGroupMsg(room.getJid(), messageBean);
    }

    // 普通成员 是否可以看到 群组内的成员
    public void showMember(User user, RoomVO roomVO, int isAdmin, Room room) {
        Map<String, Object> map = new HashMap<>();
        map.put("showMember", roomVO.getShowMember());
        roomDao.updateRoom(roomVO.getRoomId(), map);
        /**
         * 维护群组缓存
         */
        roomRedisRepository.deleteRoom(room.getId().toString());
        MessageBean messageBean = new MessageBean();
        if (1 == isAdmin) {
            messageBean.setType(MessageType.RoomShowMember);
            messageBean.setFromUserId(user.getUserId().toString());
            messageBean.setFromUserName(getMemberNickname(room.getId(), user.getUserId()));
            messageBean.setContent(String.valueOf(roomVO.getShowMember()));
            messageBean.setObjectId(room.getJid());
            messageBean.setMessageId(StringUtil.randomUUID());

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("isAttritionNotice", room.getIsAttritionNotice());
            messageBean.setOther(jsonObject.toJSONString());
        }
        // 发送群聊
        sendGroupMsg(room.getJid(), messageBean);
    }

    // 是否允许群成员邀请好友
    public void roomAllowInviteFriend(User user, int allowInviteFriend, Room room) {
        Map<String, Object> paramMap = Maps.newConcurrentMap();
        paramMap.put("allowInviteFriend", allowInviteFriend);
        baseMessageBean(user, room, MessageType.RoomAllowInviteFriend, String.valueOf(allowInviteFriend), paramMap);
    }

    // 是否允许群成员上传文件
    public void roomAllowUploadFile(User user, int allowUploadFile, Room room) {
        Map<String, Object> paramMap = Maps.newConcurrentMap();
        paramMap.put("allowUploadFile", allowUploadFile);
        baseMessageBean(user, room, MessageType.RoomAllowUploadFile, String.valueOf(allowUploadFile), paramMap);
    }

    // 群组允许成员召开会议
    public void roomAllowConference(User user, int allowConference, Room room) {
        Map<String, Object> paramMap = Maps.newConcurrentMap();
        paramMap.put("allowConference", allowConference);
        baseMessageBean(user, room, MessageType.RoomAllowConference, String.valueOf(allowConference), paramMap);
    }

    //  群组允许成员开启讲课
    public void roomAllowSpeakCourse(User user, int allowSpeakCourse, Room room) {
        Map<String, Object> paramMap = Maps.newConcurrentMap();
        paramMap.put("allowSpeakCourse", allowSpeakCourse);
        baseMessageBean(user, room, MessageType.RoomAllowSpeakCourse, String.valueOf(allowSpeakCourse), paramMap);
    }

    // 聊天记录超时设置通知
    public void ChatRecordTimeOut(User user, double chatRecordTimeOut, Room room) {
        Map<String, Object> paramMap = Maps.newConcurrentMap();
        paramMap.put("chatRecordTimeOut", chatRecordTimeOut);
        baseMessageBean(user, room, MessageType.ChatRecordTimeOut, String.valueOf(chatRecordTimeOut), paramMap);
    }

    /**
     * 是否群成员开启群组直播
     *
     * @param
     * @return
     */
    public void allowOpenLive(User user, String allowOpenLive, Room room) {
        Map<String, Object> paramMap = Maps.newConcurrentMap();
        paramMap.put("allowOpenLive", allowOpenLive);
        baseMessageBean(user, room, MessageType.IsOPenLiveRoomByMember, allowOpenLive, paramMap);
    }

    /**
     * 修改群属性，发群控制消息
     *
     * @param
     * @return
     */
    protected synchronized void baseMessageBean(User user, Room room, int msgType, String content, Map paramMap) {
        paramMap.put("modifyTime", DateUtil.currentTimeSeconds());
        roomDao.updateRoom(room.getId(), paramMap);
        // 维护群组缓存
        roomRedisRepository.deleteRoom(room.getId().toString());
        MessageBean messageBean = new MessageBean();
        messageBean.setType(msgType);
        messageBean.setFromUserId(user.getUserId().toString());
        messageBean.setFromUserName(getMemberNickname(room.getId(), user.getUserId()));
        messageBean.setContent(content);
        messageBean.setObjectId(room.getJid());
        messageBean.setMessageId(StringUtil.randomUUID());

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("isAttritionNotice", room.getIsAttritionNotice());
        messageBean.setOther(jsonObject.toJSONString());
        // 发送群聊
        sendGroupMsg(room.getJid(), messageBean);
    }

    // 单聊通知某个人
    public void sendGroupOne(Integer userIds, MessageBean messageBean) {
        try {
            messageBean.setMsgType(0);
            messageService.send(messageBean);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 发送群聊通知
    public void sendGroupMsg(String jid, MessageBean messageBean) {
        try {
            messageService.sendMsgToGroupByJid(jid, messageBean);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 发送单聊通知某个人 ,且 发送群聊通知
    public void sendChatToOneGroupMsg(Integer userIds, String jid, MessageBean messageBean) {
        try {
            // 发送单聊
            messageBean.setMsgType(0);
            messageBean.setMessageId(StringUtil.randomUUID());
            messageService.send(messageBean);
            // 发送群聊
           messageService.sendMsgToGroupByJid(jid, messageBean);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 发送单聊通知群组所有人 ,且 发送群聊通知
    public void sendChatGroupMsg(ObjectId roomId, String jid, MessageBean messageBean) {
        try {
            // 发送单聊
            messageBean.setMsgType(0);
            messageBean.setMessageId(StringUtil.randomUUID());
            messageService.send(messageBean, getMemberIdList(roomId));
            // 发送群聊
            ThreadUtils.executeInThread(new Callback() {

                @Override
                public void execute(Object obj) {
                    try {
                        messageService.sendMsgToGroupByJid(jid, messageBean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * @param @param roomId  群主ID
     * @param @param toUserId   新群主 用户ID   必须 是 群内成员
     * @Description: TODO(群主 转让)
     */
    public Room transfer(Room room, Integer toUserId) {

        String nickName = userCoreService.getNickName(toUserId);
//		Query<Room> roomQuery = getRoomDatastore().createQuery(getEntityClass()).filter("_id", room.getId());
//		UpdateOperations<Room> roomOps = getRoomDatastore().createUpdateOperations(getEntityClass());
//		roomOps.set("userId", toUserId);
//		roomOps.set("nickname", nickName);
//		getRoomDatastore().update(roomQuery, roomOps);
        Map<String, Object> map = new HashMap<>();
        map.put("userId", toUserId);
        map.put("nickname", nickName);
        roomDao.updateRoom(room.getId(), map);

        /*修改 旧群主的角色*/
//		Query<Member> query = getRoomDatastore().createQuery(Member.class);
//		query.filter("roomId", room.getId());
//		query.filter("userId", room.getUserId());
//		UpdateOperations<Member> operations = getRoomDatastore().createUpdateOperations(Member.class);
//		operations.set("role", 3);
//		getRoomDatastore().update(query,operations);
        roomMemberDao.updateRoomMemberRole(room.getId(), room.getUserId(), 3);

        /*赋值新群主的角色*/
//		query=SKBeanUtils.getImRoomDatastore().createQuery(Member.class);
//		query.filter("roomId", room.getId());
//		query.filter("userId",toUserId);
//		operations = SKBeanUtils.getImRoomDatastore().createUpdateOperations(Member.class);
//		operations.set("role", 1);
//		SKBeanUtils.getImRoomDatastore().update(query, operations);
        roomMemberDao.updateRoomMemberRole(room.getId(), toUserId, 1);
        // 更新群组、群成员相关缓存
        updateRoomInfoByRedis(room.getId().toString());
        MessageBean message = new MessageBean();
        message.setType(MessageType.RoomTransfer);
        message.setFromUserId(room.getUserId() + "");
        message.setFromUserName(getMemberNickname(room.getId(), room.getUserId()));
        message.setObjectId(room.getJid());
        message.setToUserId(toUserId.toString());
        message.setToUserName(userCoreService.getNickName(toUserId));
        message.setMessageId(StringUtil.randomUUID());

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("isAttritionNotice", room.getIsAttritionNotice());
        message.setOther(jsonObject.toJSONString());
        // 发送单聊通知被转让的人、群聊通知
        sendChatToOneGroupMsg(toUserId, room.getJid(), message);
//		return get(room.getId());
        return roomCoreDao.getRoomById(room.getId());
    }


    @Override
    public Room get(ObjectId roomId, Integer pageIndex, Integer pageSize) {
        // redis room 不包含 members noties
        Room specialRoom;
        Room redisRoom = roomRedisRepository.queryRoom(roomId);
        if (null != redisRoom) {
            if (-1 == redisRoom.getS()) {
                throw new ServiceException(KConstants.ResultCode.RoomIsLock);
            }
            specialRoom = specialHandleByRoom(redisRoom, roomId, pageIndex, pageSize);
        } else {
//			Room room = getRoomDatastore().createQuery(getEntityClass()).field("_id").equal(roomId).get();
            Room room = roomCoreDao.getRoomById(roomId);
            if (null != room && -1 == room.getS()) {
                throw new ServiceException(KConstants.ResultCode.RoomIsLock);
            }
            if (null == room) {
                throw new ServiceException(KConstants.ResultCode.NotRoom);
            }
            specialRoom = specialHandleByRoom(room, roomId, pageIndex, pageSize);
        }

        List<Member> members = specialRoom.getMembers();
        if (members != null) {
            members.forEach(obj -> obj.setHiding(userCoreService.getSettings(obj.getUserId()).getHiding()));
        }
        return specialRoom;
    }




    @Override
    public Room getRoomByJid(String roomJid) {
        return roomCoreDao.getRoomByJid(roomJid);
    }

    /**
     * @param room
     * @param roomId
     * @return
     * @Description: 房间相关特殊处理操作
     **/
    public Room specialHandleByRoom(Room room, ObjectId roomId, Integer pageIndex, Integer pageSize) {
        // 特殊身份处理
        Member member = getMember(roomId, ReqUtil.getUserId());
        if (null == member) {
            // 主动加群（二维码扫描），该用户不再群组内，需要members
            Room joinRoom = getRoom(roomId);
//			List<Member> members = getMembers(roomId,pageIndex,pageSize);
            List<Member> members = getHeadMemberListByPageImpls(roomId, pageSize, 0);
            joinRoom.setMembers(members);
            return joinRoom;
        }
        int role = member.getRole();
        List<Member> members = null;
//		 监护人和隐身人不能互看  保证每次都有自己
        if (1 != member.getRole()) {
//			Query<Member> query = getRoomDatastore().createQuery(Room.Member.class).field("roomId").equal(roomId).order("role").order("createTime").offset(pageIndex*pageSize).limit(pageSize);
            if (role > KConstants.Room_Role.CREATOR && role < KConstants.Room_Role.INVISIBLE) {
//				Query<Member> queryMemberSize = getRoomDatastore().createQuery(Room.Member.class).field("roomId").equal(roomId).offset(pageIndex*pageSize).limit(pageSize);
//				query.field("role").lessThan(KConstants.Room_Role.INVISIBLE).order("role");
//				members = query.asList();
                members = roomMemberDao.getMemberListLessThan(roomId, KConstants.Room_Role.INVISIBLE, pageIndex, pageSize);
//				int specialSize = queryMemberSize.field("role").greaterThanOrEq(4).asList().size();// 隐身人监护人
//				int specialSize = roomMemberDao.getMemberListLessThanOrEq(roomId,4,0,0).size();
                int specialSize = (int) roomMemberDao.getMemberNumGreaterThanOrEq(roomId, (byte) 4, pageIndex, pageSize);
                room.setUserSize(room.getUserSize() - specialSize);
            } else if (role == KConstants.Room_Role.INVISIBLE || role == KConstants.Room_Role.GUARDIAN) {
                // 隐身人
//				query.or(query.criteria("role").lessThan(KConstants.Room_Role.INVISIBLE),query.criteria("userId").equal(ReqUtil.getUserId()));
//				members = query.asList();
                Map<String, Object> membersMap = roomMemberDao.getMemberListOr(roomId, KConstants.Room_Role.INVISIBLE, ReqUtil.getUserId(), pageIndex, pageSize);
                members = (List<Member>) membersMap.get("members");
                room.setUserSize(Integer.valueOf(membersMap.get("count").toString()));
            }
            room.setMembers(members);
        } else {
//			List<Member> membersList = getMembers(roomId,pageIndex,pageSize);
            List<Member> membersList = getHeadMemberListByPageImpls(roomId, pageSize, member.getRole());
            room.setMembers(membersList);
        }
        // 群公告
        List<Notice> noticesCache = roomRedisRepository.getNoticeList(roomId);
        if (null != noticesCache && noticesCache.size() > 0) {
            room.setNotices(noticesCache);
        } else {
//			List<Notice> noticesDB = getRoomDatastore().createQuery(Room.Notice.class).field("roomId").equal(roomId).order("-time").asList();
            List<Notice> noticesDB = roomNoticeDao.getNoticList(roomId, 0, 0);
            room.setNotices(noticesDB);
            /**
             * 维护群公告列表缓存
             */
            roomRedisRepository.saveNoticeList(roomId, noticesDB);
        }
        return room;
    }


    /**
     * 首先返回群主、管理员然后按加群时间排序
     **/
    public List<Member> getMembers(ObjectId roomId, Integer pageIndex, Integer pageSize) {
        List<Member> members = new ArrayList<>();
        // 群成员
        List<Member> memberCacheList = roomRedisRepository.getMemberList(roomId.toString(), pageIndex, pageSize);
        if (null != memberCacheList && memberCacheList.size() > 0) {
            members = memberCacheList;
        } else {
            // 群主管理员
            List<Member> adminList = roomMemberDao.getMemberListLessThanOrEq(roomId, 2, 0, 0);
            // 普通成员
            List<Member> memberAllList = roomMemberDao.getMemberListGreaterThan(roomId, 2, 0, 0);
            int adminSize = adminList.size();
            if (pageSize > adminSize) {
                pageSize -= adminSize;
                List<Member> memberList = roomMemberDao.getMemberListGreaterThan(roomId, 2, pageIndex, pageSize);
                members.addAll(adminList);
                members.addAll(memberList);
            } else {
                members = roomMemberDao.getMemberListLessThanOrEq(roomId, 2, pageIndex, pageSize);
            }
            List<Member> dbMembers = new ArrayList<>();
            dbMembers.addAll(adminList);
            dbMembers.addAll(memberAllList);
            // 维护群成员缓存数据
            roomRedisRepository.saveMemberList(roomId.toString(), dbMembers);
        }
        return members;
    }

    /**
     * 群成员分页
     **/
    public List<Member> getMemberListByPageImpl(ObjectId roomId, long joinTime, Integer pageSize, String keyword) {
        Member member = getMember(roomId, ReqUtil.getUserId());
        if (null == member) {
            throw new ServiceException(KConstants.ResultCode.MemberNotInGroup);
        }
        List<Member> memberListByTime;
        if (StrUtil.isNotBlank(keyword)){
            memberListByTime = roomMemberDao.getMemberListByTime(roomId, member.getRole(), joinTime, pageSize,keyword);
        }else{
            if (0 == joinTime) {
                memberListByTime = getHeadMemberListByPageImpls(roomId, pageSize, member.getRole());
            }else{
                memberListByTime = roomMemberDao.getMemberListByTime(roomId, member.getRole(), joinTime, pageSize);
            }
        }
        memberListByTime.forEach(obj -> {
            if (member.getRole() == 1 || member.getRole() == 2) { // 只有群主或者管理员可以查看加群方式
                obj.setAddRoomDetails(getAddRoomDetails(obj));
            }
            User user = userCoreService.getUser(obj.getUserId());
            if (ObjectUtil.isNotNull(user)){
                obj.setAccount(user.getAccount());
            }
        });
        return memberListByTime;
    }

    /**
     * @Description:room/get 和 joinTime 为0时返回群成员 列表
     * // 补全问题 ： 例如 ：pageSize = 100 。  第一种情况 小于pageSize{ 群组 + 管理员 = 80人   返回 80+20普通群成员}
     * 第二种情况 大于等于pageSize{ 群组 + 管理员 = 120人   返回 120人 + 1名最先加群的普通群成员主要拿到createTime}
     **/
    public List<Member> getHeadMemberListByPageImpls(ObjectId roomId, Integer pageSize, int role) {
        List<Member> members = new ArrayList<Member>();
        // 群主管理员
        List<Member> adminList = roomMemberDao.getMemberListLessThanOrEq(roomId, KConstants.Room_Role.ADMIN, 0, 0);
        int adminSize = adminList.size();
        if (adminSize < pageSize) {
            // 补全 pageSize
            members.addAll(adminList);
            members.addAll(roomMemberDao.getMemberListAdminRole(roomId, role, pageSize - adminSize));
        } else {
            members.addAll(adminList);
            members.addAll(roomMemberDao.getMemberListAdminRole(roomId, role, 1));
        }
        return members;
    }

    public Room consoleGetRoom(ObjectId roomId) {
//		Room room = getRoomDatastore().createQuery(getEntityClass()).field("_id").equal(roomId).get();
        Room room = roomCoreDao.getRoomById(roomId);
        if (null != room) {
//			List<Member> members = getRoomDatastore().createQuery(Room.Member.class).field("roomId").equal(roomId).order("createTime").order("role").asList();
            List<Member> members = roomMemberDao.getMemberListOrder(roomId);
//			List<Notice> notices = getRoomDatastore().createQuery(Room.Notice.class).field("roomId").equal(roomId).order("-time").asList();
            List<Notice> notices = roomNoticeDao.getNoticList(roomId, 0, 0);
            room.setMembers(members);
            room.setNotices(notices);
            if (0 == room.getUserSize()) {
                room.setUserSize(members.size());
//				DBObject q = new BasicDBObject("_id", roomId);
//				DBObject o = new BasicDBObject("$set", new BasicDBObject("userSize", members.size()));
//				getRoomDatastore().getCollection(getEntityClass()).update(q, o);
                Map<String, Object> map = new HashMap<>();
                map.put("userSize", members.size());
                roomDao.updateRoom(roomId, map);
            }
        }
        return room;
    }

    public Room findRoom(ObjectId roomId) {
        return roomCoreDao.getRoomById(roomId);
    }




    public Room getOnlyRoom(ObjectId roomId){
        Room room = roomRedisRepository.queryRoom(roomId);
        if (null != room) {
            return room;
        } else {
            room = roomCoreDao.getRoomById(roomId);
            if (null == room) {
                throw new ServiceException(KConstants.ResultCode.NotRoom);
            }
            /**
             * 缓存 房间
             */
            roomRedisRepository.saveRoom(room);
        }
        return room;
    }

    /**
     * @param @param  roomId
     * @param @return 参数
     * @Description: TODO(获取群组详情 ， 群主和管理员信息 ， 考虑特殊身份隐身人监护人 ， 不获取普通群成员列表和公告列表, )
     */
    public Room getRoom(ObjectId roomId) {
        Room room;
        Room roomCache = roomRedisRepository.queryRoom(roomId);
        if (null != roomCache) {
            room = roomCache;
        } else {
//			Room roomDB = getRoomDatastore().createQuery(getEntityClass()).field("_id").equal(roomId).get();
            Room roomDB = roomCoreDao.getRoomById(roomId);
            if (null == roomDB) {
                throw new ServiceException(KConstants.ResultCode.NotRoom);
            }
            room = roomDB;
            /**
             * 缓存 房间
             */
            roomRedisRepository.saveRoom(room);
        }
        // 群组和管理员信息
        room.setMembers(getAdministrationMemberList(roomId));
        Integer userId = ReqUtil.getUserId();
        int userRole = roleCoreService.getUserRoleByUserId(userId);
        Member member = null;
        int role = 0;
        member = getMember(roomId, userId);
        // 面对面建群，用户不在群组中处理
        if (null == member) {
            return room;
        }
        role = member.getRole();
        // 后台管理中获取群组详情
        if (KConstants.Admin_Role.ADMIN != userRole && KConstants.Admin_Role.SUPER_ADMIN != userRole) {
            member = getMember(roomId, userId);
            if (null == member) {
                throw new ServiceException(KConstants.ResultCode.MemberNotInGroup);
            }
            role = member.getRole();
        }
//		Query<Member> memberQuery = getRoomDatastore().createQuery(Member.class).field("roomId").equal(roomId);
        if (KConstants.Room_Role.CREATOR < role && role < KConstants.Room_Role.INVISIBLE) {
//			long invisibleCustodyCount = memberQuery.field("role").greaterThanOrEq(KConstants.Room_Role.INVISIBLE).count();
            long invisibleCustodyCount = roomMemberDao.getMemberNumGreaterThanOrEq(roomId, KConstants.Room_Role.INVISIBLE, 0, 0);
            int userSize = (int) (room.getUserSize() - invisibleCustodyCount);
            room.setUserSize(userSize);
        } else if (KConstants.Room_Role.INVISIBLE == role || KConstants.Room_Role.GUARDIAN == role) {
//			memberQuery.or(memberQuery.criteria("role").lessThan(KConstants.Room_Role.INVISIBLE),memberQuery.criteria("userId").equal(userId));
//			long userCount = memberQuery.count();
            long userCount = roomMemberDao.getMemberNumLessThan(roomId, KConstants.Room_Role.INVISIBLE, userId);
            room.setUserSize((int) userCount);
        }
        return room;
    }

    @Override
    public Room getRoom(ObjectId roomId, Integer userId) {
        Room room = null;
        Room roomCache = roomRedisRepository.queryRoom(roomId);
        if (null != roomCache) {
            room = roomCache;
        } else {
//			Room roomDB = getRoomDatastore().createQuery(getEntityClass()).field("_id").equal(roomId).get();
            Room roomDB = roomCoreDao.getRoomById(roomId);
            if (null == roomDB) {
                throw new ServiceException(KConstants.ResultCode.NotRoom);
            }
            room = roomDB;
            /**
             * 缓存 房间
             */
            roomRedisRepository.saveRoom(room);
        }
        // 群组和管理员信息
        room.setMembers(getAdministrationMemberList(roomId));
        int userRole = roleCoreService.getUserRoleByUserId(userId);
        Member member = null;
        int role = 0;
        // 后台管理中获取群组详情
        if (KConstants.Admin_Role.ADMIN != userRole && KConstants.Admin_Role.SUPER_ADMIN != userRole) {
            member = getMember(roomId, userId);
            if (null == member) {
                throw new ServiceException(KConstants.ResultCode.MemberNotInGroup);
            }
            role = member.getRole();
        }
//		Query<Member> memberQuery = getRoomDatastore().createQuery(Member.class).field("roomId").equal(roomId);
        if (KConstants.Room_Role.CREATOR < role && role < KConstants.Room_Role.INVISIBLE) {
//			long invisibleCustodyCount = memberQuery.field("role").greaterThanOrEq(KConstants.Room_Role.INVISIBLE).count();
            long invisibleCustodyCount = roomMemberDao.getMemberNumGreaterThanOrEq(roomId, KConstants.Room_Role.INVISIBLE, 0, 0);
            int userSize = (int) (room.getUserSize() - invisibleCustodyCount);
            room.setUserSize(userSize);
        } else if (KConstants.Room_Role.INVISIBLE == role || KConstants.Room_Role.GUARDIAN == role) {
//			memberQuery.or(memberQuery.criteria("role").lessThan(KConstants.Room_Role.INVISIBLE),memberQuery.criteria("userId").equal(userId));
//			long userCount = memberQuery.count();
            long userCount = roomMemberDao.getMemberNumLessThan(roomId, KConstants.Room_Role.INVISIBLE, userId);
            room.setUserSize((int) userCount);
        }
        return room;
    }


    /**
     * @param @param  roomId
     * @param @return 参数
     * @Description: TODO(获取群组详情 ， 包含群主信息)
     */
    public Room getRoomInfoAndMemberByJid(String roomJid) {

        Room roomDB = roomCoreDao.getRoomByJid(roomJid);
        if (null == roomDB) {
            throw new ServiceException(KConstants.ResultCode.NotRoom);
        }
        /**
         * 缓存 房间
         */
        roomRedisRepository.saveRoom(roomDB);
        Member member = null;
        int role = 0;
        member = getMember(roomDB.getId(), ReqUtil.getUserId());
        // 面对面建群，用户不在群组中处理
        if (null == member) {
            return roomDB;
        }
        role = member.getRole();

        if (KConstants.Room_Role.CREATOR < role && role < KConstants.Room_Role.INVISIBLE) {
            long invisibleCustodyCount = roomMemberDao.getMemberNumGreaterThanOrEq(roomDB.getId(), KConstants.Room_Role.INVISIBLE, 0, 0);
            int userSize = (int) (roomDB.getUserSize() - invisibleCustodyCount);
            roomDB.setUserSize(userSize);
        } else if (KConstants.Room_Role.INVISIBLE == role || KConstants.Room_Role.GUARDIAN == role) {

            long userCount = roomMemberDao.getMemberNumLessThan(roomDB.getId(), KConstants.Room_Role.INVISIBLE, ReqUtil.getUserId());
            roomDB.setUserSize((int) userCount);
        }
        return roomDB;
    }


    /**
     * @param roomId
     * @param userId
     * @return
     * @Description: 获取群组详情，群主和管理员信息，不考虑特殊身份隐身人监护人，不获取普通群成员列表和公告列表,)
     **/
    public Room getRoomInfo(ObjectId roomId, Integer userId) {
        Room room = null;
        Room roomCache = roomRedisRepository.queryRoom(roomId);
        if (null != roomCache) {
            room = roomCache;
        } else {
            Room roomDB = roomCoreDao.getRoomById(roomId);
            if (null == roomDB) {
                throw new ServiceException(KConstants.ResultCode.NotRoom);
            }
            room = roomDB;
            /**
             * 缓存 房间
             */
            roomRedisRepository.saveRoom(room);
        }
        // 群组和管理员信息
        room.setMembers(getAdministrationMemberList(roomId));
        return room;
    }

    public Integer getCreateUserId(ObjectId roomId) {
        return roomDao.getCreateUserId(roomId);
    }

    public ObjectId getRoomId(String jid) {
        return roomCoreDao.getRoomId(jid);
    }

    public String queryRoomJid(ObjectId roomId) {
        return roomCoreDao.queryRoomJid(roomId);
    }

    public Integer queryRoomStatus(ObjectId roomId) {
        return roomDao.queryRoomStatus(roomId);
    }

    public String getRoomName(String jid) {
        return roomCoreDao.getRoomNameByJid(jid);
    }

    @Override
    public String getRoomName(ObjectId roomId) {
        return roomCoreDao.getRoomNameByRoomId(roomId);
    }

    // 房间状态
    @Override
    public Integer getRoomStatus(ObjectId roomId) {
        return roomDao.getRoomStatus(roomId);
    }

	@Override
	public List<Room> selectList(int pageIndex, int pageSize, String roomName, int isJoinGroup, Integer userId,int isSearchAllJoinGroup) {
		List<Room> rooms = roomDao.getRoomListOrName(pageIndex, pageSize, roomName,isSearchAllJoinGroup);
		if (isJoinGroup == KConstants.ONE && ObjectUtil.isNotNull(userId)){
			List<String> joinGroupRoomIds =  roomMemberDao.joinGroupList(userId);
			if (CollectionUtil.isNotEmpty(joinGroupRoomIds)){
				rooms.forEach(obj -> obj.setInGroup(joinGroupRoomIds.contains(obj.getJid())));
			}
		}
		return rooms;
	}

    /**
     * @param @param  userId
     * @param @return 参数
     * @Description: TODO(查询用户加入的所有群的jid)
     */
    public List<String> queryUserRoomsJidList(int userId) {
        List<ObjectId> roomIdList = queryUserRoomsIdList(userId);

        return roomCoreDao.queryUserRoomsJidList(roomIdList);
    }

    /**
     * @param userId
     * @return
     * @Description:在表CHAT_ROOMJIDS_USERID 下查询用户加入的所有群的jid
     **/
    public List<String> queryUserRoomsJidListByDB(int userId) {
        return roomMemberCoreDao.queryUserRoomsJidListByDB(userId);
    }

    /**
     * 查询用户开启免打扰的  群组Jid 列表
     *
     * @param userId
     * @return
     */
    public List<String> queryUserNoPushJidList(int userId) {
        return roomMemberCoreDao.queryUserNoPushJidList(userId);
    }

    /**
     * @param @param  userId
     * @param @return 参数
     * @Description: TODO(查询用户加入的所有群的roomId)
     */
    public List<ObjectId> queryUserRoomsIdList(int userId) {
        return roomMemberCoreDao.getRoomIdListByUserId(userId);
    }

    @Override
    public Object selectHistoryList(int userId, int type) {
        List<ObjectId> historyIdList;

        historyIdList = roomMemberCoreDao.getRoomIdListByType(userId, type);
        if (historyIdList.isEmpty()) {
            return null;
        }

        List<Room> historyList = roomDao.getRoomList(historyIdList, 0, 0, 0);
        historyList.forEach(room -> {
            Member member = roomMemberDao.getMember(room.getId(), userId);
            room.setMember(member);
        });

        return historyList;
    }

    @Override
    public Object selectHistoryList(int userId, int type, int pageIndex, int pageSize) {
        List<ObjectId> historyIdList = roomMemberCoreDao.getRoomIdListByType(userId, type);
        if (historyIdList.isEmpty()) {
            return historyIdList;
        }

        List<Room> historyList = roomDao.getRoomList(historyIdList, 1, pageIndex, pageSize);
        historyList.forEach(room -> {
            Member member = roomMemberDao.getMember(room.getId(), userId);
            room.setMember(member);
        });

        return historyList;
    }


    @Override
    public void deleteMember(User user, ObjectId roomId, int userId, boolean deleteUser) {
        Room room_ = roomDao.getRoom(roomId);
        Room room = getRoom(roomId, user.getUserId());
        Member roomMember = getMember(roomId, user.getUserId());
        Member member = getMember(roomId, userId);
        if (member == null) {
            throw new ServiceException(KConstants.ResultCode.MemberNotInGroup);
        }

        // 发布关闭直播间事件
        sendMemberExitLiveRoomEvent(userId, roomId.toString());

        // 处理后台管理员
        if (null == roomMember) {
            // 后台管理员
            Role role = roleDao.getUserRoleByUserId(user.getUserId());
            if (null != role) {
                if (5 == role.getRole() || 6 == role.getRole()) {
                    if (-1 == role.getStatus()) {
                        throw new ServiceException(KConstants.ResultCode.BackAdminStatusError);
                    }
                    if (!deleteUser && room.getUserId().equals(userId)) {
                        throw new ServiceException(KConstants.ResultCode.NotRemoveOwner);
                    }
                }
            } else {
                throw new ServiceException(KConstants.ResultCode.MemberNotInGroup);
            }
        } else {
            // 自己退群
            if (!user.getUserId().equals(userId)) {
                if (roomMember.getRole() >= 3) {
                    throw new ServiceException(KConstants.ResultCode.NO_PERMISSION);
                }
                if (room.getUserId().equals(userId)) {
                    throw new ServiceException(KConstants.ResultCode.NotRemoveOwner);
                }
                // 处理管理员踢管理员和隐身人监护人的问题
                if (member.getRole() != 1 && member.getRole() != 3) {
                    // 处理群主和后台管理员踢人问题
                    if (2 == roomMember.getRole()) {
                        throw new ServiceException(KConstants.ResultCode.NO_PERMISSION);
                    }
                }
            }
        }
        // 处理解散群组
        if (room.getUserId().equals(userId)) {
            delete(roomId, userId);
            return;
        }
        User toUser = userCoreService.getUser(userId);
        // IMPORTANT 1-4、删除成员推送-已改
        MessageBean messageBean = new MessageBean();
        messageBean.setFromUserId(user.getUserId() + "");
        messageBean.setFromUserName(getMemberNickname(roomId, user.getUserId()));
        messageBean.setType(MessageType.DELETE_MEMBER);
        // messageBean.setObjectId(roomId.toString());
        messageBean.setObjectId(room.getJid());
        messageBean.setToUserId(userId + "");
        if(null!=toUser) {
            messageBean.setToUserName(toUser.getNickname());
        }
        messageBean.setContent(room.getName());
        messageBean.setMessageId(StringUtil.randomUUID());

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("isAttritionNotice", room_.getIsAttritionNotice());
        messageBean.setOther(jsonObject.toJSONString());
        /**
         * 2021 01 23修改
         * 单聊通知被踢出人，群聊通知群组人员
         *
         *
         */
        // 群组减员发送通知
        if(0==room.getIsAttritionNotice()){
            messageRepository.deleteRoomMemberMessage(room.getJid(),userId);
        }
        if (KConstants.Room_Role.INVISIBLE != member.getRole() && KConstants.Room_Role.GUARDIAN != member.getRole()) {
            // 发送单聊通知被踢出本人、群聊
            sendChatToOneGroupMsg(userId, room.getJid(), messageBean);
        } else {
            sendGroupOne(userId, messageBean);
        }


        /**
         * 2020 11 03 修改不管怎样都发送群聊减员通知，不然客户端会有影响
         */

        // 发送单聊通知被踢出本人、群聊
        //sendChatToOneGroupMsg(userId, room.getJid(), messageBean);
        /**
         必须在 roomMemberDao.deleteRoomMember(roomId, userId); 之前执行
         */
        updateRoomAvatarUserIds(roomId,room.getJid(),userId);

        roomMemberDao.deleteRoomMember(roomId, userId);
        updateUserSize(roomId, -1);

        // 维护用户加入群组的jids
        roomCoreRedisRepository.delJidsByUserId(userId, room.getJid());
        // 维护群组、群成员推送
        roomCoreRedisRepository.removeRoomPushMember(room.getJid(), userId);
        // 维护群组、群成员 缓存
        updateRoomInfoByRedis(roomId.toString());
        // 更新群组相关设置操作时间
        updateOfflineOperation(user.getUserId(), roomId, String.valueOf(userId));


    }


    /*
     *907 邀请群成员消息  只发送单聊
     * */
    public void sendToChatNewMemberMessage(int fromUserId, Room room, Member member) {
        // IMPORTANT 1-7、新增成员
        MessageBean messageBean = createNewMemberMessage(fromUserId, room, member);

        // 发送单聊通知到被邀请人
        messageBean.setMsgType(0);
        if (StringUtil.isEmpty(messageBean.getMessageId())) {
            messageBean.setMessageId(StringUtil.randomUUID());
        }
        messageService.send(messageBean);

        updateRoomAvatarUserIds(room);

    }


    /**
     * 发送 907 邀请群成员 进群消息 单聊和群聊
     */
    public void sendNewMemberMessage(int fromUserId, Room room, Member member) {
        MessageBean messageBean = createNewMemberMessage(fromUserId, room, member);

        // 发送单聊通知到被邀请人， 群聊
        sendChatToOneGroupMsg(member.getUserId(), room.getJid(), messageBean);
        updateRoomAvatarUserIds(room);
    }

    @Override
    public void updateRoomAvatarUserIds(Room room){
        if(5<room.getUserSize()){
            return;
        }
        updateRoomAvatarUserIds(room.getId(),room.getJid(),null);

    }


    private void updateRoomAvatarUserIds(ObjectId roomId,String roomJid,Integer userId){
        List<Integer> roomAvatarUserIdList = roomMemberDao.getRoomAvatarUserIdList(roomId);

        if(null!=userId){
            if( null == roomAvatarUserIdList || !roomAvatarUserIdList.contains(userId)) {
                return;
            }else {
                roomAvatarUserIdList.remove(userId);
            }
        }

        ThreadUtils.executeInThread(obj->{
            FileUtil.updateGroupAvatarUserIds(SKBeanUtils.getImCoreService().getAppConfig().getUploadDomain(),
                    roomJid, roomAvatarUserIdList);
        });

    }


    private MessageBean createNewMemberMessage(int fromUserId, Room room, Member member) {
        // IMPORTANT 1-7、新增成员
        MessageBean messageBean = new MessageBean();
        messageBean.setType(MessageType.NEW_MEMBER);
        // messageBean.setObjectId(roomId.toString());
        messageBean.setObjectId(room.getJid());
        messageBean.setFromUserId(fromUserId + "");
        messageBean.setFromUserName(userCoreService.getNickName(fromUserId));
        messageBean.setToUserId(member.getUserId() + "");
        messageBean.setToUserName(member.getNickname());
        messageBean.setFileSize(room.getShowRead());
        messageBean.setContent(room.getName());
        messageBean.setFileName(room.getId().toString());

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("showRead", room.getShowRead());
        jsonObject.put("lsLook", room.getIsLook());
        jsonObject.put("isNeedVerify", room.getIsNeedVerify());
        jsonObject.put("showMember", room.getShowMember());
        jsonObject.put("allowSendCard", room.getAllowSendCard());
        jsonObject.put("maxUserSize", room.getMaxUserSize());
        jsonObject.put("isSecretGroup", room.getIsSecretGroup());
        jsonObject.put("chatKeyGroup", member.getChatKeyGroup());
        jsonObject.put("joinSeqNo", member.getJoinSeqNo());
        jsonObject.put("createrId", room.getUserId());
        messageBean.setOther(jsonObject.toJSONString());
        messageBean.setMessageId(StringUtil.randomUUID());
        return messageBean;

    }


    @Override
    public Map<String, List<Integer>> updateMember(User user, ObjectId roomId, List<Integer> userIdList, JSONObject userKeys, int operationType, int inviteUserId) {
        List<Integer> failList = new ArrayList<>();
        Room room = roomCoreDao.getRoomById(roomId);
        Member invitationMember = getMember(roomId, user.getUserId());

        if (null != invitationMember && KConstants.Room_Role.INVISIBLE == invitationMember.getRole()) {
            throw new ServiceException(KConstants.ResultCode.NotInviteInvisible);
        }

        if (room.getMaxUserSize() < room.getUserSize() + userIdList.size()) {
            throw new ServiceException(KConstants.ResultCode.RoomMemberAchieveMax);
        }
        List<Member> list = new ArrayList<>();
        int i = 0;
        long messageSeqNo = roomCoreRedisRepository.queryGroupMessageSeqNo(room.getJid());
        // 发送 943 消息通知开启被邀请加群确认的用户，通知客户端哪些用户开启了验证
        List<Integer> noticeUserIds = new ArrayList<>();
        for (int userId : userIdList) {
            i++;
            long currenTimes = DateUtil.currentTimeSeconds();
            currenTimes += i;
            User _user = userCoreService.getUser(userId);
            if (null == _user) {
                continue;
            }
            //如果是付费群
            if (room.getNeedPay()==1){
                noticeUserIds.add(userId);
                continue;
            }
            // 开启被邀请确认
            if (userId != inviteUserId){
                if (1 == _user.getSettings().getBeInvitedJoinRoom()) {
                    noticeUserIds.add(userId);
                    continue;
                }
            }
            if (0 < findMemberAndRole(roomId, userId)) {
                log.info(" 用户   {}   已经加入群组   ", userId);
                failList.add(userId);
                continue;
            }
            Member _member = new Member();
            _member.setUserId(userId);
            _member.setRole(3);
            _member.setActive(currenTimes);
            _member.setCreateTime(currenTimes);
            _member.setJoinSeqNo(messageSeqNo);
            _member.setModifyTime(0L);
            _member.setNickname(userCoreService.getNickName(userId));
            _member.setRoomId(roomId);
            _member.setSub(1);
            _member.setTalkTime(0L);

            if (userKeys != null) {
                _member.setChatKeyGroup(userKeys.getString(userId + ""));
            }

            if (operationType == 1) {
                _member.setInviteUserId(inviteUserId);
                _member.setOperationType(inviteUserId != 0 ? Member.OperationType.INVITE : Member.OperationType.OTHER);
            } else if (operationType == 2 || operationType == 3) {    // 用户邀请
                _member.setInviteUserId(user.getUserId());
                _member.setOperationType(operationType == 2 ? Member.OperationType.INVITE : Member.OperationType.PAY_INVITE);
            }
            list.add(_member);
        }

        roomMemberDao.addMemberList(list);
        updateUserSize(roomId, list.size());

        for (Member member : list) {
            sendNewMemberMessage(operationType == 1 ? inviteUserId : user.getUserId(), room, member);
            // 维护用户加入的群 jids
            roomCoreRedisRepository.saveJidsByUserId(member.getUserId(), room.getJid(), roomId);
            if (0 == userCoreService.getOnlinestateByUserId(member.getUserId())) {
                roomCoreRedisRepository.addRoomPushMember(room.getJid(), member.getUserId());
            }
            updateOfflineOperation(member.getUserId(), room.getId(), null);
        }

        // 维护群组、群成员缓存
        updateRoomInfoByRedis(roomId.toString());
        // 更新群组相关设置操作时间
        updateOfflineOperation(user.getUserId(), roomId, StringUtil.getIntegerByList(userIdList, ","));
        // 通知开启被邀请加群确认的用户
        beInvitedNotice(0 != inviteUserId ? inviteUserId : user.getUserId(), noticeUserIds, roomId);
        Map<String, List<Integer>> members = Maps.newConcurrentMap();
        members.put("failList", failList);
        members.put("noticeUserIds", noticeUserIds);
        return members;
    }

    /**
     * 通知开启被邀请加群确认用户
     */
    public void beInvitedNotice(Integer userId, List<Integer> userIds, ObjectId roomId) {
        Room room = getRoom(roomId);
        MessageBean messageBean = new MessageBean();
        messageBean.setType(MessageType.inviteJoinRoom);
        messageBean.setFromUserId(userId + "");
        messageBean.setFromUserName(userCoreService.getNickName(userId));
        messageBean.setContent(room.getName());
        messageBean.setObjectId(room.getJid());
        messageBean.setFileSize(room.getUserSize());
        messageBean.setFileName(room.getId() + "");
        messageBean.setMessageId(StringUtil.randomUUID());

        userIds.forEach(toUserId->{
            roomCoreRedisRepository.addInviteCode(toUserId,roomId);// 添加用户邀请记录
        });
        // 发送单聊通知
        messageService.send(messageBean, userIds);
    }

    @Override
    public void updateMember(User user, ObjectId roomId, Member member, int operationType, int inviteUserId) {
        Room room = getRoom(roomId);
        if (null != room && room.getS() == -1) {
            throw new ServiceException(KConstants.ResultCode.RoomIsLock);
        }
        Integer memberUserId = member.getUserId();
        Member oldMember = getMember(roomId, memberUserId);
        if (null == oldMember) {
            throw new ServiceException(KConstants.ResultCode.NotGroupMember);
        }

        //User toUser = userCoreService.getUser(memberUserId);

        if (null != roomMemberDao.getMember(roomId, memberUserId)) {
            if (!StringUtil.isEmpty(member.getNickname()) && !oldMember.getNickname().equals(member.getNickname())) {
                // IMPORTANT 1-1、改昵称推送-已改
                MessageBean messageBean = new MessageBean();
                messageBean.setType(MessageType.CHANGE_NICK_NAME);
                messageBean.setObjectId(room.getJid());
                messageBean.setFromUserId(user.getUserId() + "");
                messageBean.setFromUserName(getMemberNickname(roomId, user.getUserId()));
                messageBean.setToUserId(memberUserId+ "");
                messageBean.setToUserName(oldMember.getNickname());
                messageBean.setContent(member.getNickname());
                messageBean.setMessageId(StringUtil.randomUUID());

                JSONObject jsonObject = new JSONObject();
                jsonObject.put("isAttritionNotice", room.getIsAttritionNotice());
                messageBean.setOther(jsonObject.toJSONString());
                // 发送群聊
                sendGroupMsg(room.getJid(), messageBean);
            }
            if (null != member.getTalkTime()) {
                log.info("[单个成员禁言] 开始处理成员禁言 - 群组ID: {}, 被禁言用户ID: {}, 操作用户ID: {}, 禁言时间: {}", 
                        roomId, memberUserId, user.getUserId(), member.getTalkTime());
                        
                if (oldMember.getRole() == KConstants.Room_Role.INVISIBLE) {
                    log.warn("[单个成员禁言] 隐身用户无法被禁言 - 用户ID: {}, 群组ID: {}", memberUserId, roomId);
                    throw new ServiceException(KConstants.ResultCode.NotChatInvisible);
                }
                
                if (member.getTalkTime() == 0) {
                    log.info("[单个成员禁言] 取消用户禁言 - 用户ID: {}, 群组ID: {}", memberUserId, roomId);
                } else {
                    log.info("[单个成员禁言] 设置用户禁言，截止时间: {} ({}) - 用户ID: {}, 群组ID: {}", 
                            member.getTalkTime(), new java.util.Date(member.getTalkTime()), memberUserId, roomId);
                }
                
                try {
                    // IMPORTANT 1-6、禁言
                    String operatorNickname = getMemberNickname(roomId, user.getUserId());
                    log.debug("[单个成员禁言] 获取操作者昵称: {} - 用户ID: {}", operatorNickname, user.getUserId());
                    
                    MessageBean messageBean = new MessageBean();
                    messageBean.setType(MessageType.GAG);
                    // messageBean.setObjectId(roomId.toString());
                    messageBean.setObjectId(room.getJid());
                    messageBean.setFromUserId(user.getUserId() + "");
                    messageBean.setFromUserName(operatorNickname);
                    messageBean.setToUserId(memberUserId + "");
                    messageBean.setToUserName(oldMember.getNickname());
                    messageBean.setContent(member.getTalkTime() + "");
                    messageBean.setMessageId(StringUtil.randomUUID());

                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("isAttritionNotice", room.getIsAttritionNotice());
                    messageBean.setOther(jsonObject.toJSONString());
                    
                    log.info("[单个成员禁言] 准备发送禁言通知消息 - 群组JID: {}, 被禁言用户: {} ({}), 操作者: {} ({}), 消息ID: {}", 
                            room.getJid(), memberUserId, oldMember.getNickname(), user.getUserId(), operatorNickname, messageBean.getMessageId());
                    
                    // 发送单聊通知被禁言的人,群聊
                    sendChatToOneGroupMsg(memberUserId, room.getJid(), messageBean);
                    log.info("[单个成员禁言] 成功发送禁言通知消息 - 用户ID: {}, 群组ID: {}", memberUserId, roomId);
                    
                } catch (Exception e) {
                    log.error("[单个成员禁言] 发送禁言通知消息失败 - 用户ID: {}, 群组ID: {}, 错误: {}", 
                            memberUserId, roomId, e.getMessage(), e);
                    throw e;
                }
            }
            Map<String, Object> map = new HashMap<>();
            if (!memberUserId.equals(user.getUserId()) && 0 != member.getRole()) {
                map.put("role", member.getRole());
            }
            if (null != member.getSub()) {
                map.put("sub", member.getSub());
            }
            if (null != member.getTalkTime()) {
                map.put("talkTime", member.getTalkTime());
                log.debug("[成员信息更新] 更新成员禁言时间 - 用户ID: {}, 群组ID: {}, 禁言时间: {}", 
                        memberUserId, roomId, member.getTalkTime());
            }
            if (!StringUtil.isEmpty(member.getNickname())) {
                map.put("nickname", member.getNickname());
            }
            if (!StringUtil.isEmpty(member.getRemarkName())) {
                map.put("remarkName", member.getRemarkName());
            }
            map.put("modifyTime", DateUtil.currentTimeSeconds());
            map.put("call", room.getCall());
            map.put("videoMeetingNo", room.getVideoMeetingNo());
            map.put("role", oldMember.getRole());
            // 更新成员信息
            roomMemberDao.updateRoomMember(roomId, memberUserId, map);
        } else {
            Member invitationMember = getMember(roomId, user.getUserId());
            if (null != invitationMember && 4 == invitationMember.getRole()) {
                throw new ServiceException(KConstants.ResultCode.NotInviteInvisible);
            }
            if (room.getMaxUserSize() < room.getUserSize() + 1) {
                throw new ServiceException(KConstants.ResultCode.RoomMemberAchieveMax);
            }
            User _user = userCoreService.getUser(memberUserId);
            Member _member = new Member(roomId, _user.getUserId(), _user.getNickname());

            if (operationType == 1) {
                _member.setInviteUserId(inviteUserId);
                _member.setOperationType(inviteUserId != 0 ? Member.OperationType.INVITE : Member.OperationType.OTHER);
            }

            member.setJoinSeqNo(roomCoreRedisRepository.queryGroupMessageSeqNo(room.getJid()));
            roomMemberDao.addMember(_member);
            updateUserSize(roomId, 1);


            // 发送单聊通知到被邀请人， 群聊
            //sendNewMemberMessage(operationType == 1 ? inviteUserId : user.getUserId(), room, _member);
            sendNewMemberMessage(user.getUserId(), room, _member);

            // 维护用户加入的群jids
            roomCoreRedisRepository.saveJidsByUserId(memberUserId, room.getJid(), roomId);
            if (0 == userCoreService.getOnlinestateByUserId(memberUserId)) {
                roomCoreRedisRepository.addRoomPushMember(room.getJid(), memberUserId);
            }
            updateOfflineOperation(_member.getUserId(), room.getId(), null);
        }

        /**
         * 维护群组、群成员缓存
         */
        updateRoomInfoByRedis(roomId.toString());
        // 更新群组相关设置操作时间
        updateOfflineOperation(user.getUserId(), roomId, null);
    }

    @Override
    public Member getMember(ObjectId roomId, int userId) {
        Member member = roomMemberDao.getMember(roomId, userId);
        Integer reqUserId = ReqUtil.getUserId();
        if(userId!=reqUserId) {
            Member reqMember = (reqUserId != userId && userId != 0) ? roomMemberDao.getMember(roomId, reqUserId) : member;
            if (null != member && null != reqMember) {
                if (reqMember.getRole() == 1 || reqMember.getRole() == 2) {
                    member.setAddRoomDetails(getAddRoomDetails(member));
                }
                member.setHiding(userCoreService.getSettings(userId).getHiding());
            }
        }
        return member;
    }

    public int findMemberAndRole(ObjectId roomId, int userId) {
        Object role = roomMemberDao.findMemberRole(roomId, userId);
        return null != role ? (int) role : -1;
    }


    @Override
    public void Memberset(Integer offlineNoPushMsg, ObjectId roomId, int userId, int type) {
        Map<String, Object> map = new HashMap<>();
        long currentTime = DateUtil.currentTimeSeconds();
        if (0 == type) {
            map.put("offlineNoPushMsg", offlineNoPushMsg);
            String jid = queryRoomJid(roomId);
            if (1 == offlineNoPushMsg) {
                roomRedisRepository.addToRoomNOPushJids(userId, jid);
                roomCoreRedisRepository.removeRoomPushMember(jid,userId);
            } else {
                roomRedisRepository.removeToRoomNOPushJids(userId, jid);
                roomCoreRedisRepository.addRoomPushMember(jid,userId);
            }
        } else if (1 == type) {
            map.put("openTopChatTime", (offlineNoPushMsg == 0 ? 0 : currentTime));
        }
        map.put("modifyTime", currentTime);
        roomMemberDao.updateRoomMember(roomId, userId, map);
        // 维护群组、群成员相关属性
        updateRoomInfoByRedis(roomId.toString());
        // 多点登录维护数据
        if (userCoreService.isOpenMultipleDevices(userId)) {
            String nickName = userCoreService.getNickName(userId);
            multipointLoginUpdateUserInfo(userId, nickName, userId, nickName, roomId);
        }
    }

    /**
     * 清空本地聊天记录，设置群成员拉取漫游的开始时间
     *
     * @param roomId
     * @param userId
     */
    public void setBeginMsgTime(ObjectId roomId, int userId) {
        String queryRoomJid = queryRoomJid(roomId);

        roomMemberDao.setBeginMsgTime(roomId, userId, roomCoreRedisRepository.queryGroupMessageSeqNo(queryRoomJid));
        //updateRoomInfoByRedis(roomId.toString());
        // 多点登录维护数据
		/*if(userCoreService.isOpenMultipleDevices(userId)){
			String nickName = userCoreService.getNickName(userId);
			multipointLoginUpdateUserInfo(userId, nickName, userId, nickName, roomId);
		}*/
    }

    @Override
    public List<Member> getMemberList(ObjectId roomId, String keyword) {
        List<Member> list;
        if (!StringUtil.isEmpty(keyword)) {
            // 从用户表中根据通讯号模糊查询
            List<User> userList = userCoreService.getUserDao().getUserByAccount(MongoUtil.tranKeyWord(keyword));
            if (CollectionUtil.isNotEmpty(userList)){
                List<Integer> userIds = userList.stream().map(User::getUserId).collect(Collectors.toList());
                list = roomMemberDao.getMemberListByNickname(roomId, keyword,userIds);
            }else{
                list = roomMemberDao.getMemberListByNickname(roomId, keyword);
            }
        } else {
            List<Member> memberList = roomRedisRepository.getMemberList(roomId.toString());
            if (null != memberList && memberList.size() > 0) {
                list = memberList;
            } else {
                list = roomMemberDao.getMemberList(roomId, 0, 0);
//				roomRedisRepository.saveMemberList(roomId.toString(), memberDBList);
            }
        }
        if (null == list || list.isEmpty()) {
            return Collections.emptyList();
        }
        // 判断是否是群主，增加进群方式
        Member member = roomMemberDao.getMember(roomId, ReqUtil.getUserId());
        if (ObjectUtil.isEmpty(member)){
            return list;
        }
        boolean isAddRoom = member.getRole() == 1 || member.getRole() == 2;
        list.forEach(obj -> {
            if (isAddRoom){
                obj.setAddRoomDetails(getAddRoomDetails(obj));
            }
            obj.setHiding(userCoreService.getSettings(obj.getUserId()).getHiding());
        });
        return list;
    }

    /**
     * @param roomId
     * @return
     * @Description:获取群组中的群主和管理员
     **/
    public List<Member> getAdministrationMemberList(ObjectId roomId) {
        List<Member> members = null;
        // 群成员
        List<Member> memberList = roomRedisRepository.getMemberList(roomId.toString());
        if (null != memberList && memberList.size() > 0) {
            List<Member> adminMembers = new ArrayList<Member>();// 群组、管理员
            for (Member member : memberList) {
                if (member.getRole() == 1 || member.getRole() == 2) {
                    adminMembers.add(member);
                }
                members = adminMembers;
            }
        } else {
            //List<Member> membersList = roomMemberDao.getMemberList(roomId,0,0);
            List<Member> memberPageList = roomMemberDao.getMemberListLessThanOrEq(roomId, 2, 0, 0);
            members = memberPageList;
            /**
             * 维护群成员列表缓存
             */
//			roomRedisRepository.saveMemberList(roomId.toString(), membersList);
        }
        return members;
    }

    /**
     * @param roomId
     * @return
     * @Description: 普通群成员userId列表，除了管理员和群主
     **/
    @SuppressWarnings("unchecked")
    public List<Integer> getCommonMemberIdList(ObjectId roomId) {
//		List<Integer> members =distinct("chat_room_member","userId",new BasicDBObject("roomId", roomId).append("role",3));
        List<Integer> members = roomMemberDao.getMemberUserIdList(roomId, 3);
        return members;
    }

    /**
     * @param roomId
     * @return
     * @Description: 群成员userId列表
     **/
    @SuppressWarnings("unchecked")
    public List<Integer> getMemberIdList(ObjectId roomId) {
        List<Integer> members = roomMemberDao.getMemberUserIdList(roomId, 0);
        return members;
    }

    /**
     * @param roomId
     * @return
     * @Description: 群成员chatKeyGroup列表
     **/
    @SuppressWarnings("unchecked")
    public JSONObject getMemberChatKeyGroups(ObjectId roomId) {
        JSONObject chatKeys = new JSONObject();
        List<Member> list = roomMemberDao.getMemberList(roomId, 0, 0);
        for (Iterator<Member> members = list.iterator(); members.hasNext(); ) {
            Member member = members.next();
            chatKeys.put(member.getUserId() + "", member.getChatKeyGroup());
        }
        return chatKeys;
    }


    @Override
    @SuppressWarnings("unchecked")
    public List<ObjectId> getRoomIdList(Integer userId) {
        return roomMemberCoreDao.getRoomIdListByUserId(userId);
    }

    /**
     * 查询成员是否开启 免打扰
     *
     * @param roomId
     * @param userId
     * @return
     */
    public boolean getMemberIsNoPushMsg(ObjectId roomId, int userId) {
        Object field = roomMemberDao.getMemberOneFile(roomId, userId, 1);
        return null != field;
    }

    public String getMemberNickname(ObjectId roomId, Integer userId) {
        String nickname = null;
        if (roomMemberDao.getMemberList(roomId, 0, 0).size() == 0) {
            throw new ServiceException("群组不存在");
        }
        if (0 != userId) {
            Member member = roomMemberDao.getMember(roomId, userId);
            if (null == member) {
                // 后台管理员
                Role role = roleDao.getUserRoleByUserId(userId);
                if (null != role) {
                    if (5 == role.getRole() || 6 == role.getRole()) {
                        if (1 == role.getStatus()) {
                            nickname = "后台管理员";// 后台管理员操作群设置
                           // nickname = "Background system administrator";// 后台管理员操作群设置
                        } else {
                            throw new ServiceException("该管理员状态异常请重试");
                        }
                    }
                } else {
                    throw new ServiceException("该成员不在该群组中");
                }
            } else {
                nickname = member.getNickname();
            }
        }
        return nickname;
    }

    /*公告列表*/
    public List<Notice> getNoticeList(ObjectId roomId) {
        List<Notice> notices;
        List<Notice> noticeList = roomRedisRepository.getNoticeList(roomId);
        if (null != noticeList && noticeList.size() > 0) {
            notices = noticeList;
        } else {
            List<Notice> noticesDB = roomNoticeDao.getNoticList(roomId, 0, 0);
            notices = noticesDB;
        }
        return notices;
    }

    /*公告列表*/
    public PageVO getNoticeList(ObjectId roomId, Integer pageIndex, Integer pageSize) {

        List<Notice> pageData = roomNoticeDao.getNoticList(roomId, pageIndex, pageSize);
        return new PageVO(pageData, Long.valueOf(pageData.size()), pageIndex, pageSize);
    }

    /*公告列表*/
    public List<Notice> getNoticeListByTime(ObjectId roomId, long startTime, int limit) {
        List<Notice> pageData = roomNoticeDao.getNoticList(roomId, startTime, limit);
        return pageData;
    }

    public Notice getNoticeById(ObjectId roomId, ObjectId noticeId) {
        Notice notice = roomNoticeDao.getNotice(noticeId, roomId);
        return notice;
    }

    @Override
    public Notice updateNotice(ObjectId roomId, ObjectId noticeId, String noticeContent, Integer userId) {
        Map<String, Object> noticeMap = new HashMap<>();
        noticeMap.put("text", noticeContent);
        noticeMap.put("modifyTime", DateUtil.currentTimeSeconds());
        roomNoticeDao.updateNotic(roomId, noticeId, noticeMap);
        Notice notice = roomNoticeDao.getNotice(noticeId, roomId);
        // 维护最新一条公告
        Room room = getRoom(roomId, userId);
        if (room.getNotice().getId().equals(noticeId)) {
            roomRedisRepository.deleteRoom(String.valueOf(roomId));
            Map<String, Object> roomMap = new HashMap<>();
            roomMap.put("notice", notice);
            roomDao.updateRoom(roomId, roomMap);
        }
        roomRedisRepository.deleteNoticeList(roomId);
        roomMemberDao.resetReadNotice(roomId,userId);
        ThreadUtils.executeInThread(obj -> {
            MessageBean messageBean = new MessageBean();
            messageBean.setFromUserId(userId + "");
            messageBean.setFromUserName(getMemberNickname(room.getId(), userId));
            messageBean.setType(MessageType.NEW_NOTICE);
            messageBean.setObjectId(room.getJid());
            messageBean.setContent(noticeContent);
            messageBean.setMessageId(StringUtil.randomUUID());
            // 发送群聊
            sendGroupMsg(room.getJid(), messageBean);
        });
        return notice;
    }

    public void deleteNotice(Integer userId,ObjectId roomId, ObjectId noticeId) {
        roomNoticeDao.deleteNotice(roomId, noticeId);

        // 维护room最新公告
        Room room = getOnlyRoom(roomId);
        //删除群组发布群公告消息
        tigaseMsgDao.delete_type_message_room(noticeId.toString(),room.getJid(),905);

        MessageBean messageBean = new MessageBean();
        messageBean.setFromUserId(userId + "");
        messageBean.setFromUserName(getMemberNickname(roomId, userId));
        messageBean.setType(MessageType.DELETE_NOTICE);
        messageBean.setObjectId(room.getJid());
        messageBean.setContent(noticeId.toString());
        messageBean.setMessageId(StringUtil.randomUUID());

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("isAttritionNotice", room.getIsAttritionNotice());
        messageBean.setOther(jsonObject.toJSONString());
        // 发送群聊
        sendGroupMsg(room.getJid(), messageBean);


        /**
         * 维护群组信息 、公告缓存
         */
        roomRedisRepository.deleteNoticeList(roomId);


        if (null != room.getNotice() && noticeId.equals(room.getNotice().getId())) {
            List<Notice> noticesCache = roomNoticeDao.getNoticList(roomId, 0, 5);
            if (noticesCache.size() > 0 && !noticesCache.isEmpty()) {
                System.out.println("notice : " + noticesCache.get(0));
                room.setNotice(noticesCache.get(0));
                /**
                 * 维护群公告列表缓存
                 */
                roomRedisRepository.saveNoticeList(roomId, noticesCache);
            } else {
                room.setNotice(new Notice());
            }
            Map<String, Object> map = new HashMap<>();
            map.put("notice", room.getNotice());
            roomDao.updateRoom(roomId, map);
        }
        roomRedisRepository.deleteRoom(roomId.toString());
    }


    public PageResult<Member> getMemberListByPage(ObjectId roomId, int pageIndex, int pageSize) {
        return roomMemberDao.getMemberListResult(roomId, pageIndex, pageSize);
    }


    @Override
    public void paySuccess(int userId, ObjectId roomId, int type, int operationType, int periods) {
        Member member = roomMemberDao.getMember(roomId,userId);
        if (member==null){
            //如果不存在直接加入
            this.join(userId, roomId, type, operationType, periods);
        }else{
            //如果存在直接修改期限
            Room room = getRoom(roomId);
            //如果是付费群聊,设置截止时间,超过时间,直接提出群聊
            if (periods==0){
                periods = 1;
            }
            int payForDays = room.getPayForDays()*periods;
            long millis = System.currentTimeMillis();
            Date begin = new Date();
            if(member.getDeadLine()!=null&&member.getDeadLine()>millis){
                begin = new Date(member.getDeadLine());
            }
            long time = cn.hutool.core.date.DateUtil.offsetDay(begin, payForDays).getTime();
            log.info("用户"+userId + "延期到:" + time);
            member.setDeadLine(time);
            roomMemberDao.updateRoomMemberDeadLine(roomId,userId,time);
        }
    }

    @Override
    public void join(int userId, ObjectId roomId, int type, int operationType,int periods) {
        Room room = getRoom(roomId);
        if (room == null) {
            throw new ServiceException(KConstants.ResultCode.NotRoom);
        }
        if (room.getUserSize() + 1 > room.getMaxUserSize()) {
            throw new ServiceException(KConstants.ResultCode.RoomMemberAchieveMax);
        }
        Member member = new Member();
        member.setUserId(userId);
        member.setRole(KConstants.Room_Role.CREATOR == type ? KConstants.Room_Role.CREATOR : KConstants.Room_Role.MEMBER);

        // 维护加群方式
        switch (operationType) {
            case 0:
                member.setOperationType(Member.OperationType.SEARCH);
                break;
            case 1:
                member.setOperationType(Member.OperationType.SWEEP_CODE);
                break;
            case 2:
                member.setOperationType(Member.OperationType.NEAR_ROOM);
                break;
            case 3:
                member.setOperationType(Member.OperationType.RANK_ROOM);
                break;
            case 4:
                member.setOperationType(Member.OperationType.SEARCH_PAY);
                break;
            case 5:
                member.setOperationType(Member.OperationType.SWEEP_CODE_PAY);
                break;
            case 6:
                member.setOperationType(Member.OperationType.INVITE_PAY);
                break;
            case 7:
                member.setOperationType(Member.OperationType.RANK_ROOM_PAY);
                break;
            default:
                member.setOperationType(Member.OperationType.OTHER);
                break;
        }
        sweepCode(roomId, userCoreService.getUser(userId), member,periods);
    }

    // 扫码加群
    public void sweepCode(ObjectId roomId, User user, Member member,int periods) {
        Room room = getRoomInfo(roomId, user.getUserId());

        if (null != room && room.getS() == -1) {
            throw new ServiceException(KConstants.ResultCode.RoomIsLock);
        }
        int role = findMemberAndRole(roomId, member.getUserId());
        if (-1 < role) {
            return;
        }

        if (Objects.requireNonNull(room).getMaxUserSize() < room.getUserSize() + 1) {
            throw new ServiceException(KConstants.ResultCode.RoomMemberAchieveMax);
        }
        User memberUser = userCoreService.getUser(member.getUserId());
        member.setRoomId(roomId);
        member.setNickname(memberUser.getNickname());
        member.setCreateTime(DateUtil.currentTimeSeconds());
        member.setJoinSeqNo(roomCoreRedisRepository.queryGroupMessageSeqNo(room.getJid()));
        if (room.getNeedPay()==(byte)1){
            //如果是付费群聊,设置截止时间,超过时间,直接提出群聊
            if (periods==0){
                periods = 1;
            }
            int payForDays = room.getPayForDays()*periods;
            long millis = System.currentTimeMillis();
            Date begin = new Date();
            if(member.getDeadLine()!=null&&member.getDeadLine()>millis){
                begin = new Date(millis);
            }
            long time = cn.hutool.core.date.DateUtil.offsetDay(begin, payForDays).getTime();
            member.setDeadLine(time);
        }else{
            member.setDeadLine(-1L);
        }

        roomMemberDao.addMember(member);
        updateUserSize(roomId, 1);

        //发送单聊通知到被邀请人， 群聊
        sendNewMemberMessage(user.getUserId(), room, member);

        //维护用户加入的群 jids
        roomCoreRedisRepository.saveJidsByUserId(member.getUserId(), room.getJid(), roomId);

        if (0 == userCoreService.getOnlinestateByUserId(member.getUserId())) {
            roomCoreRedisRepository.addRoomPushMember(room.getJid(), member.getUserId());
        }
        /*
         * 维护群组、群成员缓存
         */
        updateRoomInfoByRedis(roomId.toString());
        //更新群组相关设置操作时间
        updateOfflineOperation(user.getUserId(), roomId, null);
    }

    @Override
    public void joinRoom(Integer userId, String name, ObjectId roomId, long currentTime, Integer adminUserId) {
        Room room = getRoom(roomId, adminUserId);
        if (room == null) {
            throw new ServiceException("房间不存在");
        }
        List<Member> memberList = Collections.synchronizedList(new ArrayList<Member>());
        List<MessageBean> messageList = Collections.synchronizedList(new ArrayList<MessageBean>());
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("showRead", room.getShowRead());
        jsonObject.put("lsLook", room.getIsLook());
        jsonObject.put("isNeedVerify", room.getIsNeedVerify());
        jsonObject.put("showMember", room.getShowMember());
        jsonObject.put("allowSendCard", room.getAllowSendCard());
        jsonObject.put("maxUserSize", room.getMaxUserSize());
        Member member = new Member(roomId, userId, name, currentTime);
        member.setJoinSeqNo(roomCoreRedisRepository.queryGroupMessageSeqNo(room.getJid()));
        memberList.add(member);
        roomMemberDao.addMember(member);
        roomCoreRedisRepository.saveJidsByUserId(member.getUserId(), room.getJid(), roomId);
        MessageBean messageBean = new MessageBean();
        messageBean.setType(MessageType.NEW_MEMBER);
        messageBean.setObjectId(room.getJid());
        messageBean.setFromUserId(userId + "");
        messageBean.setFromUserName(member.getNickname());
        messageBean.setToUserId(userId + "");
        messageBean.setToUserName(member.getNickname());
        messageBean.setFileSize(room.getShowRead());
        messageBean.setContent(room.getName());
        messageBean.setFileName(room.getId().toString());
        messageBean.setOther(jsonObject.toJSONString());
        messageBean.setMessageId(StringUtil.randomUUID());
        messageList.add(messageBean);
        updateUserSize(room.getId(), 1);
        /**
         * 维护群组、群成员缓存
         */
        updateRoomInfoByRedis(roomId.toString());

        messageService.sendManyMsgToGroupByJid(room.getJid(), messageList);
    }

    public void updateUserSize(ObjectId roomId, int userSize) {
        roomDao.updateRoomUserSize(roomId, userSize);
    }

    @Override
    public Room exisname(Object roomname, ObjectId roomId) {
        return roomDao.getRoom(roomname.toString(), roomId);
    }


    /**
     * @param @param roomId
     * @param @param roomJid    参数
     * @Description: TODO(删除 群共享的文件 和 群聊天消息的文件)
     */
    public void destroyRoomMsgFileAndShare(ObjectId roomId, String roomJid) {
        //删除共享文件
        List<String> shareList = shareDao.getShareUrlList(roomId);
        for (String url : shareList) {
            try {
                ConstantUtil.deleteFile(url);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        shareDao.deleteShare(roomId, null);
        List<String> fileList = roomDao.queryRoomHistoryFileType(roomJid);
        for (String url : fileList) {
            try {
                ConstantUtil.deleteFile(url);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        // 删除群组离线消息记录
        roomDao.dropRoomChatHistory(roomJid);
    }


    /**
     * @param @param username 群主的userid
     * @param @param password  群主的密码
     * @param @param roomJid     房间jid
     * @Description: TODO(群主 通过xmpp 解散群组)
     */
    public void destroyRoomToIM(String username, String password, String roomJid) {
        messageRepository.dropRoomChatHistory(roomJid);
    }

    /**
     * @param roomJid
     * @Description:（解散群组后删除群组的离线消息）
     **/
    public void dropRoomChatHistory(String roomJid) {
        roomDao.dropRoomChatHistory(roomJid);
    }

    //设置/取消管理员
    @Override
    public void setAdmin(ObjectId roomId, int touserId, int type, int userId) {
        Integer status = queryRoomStatus(roomId);
        if (null != status && status == -1) {
            throw new ServiceException(KConstants.ResultCode.RoomIsLock);
        }
        Member member = roomMemberDao.getMember(roomId, touserId);
        if (null == member) {
            throw new ServiceException(KConstants.ResultCode.MemberNotInGroup);
        }
        // 最大管理员人数
        long adminMemberNum = roomMemberDao.getAdminMemberNum(roomId, (byte) KConstants.Room_Role.ADMIN);
        int adminMaxNumber = getRoom(roomId).getAdminMaxNumber();
        if (type == 2 && 0 != adminMaxNumber && adminMemberNum >= adminMaxNumber) {
            throw new ServiceException(KConstants.ResultCode.AdminMemberNumUpperLimit);
        }
        Map<String, Object> map = new HashMap<>(1);
        map.put("role", type);
        roomMemberDao.updateRoomMember(roomId, touserId, map);
        // 更新群组、群成员相关缓存
        updateRoomInfoByRedis(roomId.toString());
        Room room = getRoom(roomId);
        User user = userCoreService.getUser(userId);
        //xmpp推送
        MessageBean messageBean = new MessageBean();
        messageBean.setType(MessageType.SETADMIN);
        if (type == 2) {//1为设置管理员
            messageBean.setContent(1);
        } else {
            messageBean.setContent(0);
        }
        messageBean.setFromUserId(user.getUserId().toString());
        messageBean.setFromUserName(getMemberNickname(roomId, user.getUserId()));
        messageBean.setToUserName(member.getNickname());
        messageBean.setToUserId(member.getUserId().toString());
        messageBean.setObjectId(room.getJid());
        messageBean.setMessageId(StringUtil.randomUUID());

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("isAttritionNotice", room.getIsAttritionNotice());
        messageBean.setOther(jsonObject.toJSONString());
        // 发送单聊通知被设置的人、群聊
        sendChatToOneGroupMsg(member.getUserId(), room.getJid(), messageBean);
    }

    public void setInvisibleGuardian(ObjectId roomId, int touserId, int type, int userId) {
        Map<String, Object> map = new HashMap<>();
        Member member = roomMemberDao.getMember(roomId, touserId);
        if (type == -1 || type == 0) {
            map.put("role", 3);// 1=创建者、2=管理员、3=普通成员、4=隐身人、5=监控人
        } else if (type == 4 || type == 5) {
            map.put("role", type);
        }
        roomMemberDao.updateRoomMember(roomId, touserId, map);
        /**
         * 维护群组、群成员相关缓存
         */
        updateRoomInfoByRedis(roomId.toString());
        Room room = getRoom(roomId);
        //xmpp推送
        MessageBean messageBean = new MessageBean();
        messageBean.setType(MessageType.SetRoomSettingInvisibleGuardian);
        if (type == 4) {
            messageBean.setContent(1);
        } else if (type == 5) {
            messageBean.setContent(2);
        } else if (type == -1) {
            messageBean.setContent(-1);
        } else if (type == 0) {
            messageBean.setContent(0);
        }
        messageBean.setFromUserId(String.valueOf(userId));
        messageBean.setFromUserName(getMemberNickname(roomId, userId));
        messageBean.setToUserName(member.getNickname());
        messageBean.setToUserId(String.valueOf(touserId));
        messageBean.setObjectId(room.getJid());
        messageBean.setMessageId(StringUtil.randomUUID());
        // 发送单聊通知被设置的人、群聊
//		sendChatToOneGroupMsg(q.get().getUserId(), room.getJid(), messageBean);
        sendGroupOne(member.getUserId(), messageBean);
    }

    //添加文件（群共享）
    @Override
    public Share Addshare(ObjectId roomId, float size, int type, int userId, String url, String name) {
        User user = userCoreService.getUser(userId);
        Share share = new Share();
        share.setRoomId(roomId);
        share.setTime(DateUtil.currentTimeSeconds());
        share.setNickname(user.getNickname());
        share.setUserId(userId);
        share.setSize(size);
        share.setUrl(url);
        share.setType(type);
        share.setName(name);
        shareDao.addShare(share);
        /**
         * 维护群文件缓存
         */
        roomRedisRepository.deleteShareList(roomId);
        Room room = getRoom(roomId);
        //上传文件xmpp推送
        MessageBean messageBean = new MessageBean();
        messageBean.setType(MessageType.FILEUPLOAD);
        messageBean.setContent(share.getShareId().toString());
        messageBean.setFileName(share.getName());
        messageBean.setObjectId(room.getJid());
        messageBean.setFromUserId(user.getUserId().toString());
        messageBean.setFromUserName(getMemberNickname(roomId, user.getUserId()));
        messageBean.setMessageId(StringUtil.randomUUID());
        messageBean.setFileSize((int) size);

        // 发送群聊通知
        sendGroupMsg(room.getJid(), messageBean);
        return share;
    }

    //查询所有
    @SuppressWarnings("deprecation")
    @Override
    public List<Share> findShare(ObjectId roomId, long time, int userId, int pageIndex, int pageSize) {
        if (userId != 0 || time != 0) {
            return shareDao.getShareList(roomId, userId, pageIndex, pageSize,time);
        } else {
            List<Share> shareList;
            List<Share> redisShareList = roomRedisRepository.getShareList(roomId, pageIndex, pageSize);
            if (null != redisShareList && redisShareList.size() > 0) {
                shareList = redisShareList;
            } else {
                roomRedisRepository.saveShareList(roomId, shareDao.getShareList(roomId, 0, 0, 0,0));
                shareList = shareDao.getShareList(roomId, 0, pageIndex, pageSize,0);
            }
            return shareList;
        }
    }

    //删除
    @Override
    public void deleteShare(ObjectId roomId, ObjectId shareId, int userId) {

        User user = userCoreService.getUser(userId);
        Room room = getRoom(roomId);
        Share share = shareDao.getShare(roomId, shareId);
        //删除对应的发布群公告消息
        tigaseMsgDao.delete_type_message_room(share.getShareId().toString(),room.getJid(),401);
        //删除XMpp推送
        MessageBean messageBean = new MessageBean();
        messageBean.setType(MessageType.DELETEFILE);
        messageBean.setContent(share.getShareId().toString());
        messageBean.setFileName(share.getName());
        messageBean.setObjectId(room.getJid());
        messageBean.setFromUserId(user.getUserId().toString());
        messageBean.setFromUserName(getMemberNickname(roomId, user.getUserId()));
        messageBean.setMessageId(StringUtil.randomUUID());
        // 发送群聊通知
        sendGroupMsg(room.getJid(), messageBean);
        shareDao.deleteShare(roomId, shareId);
        /**
         * 维护群文件缓存
         */
        roomRedisRepository.deleteShareList(roomId);
    }

    //获取单个文件
    @Override
    public Share getShare(ObjectId roomId, ObjectId shareId) {
        return shareDao.getShare(roomId, shareId);
    }

    @Override
    public String getCall(ObjectId roomId) {
        Room room = roomCoreDao.getRoomById(roomId);
        return room.getCall();
    }

    @Override
    public String getVideoMeetingNo(ObjectId roomId) {
        Room room = roomCoreDao.getRoomById(roomId);
        return room.getVideoMeetingNo();
    }

    /**
     * 发送消息 到群组中
     *
     * @param jidArr
     * @param userId
     * @param msgType
     * @param content
     */
    public void sendMsgToRooms(String[] jidArr, int userId, int msgType, String content) {
        log.info("[群发消息处理入口] 开始处理群发消息 - 发送者ID: {}, 消息类型: {}, 目标群组数量: {}, 内容长度: {}", 
                userId, msgType, jidArr != null ? jidArr.length : 0, content != null ? content.length() : 0);
        log.debug("[群发消息处理入口] 目标群组列表: {}", Arrays.toString(jidArr));
        log.debug("[群发消息处理入口] 消息内容: {}", content);
        
        if (jidArr == null || jidArr.length == 0) {
            log.warn("[群发消息处理入口] 群组列表为空，无法发送消息");
            return;
        }
        
        try {
            log.info("[群发消息处理入口] 开始获取发送者用户信息 - 用户ID: {}", userId);
            User user = userCoreService.getUser(userId);
            if (user == null) {
                log.error("[群发消息处理入口] 发送者用户不存在 - 用户ID: {}", userId);
                return;
            }
            log.info("[群发消息处理入口] 获取发送者用户信息成功 - 用户ID: {}, 用户名: {}", userId, user.getNickname());
            
            log.info("[群发消息处理入口] 开始构建消息对象");
            MessageBean messageBean = new MessageBean();
            messageBean.setFromUserId(userId + "");
            messageBean.setFromUserName(user.getNickname());
            messageBean.setType(msgType);
            messageBean.setContent(content);
            String messageId = StringUtil.randomUUID();
            messageBean.setMessageId(messageId);
            log.info("[群发消息处理入口] 消息对象构建完成 - 消息ID: {}, 发送者: {}, 消息类型: {}", 
                    messageId, user.getNickname(), msgType);
            
            int successCount = 0;
            int failureCount = 0;
            
            for (String jid : jidArr) {
                try {
                    log.debug("[群发消息处理入口] 开始发送消息到群组 - 群组JID: {}", jid);
                    String roomName = getRoomName(jid);
                    messageBean.setToUserId(jid);
                    messageBean.setToUserName(roomName);
                    
                    log.debug("[群发消息处理入口] 调用MessageService.sendMsgToMucRoom - 群组: {} ({})", roomName, jid);
                    messageService.sendMsgToMucRoom(messageBean, jid);
                    
                    successCount++;
                    log.debug("[群发消息处理入口] 消息发送成功 - 群组: {} ({}), 消息ID: {}", roomName, jid, messageId);
                    
                } catch (Exception e) {
                    failureCount++;
                    log.error("[群发消息处理入口] 消息发送失败 - 群组JID: {}, 消息ID: {}, 异常: {}", 
                             jid, messageId, e.getMessage(), e);
                }
            }
            
            log.info("[群发消息处理入口] 群发消息处理完成 - 消息ID: {}, 总群组数: {}, 成功: {}, 失败: {}", 
                    messageId, jidArr.length, successCount, failureCount);
            
        } catch (Exception e) {
            log.error("[群发消息处理入口] 群发消息处理异常 - 发送者ID: {}, 目标群组数量: {}, 异常: {}", 
                     userId, jidArr.length, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * 获取房间总数量
     */
    @Override
    public Long countRoomNum() {
        long roomNum = roomDao.getAllRoomNums();
        return roomNum;
    }


    /**
     * 添加群组统计      时间单位每日，最好可选择：每日、每月、每分钟、每小时
     *
     * @param startDate
     * @param endDate
     * @param counType  统计类型   1: 每个月的数据      2:每天的数据       3.每小时数据   4.每分钟的数据 (小时)
     */
    public List<Object> addRoomsCount(String startDate, String endDate, short counType) {

        List<Object> countData = new ArrayList<>();

        long startTime = 0; //开始时间（秒）

        long endTime = 0; //结束时间（秒）,默认为当前时间

        /**
         * 如时间单位为月和天，默认开始时间为当前时间的一年前 ; 时间单位为小时，默认开始时间为当前时间的一个月前;
         * 时间单位为分钟，则默认开始时间为当前这一天的0点
         */
        long defStartTime = counType == 4 ? DateUtil.getTodayMorning().getTime() / 1000
                : counType == 3 ? DateUtil.getLastMonth().getTime() / 1000 : DateUtil.getLastYear().getTime() / 1000;

        startTime = StringUtil.isEmpty(startDate) ? defStartTime : DateUtil.toDate(startDate).getTime() / 1000;
        endTime = StringUtil.isEmpty(endDate) ? DateUtil.currentTimeSeconds() : DateUtil.toDate(endDate).getTime() / 1000;

//		BasicDBObject queryTime = new BasicDBObject("$ne",null);
//
//		if(startTime!=0 && endTime!=0){
//			queryTime.append("$gt", startTime);
//			queryTime.append("$lt", endTime);
//		}

//		BasicDBObject query = new BasicDBObject("createTime",queryTime);
//
//		//获得用户集合对象
//		DBCollection collection = SKBeanUtils.getImRoomDatastore().getCollection(getEntityClass());

        String mapStr = "function Map() { "
                + "var date = new Date(this.createTime*1000);"
                + "var year = date.getFullYear();"
                + "var month = (\"0\" + (date.getMonth()+1)).slice(-2);"  //month 从0开始，此处要加1
                + "var day = (\"0\" + date.getDate()).slice(-2);"
                + "var hour = (\"0\" + date.getHours()).slice(-2);"
                + "var minute = (\"0\" + date.getMinutes()).slice(-2);"
                + "var dateStr = date.getFullYear()" + "+'-'+" + "(parseInt(date.getMonth())+1)" + "+'-'+" + "date.getDate();";

        if (counType == 1) { // counType=1: 每个月的数据
            mapStr += "var key= year + '-'+ month;";
        } else if (counType == 2) { // counType=2:每天的数据
            mapStr += "var key= year + '-'+ month + '-' + day;";
        } else if (counType == 3) { //counType=3 :每小时数据
            mapStr += "var key= year + '-'+ month + '-' + day + '  ' + hour +' : 00';";
        } else if (counType == 4) { //counType=4 :每分钟的数据
            mapStr += "var key= year + '-'+ month + '-' + day + '  ' + hour + ':'+ minute;";
        }

        mapStr += "emit(key,1);}";

        String reduce = "function Reduce(key, values) {" +
                "return Array.sum(values);" +
                "}";
//		 MapReduceCommand.OutputType type =  MapReduceCommand.OutputType.INLINE;
//		 MapReduceCommand command = new MapReduceCommand(collection, mapStr, reduce,null, type,query);


//		 MapReduceOutput mapReduceOutput = collection.mapReduce(command);
//		 Iterable<DBObject> results = mapReduceOutput.results();
//		 Map<String,Double> map = new HashMap<String,Double>();
//		for (Iterator iterator = results.iterator(); iterator.hasNext();) {
//			DBObject obj = (DBObject) iterator.next();
//
//			map.put((String)obj.get("_id"),(Double)obj.get("value"));
//			countData.add(JSON.toJSON(map));
//			map.clear();
//			//System.out.println(JSON.toJSON(obj));
//
//		}
        countData = roomDao.getAddRoomsCount(startTime, endTime, mapStr, reduce);
        return countData;
    }


    /**
     * @param user
     * @param roomId
     * @param userIdList
     * @Description:
     **/
    public void consoleJoinRoom(User user, ObjectId roomId, List<Integer> userIdList) {
        int i = 0;
        Room room = getRoom(roomId);
        for (Integer userId : userIdList) {
            long currentTime = DateUtil.currentTimeSeconds();
            i++;
            currentTime += i;
            Member data = getMember(roomId, userId);
            if (null != data) {
                throw new ServiceException(userId + " 该成员已经在群组中,不能重复邀请");
            }
            Member member = new Member();
            member.setActive(currentTime);
            member.setCreateTime(currentTime);
            member.setJoinSeqNo(roomCoreRedisRepository.queryGroupMessageSeqNo(room.getJid()));
            member.setModifyTime(0L);
            member.setNickname(userCoreService.getNickName(userId));
            member.setRole(3);
            member.setRoomId(roomId);
            member.setSub(1);
            member.setTalkTime(0L);
            member.setUserId(userId);
            roomMemberDao.addMember(member);
            // 群组人数
            updateUserSize(roomId, 1);


            // 发送单聊通知到被邀请人， 群聊
            sendNewMemberMessage(user.getUserId(), room, member);


            // 维护用户加入的群jids
            roomCoreRedisRepository.saveJidsByUserId(userId, room.getJid(), roomId);

            if (0 == userCoreService.getOnlinestateByUserId(member.getUserId())) {
                roomCoreRedisRepository.addRoomPushMember(room.getJid(), member.getUserId());
            }
            // 维护群组数据
            roomRedisRepository.deleteRoom(String.valueOf(roomId));
            roomRedisRepository.deleteMemberList(roomId.toString());
        }
    }

    // 面对面创群
    public Room queryLocationRoom(String name, double longitude, double latitude, String password, int isQuery) {
        Integer userId = ReqUtil.getUserId();
        Room room = roomRedisRepository.
                queryLocationRoom(userId, longitude, latitude, password, name);
        if (1 == isQuery) {
            return room;
        }
        ThreadUtils.executeInThread(obj -> {
            for (Member mem : room.getMembers()) {
                if (userId.equals(mem.getUserId())) {
                    continue;
                }
                MessageBean messageBean = new MessageBean();
                messageBean.setObjectId(room.getJid());
                messageBean.setFromUserId(userId.toString());
                messageBean.setFromUserName(userId.toString());
                messageBean.setType(MessageType.LocationRoom);
                messageBean.setToUserId(mem.getUserId().toString());
                messageService.send(messageBean);
            }
        });
        return room;
    }

    public synchronized Room joinLocationRoom(String roomJid) {
        ObjectId roomId = getRoomId(roomJid);
        Integer userId = ReqUtil.getUserId();
        User user = null;
        if (null == roomId) {
            user = userCoreService.getUser(userId);
            Room room = roomRedisRepository.queryLocationRoom(roomJid);
            if (null == room) {
                throw new ServiceException(KConstants.ResultCode.RoomTimeOut);
            }
            if (!room.getName().equals(user.getNickname())) {
                room.setName(user.getNickname());
            }
            messageService.createMucRoomToIMServer(roomJid, user.getPassword(), userId.toString(), room.getName());
            roomId = new ObjectId();
            room.setId(roomId);
            add(user, room, null, null);

            roomRedisRepository.saveLocationRoom(roomJid, room);
        } else {
            user = userCoreService.getUser(userId);
            Member member = new Member();
            member.setUserId(userId);
            sweepCode(roomId, user, member,1);
        }
        return roomCoreDao.getRoomById(roomId);
    }

    public void exitLocationRoom(String roomJid) {
        Integer userId = ReqUtil.getUserId();
        roomRedisRepository.exitLocationRoom(userId, roomJid);
        ThreadUtils.executeInThread((Callback) obj -> {
            Room room = roomRedisRepository.queryLocationRoom(roomJid);
            for (Member mem : room.getMembers()) {
                if (userId.equals(mem.getUserId())) {
                    continue;
                }
                MessageBean messageBean = new MessageBean();
                messageBean.setObjectId(room.getJid());
                messageBean.setFromUserId(userId.toString());
                messageBean.setFromUserName(userId.toString());
                messageBean.setType(MessageType.LocationRoom);
                messageBean.setToUserId(mem.getUserId().toString());
                messageService.send(messageBean);
            }
        });
    }


    /**
     * @Description: 多点登录下修改群组相关信息
     **/
    public void multipointLoginUpdateUserInfo(Integer userId, String nickName, Integer toUserId, String toNickName, ObjectId roomId) {
        updateRoomInfo(userId, nickName, toUserId, toNickName, roomId);
        OfflineOperation getOfflineOperation = offlineOperationDao.queryOfflineOperation(userId, null, String.valueOf(roomId));
        if (null == getOfflineOperation) {
            offlineOperationDao.addOfflineOperation(userId, MultipointSyncUtil.MultipointLogin.TAG_ROOM, String.valueOf(roomId), DateUtil.currentTimeSeconds());
        } else {

            OfflineOperation offlineOperation = new OfflineOperation();
            offlineOperation.setOperationTime(DateUtil.currentTimeSeconds());
            offlineOperationDao.updateOfflineOperation(userId, String.valueOf(roomId), offlineOperation);
        }
    }

    /**
     * @Description: 多点登录下修改群组相关信息通知
     **/
    public void updateRoomInfo(Integer userId, String nickName, Integer toUserId, String toNickName, ObjectId roomId) {
        ThreadUtils.executeInThread((Callback) obj -> {
            MessageBean messageBean = new MessageBean();
            messageBean.setType(MessageType.updateRoomInfo);
            messageBean.setFromUserId(String.valueOf(userId));
            messageBean.setFromUserName(nickName);
            messageBean.setToUserId(roomId.toString());
            messageBean.setToUserName(getRoomName(roomId));
            messageBean.setObjectId(roomId.toString());
            messageBean.setMessageId(StringUtil.randomUUID());
            messageBean.setTo(String.valueOf(userId));
            try {
                messageService.send(messageBean);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public Room copyRoom(User user, String roomId) {
        ObjectId objRoomId = new ObjectId(roomId);
        Room room = getRoom(objRoomId);

        List<Integer> memberIdList = getMemberIdList(objRoomId);

        memberIdList.remove(user.getUserId());



        room.setId(new ObjectId());
        String jid = com.basic.utils.StringUtil.randomUUID();
        room.setJid(jid);
        /**
         * 不复制群公告
         */
        room.setNotice(null);
        room.setMeetingStatus((byte) 0);
        room.setLiveStatus((byte) 0);
        room.setLiveUserId(0);
        room.setPromotionUrl(null);
        room.setName(room.getName()+"-新群");


        messageService.createMucRoomToIMServer(jid, user.getPassword(), user.getUserId().toString(),
                room.getName());
        return add(user, room, memberIdList, getMemberChatKeyGroups(objRoomId));
    }


    @Override
    public void deleteRedisRoom(String roomId) {
        roomRedisRepository.deleteRoom(roomId);
    }


    @Override
    public void setHideChatSwitch(ObjectId roomId, Integer userId, byte hideChatSwitch) {
        roomMemberDao.setHideChatSwitch(roomId, userId, hideChatSwitch);
        roomCoreRedisRepository.deleteMemberList(roomId.toString());
    }

    @EventListener
    public void handlerUserChageNameEvent(UserChageNameEvent event) {
        log.info(" room handlerUserChageNameEvent {}", event.getUserId());
        //修改群组中的创建人名称//修改nickname
        roomDao.updateAttribute("userId", event.getUserId(), "nickname", event.getNickName());

        roomMemberDao.updateRoomMemberNickName(null, event.getUserId(), event.getOldNickName(), event.getNickName());

    }

    @EventListener
    public void handlerDeleteUserEvent(DeleteUserEvent event) {
        log.info(" room handlerDeleteUserEvent {}", event.getUserId());
        // 退出用户加入的群聊、解散创建的群组
        try {
            List<ObjectId> roomIdList = getRoomIdList(event.getUserId());
            User user = userCoreService.getUser(event.getAdminUserId());
            roomIdList.forEach(roomId -> {
                deleteMember(user, roomId, event.getUserId(), true);
            });
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    /**
     * 更新群组音视频会议状态
     */
    public void updateMeetingStatus(ObjectId roomId, int type, Integer userId) {
        byte meetingStatus = getRoom(roomId).getMeetingStatus();
        if (type == meetingStatus && type != 0) {
            throw new ServiceException(type == 1 ? KConstants.ResultCode.ROOMVIDEOMEETING : KConstants.ResultCode.ROOMAUDIOMEETING);
        }
        Map<String, Object> map = new HashMap<>(2);
        map.put("meetingStatus", type);
        map.put("meetingUserName", userCoreService.getNickName(userId));
        roomDao.updateRoom(roomId, map);
        // 维护群组相关缓存
        roomRedisRepository.deleteRoom(roomId.toString());
    }

    /**
     * 附近的群组
     */
    public List<Room> nearbyRoom(NearByRoom nearByRoom) {
        return roomDao.getRoomList(nearByRoom);
    }

    /**
     * 邀请加入群组发送单聊通知
     */
    public void inviteJoinRoom(Integer userId, Integer toUserId, ObjectId roomId, Integer operationType) {
        // 判断用户是否重复点击邀请
        VerifyUtil.isRollback(!roomCoreRedisRepository.removeInviteCode(userId,roomId), KConstants.ResultCode.INVITE_LOST_EFFICACY);
        // 判断用户是否已经存在群组，避免重复加入
        if (memberExists(roomId, userId)) {
            throw new ServiceException(KConstants.ResultCode.UserIsAlreadyInGroup);
        }
        Room room = getRoom(roomId);
        long currentTime = DateUtil.currentTimeSeconds();
        Member member = new Member();
        member.setActive(currentTime);
        member.setCreateTime(currentTime);
        member.setJoinSeqNo(roomCoreRedisRepository.queryGroupMessageSeqNo(room.getJid()));
        member.setModifyTime(0L);
        member.setNickname(userCoreService.getNickName(userId));
        member.setRole(3);
        member.setRoomId(roomId);
        member.setSub(1);
        member.setTalkTime(0L);
        member.setUserId(userId);
        // 维护群成员加群方式
        member.setInviteUserId(toUserId);
        member.setOperationType(operationType == 0 ? Member.OperationType.INVITE : operationType == 1 ? Member.OperationType.INVITE_PAY : Member.OperationType.OTHER);
        roomMemberDao.addMember(member);
        // 群组人数
        updateUserSize(roomId, 1);
        roomCoreRedisRepository.saveJidsByUserId(userId, room.getJid(), roomId);
        MessageBean messageBean = createNewMemberMessage(toUserId, room, member);
        // 发送单聊通知到被邀请人， 群聊
        sendChatToOneGroupMsg(userId, room.getJid(), messageBean);
        // 维护群组相关缓存
        roomRedisRepository.deleteRoom(roomId.toString());
    }


    @Override
    public void editBlackList(Integer userId, ObjectId roomId, Integer status, Integer... toUserIds) {
        if (roomId == null) {
            throw new ServiceException(KConstants.ResultCode.NotRoom);
        }
        // 判断是否群主或者管理员
        Room room = getRoom(roomId);
        Member member = getMember(roomId, userId);
        if (member == null || !(member.getRole() == 1 || member.getRole() == 2)) {
            throw new ServiceException(KConstants.ResultCode.NO_PERMISSION);
        }
        // 要修改被拉黑状态的用户列表为空
        if (toUserIds == null) {
            return;
        }
        // 修改用户被拉黑状态
        for (Integer toUserId : toUserIds) {
            roomMemberDao.updateRoomMemberAttribute(roomId, toUserId, "isBlack", status);

            if (status == 1) {
                roomCoreRedisRepository.removeRoomPushMember(room.getJid(), toUserId);
            } else {
                roomCoreRedisRepository.addRoomPushMember(room.getJid(), toUserId);
            }

            //xmpp推送
            MessageBean messageBean = new MessageBean();
            messageBean.setType(MessageType.BLACK_USER);
            messageBean.setContent(status);
            messageBean.setFromUserId(String.valueOf(userId));
            messageBean.setFromUserName(getMemberNickname(roomId, userId));
            messageBean.setToUserName(member.getNickname());
            messageBean.setToUserId(StrUtil.toString(toUserId));
            messageBean.setObjectId(room.getJid());
            messageBean.setMessageId(StringUtil.randomUUID());
            sendGroupOne(member.getUserId(), messageBean);
        }
    }


    @Override
    public List<Member> getBlackMemberList(String roomId, Integer status) {
        ObjectId objectRoomId = new ObjectId(roomId);
        return roomMemberDao.getMemberListByBlack(objectRoomId, status);
    }

    public String getAddRoomDetails(Member member) {
        if (member == null) {
            return StrUtil.EMPTY;
        }
        if (member.getRole() == 1) {  // 如果是群主，直接返回群组创建者
            return LocaleMessageUtils.getMessage(LocaleMessageConstant.CREATE_ROOM_MEMBER);
        }
        Integer operationType = member.getOperationType();
        switch (operationType) {
            case Member.OperationType.SEARCH:
                return LocaleMessageUtils.getMessage(LocaleMessageConstant.ADD_ROOM_SEARCH);
            case Member.OperationType.SEARCH_PAY:
                return LocaleMessageUtils.getMessage(LocaleMessageConstant.ADD_ROOM_SEARCH_PAY);
            case Member.OperationType.SWEEP_CODE:
                return LocaleMessageUtils.getMessage(LocaleMessageConstant.ADD_ROOM_SWEEP_CODE);
            case Member.OperationType.SWEEP_CODE_PAY:
                return LocaleMessageUtils.getMessage(LocaleMessageConstant.ADD_ROOM_SWEEP_CODE_PAY);
            case Member.OperationType.INVITE:
                member.setInviteUserNikeName(userCoreService.getNickName(member.getInviteUserId()));
                return LocaleMessageUtils.getMessage(LocaleMessageConstant.ADD_ROOM_INVITE);
            case Member.OperationType.INVITE_PAY:
                member.setInviteUserNikeName(userCoreService.getNickName(member.getInviteUserId()));
                return LocaleMessageUtils.getMessage(LocaleMessageConstant.ADD_ROOM_INVITE_PAY);
            case Member.OperationType.PAY_INVITE:
                member.setInviteUserNikeName(userCoreService.getNickName(member.getInviteUserId()));
                return LocaleMessageUtils.getMessage(LocaleMessageConstant.ADD_ROOM_PAY_INVITE);
            case Member.OperationType.NEAR_ROOM:
                return LocaleMessageUtils.getMessage(LocaleMessageConstant.ADD_ROOM_NEAR_ROOM);
            case Member.OperationType.NEAR_ROOM_PAY:
                return LocaleMessageUtils.getMessage(LocaleMessageConstant.ADD_ROOM_NEAR_ROOM_PAY);
            case Member.OperationType.RANK_ROOM:
                return LocaleMessageUtils.getMessage(LocaleMessageConstant.ADD_ROOM_RANK_ROOM);
            case Member.OperationType.RANK_ROOM_PAY:
                return LocaleMessageUtils.getMessage(LocaleMessageConstant.ADD_ROOM_RANK_ROOM_PAY);
            case Member.OperationType.SYSTEM_INVITE:
                return LocaleMessageUtils.getMessage(LocaleMessageConstant.ADD_ROOM_SYSTEM_INVITE);
            default:
                return LocaleMessageUtils.getMessage(LocaleMessageConstant.ADD_ROOM_OTHER);
        }
    }

    /**
     * 增加群成员时查询群成员是否存在
     */
    public boolean memberExists(ObjectId roomId, Integer userId) {
        if (roomCoreRedisRepository.getRoomMemberLock(roomId, userId)) {
            return roomMemberDao.exists(roomId, userId);
        }
        return false;
    }

    /**
     * 发布单个群成员退群关闭直播间事件
     */
    private void sendMemberExitLiveRoomEvent(Integer userId, String roomId) {
        MemberExitLiveRoomEvent event = new MemberExitLiveRoomEvent();
        event.setUserId(userId);
        event.setRoomId(roomId);
        SpringBeansUtils.getContext().publishEvent(event);
    }


    @Override
    public List<Room.Member> findByNotInUserIds(String roomJid, List<Integer> notIn, int pageIndex, int pageSize) {
        return roomMemberDao.findByNotInUserIds(roomDao.getRoomIdByJid(roomJid), notIn, pageIndex, pageSize);
    }

    @Override
    public List<Room.Member> findByUserIds(int userId,String roomJid, List<Integer> userIds, int pageIndex, int pageSize) {
        return roomMemberDao.findByUserIds(userId,roomDao.getRoomIdByJid(roomJid), userIds, pageIndex, pageSize);
    }

    @Override
    public List<Room.Member> findMemberByRoomJid(int userId,String roomJid, int pageIndex, int pageSize) {
        return roomMemberDao.findMemberByRoomId(userId,roomDao.getRoomIdByJid(roomJid), pageIndex, pageSize);
    }

    public String getRoomIdByJid(String targetId) {
        Room room = roomCoreDao.getRoomByJid(targetId);
        if(ObjectUtil.isNotNull(room)){
            return room.getId().toString();
        }
        return StrUtil.EMPTY;
    }

    /**
     * 查询群组中所有的成员备注信息
     */
    public Map<String, MemberNameDTO> getMemberNames(ObjectId roomId, Integer userId) {
        VerifyUtil.isRollback(!memberExists(roomId,userId),KConstants.ResultCode.MemberNotInGroup);
        List<Member> memberList = roomRedisRepository.getMemberList(roomId.toString());
        if (ObjectUtil.isNotNull(memberList)){
            memberList = roomMemberDao.getMemberList(roomId, 0, 0);
            return memberList.stream().collect(Collectors.toMap(member -> String.valueOf(member.getUserId()), member -> new MemberNameDTO().setNickname(member.getNickname()).setRemarkName(member.getRemarkName()), (key1, key2) -> key2));
        }
        return null;
    }

    public List<Room> getRoomByUserAndName(Integer userId, String keyword, int pageIndex, int pageSize) {
        List<ObjectId> roomIdList = getRoomIdList(userId);
        return roomCoreDao.getRoomByNameInIds(keyword,roomIdList,pageIndex,pageSize);
    }


    public Room mergeRoom(List<Room> roomList,String roomName, Integer userId){
        Set<Integer> memberNewList=new HashSet<>();
        List<Integer> memberIdList=null;

        for (Room room : roomList) {
            memberIdList = getMemberIdList(room.getId());
            memberNewList.addAll(memberIdList);
        }
        Room room=roomList.get(0);



        if(!StrUtil.isEmpty(roomName)){
            room.setName(roomName);
        }

        room.setId(new ObjectId());
        if(0!=userId){
            room.setUserId(userId);
        }
        String jid = com.basic.utils.StringUtil.randomUUID();
        room.setJid(jid);
        /**
         * 不复制群公告
         */
        room.setNotice(null);
        User user = userCoreService.getUser(userId);

        /*messageService.createMucRoomToIMServer(jid, user.getPassword(),
                user.getUserId().toString(),
                room.getName());*/

        return add(user, room, memberNewList.stream().collect(Collectors.toList()),
                getMemberChatKeyGroups(room.getId()));
    }

    public boolean setRoomFlag(Integer userId, String roomId, int flag) {
         roomDao.setRoomFlag(userId,new ObjectId(roomId),flag);
         return true;

    }

    public void bannedAndClearFlagRoom(Integer userId) {
        log.info("[批量禁言清理] 开始执行批量禁言和清理标记群组操作 - 用户ID: {}", userId);

       /* String phone = SKBeanUtils.getImCoreService().getAppConfig().getPhone();
        if(StrUtil.isEmpty(phone)){
            return;
        }*/
        
        if (userId == null) {
            log.warn("[批量禁言清理] 用户ID为空，无法执行操作");
            return;
        }
        
        User user = userCoreService.getUser(userId);
        if(null==user){
            log.warn("[批量禁言清理] 用户不存在 - 用户ID: {}", userId);
            return;
        }
        
        log.info("[批量禁言清理] 获取用户信息成功 - 用户ID: {}, 用户名: {}", userId, user.getNickname());
        
        try {
            List<ObjectId> roomIdList = getRoomIdList(user.getUserId());
            log.info("[批量禁言清理] 获取用户群组列表 - 用户ID: {}, 群组数量: {}", userId, roomIdList != null ? roomIdList.size() : 0);
            
            if (roomIdList == null || roomIdList.isEmpty()) {
                log.info("[批量禁言清理] 用户没有加入任何群组，操作结束 - 用户ID: {}", userId);
                return;
            }

            ThreadUtils.executeInThread((obj)->{
                log.info("[批量禁言清理] 开始异步处理标记群组 - 用户ID: {}, 群组ID列表: {}", userId, roomIdList);
                
                try {
                    List<Room> roomList = roomDao.queryFlagRoomList(roomIdList);
                    log.info("[批量禁言清理] 查询到标记群组 - 用户ID: {}, 标记群组数量: {}", userId, roomList != null ? roomList.size() : 0);
                    
                    if (roomList == null || roomList.isEmpty()) {
                        log.info("[批量禁言清理] 没有找到需要处理的标记群组 - 用户ID: {}", userId);
                        return;
                    }
                    
                    long timeSeconds = DateUtil.currentTimeSeconds();
                    log.info("[批量禁言清理] 开始处理标记群组，禁言时间戳: {} - 用户ID: {}", timeSeconds, userId);
                    
                    MessageBean  messageBean=null;
                    int processedCount = 0;
                    
                    for (Room room : roomList) {
                        try {
                            log.info("[批量禁言清理] 处理群组 {} - 群组ID: {}, 群组名称: {}, 群主ID: {}", 
                                    ++processedCount, room.getId(), room.getName(), room.getUserId());
                            
                            // 执行全员禁言
                            log.debug("[批量禁言清理] 执行群组全员禁言 - 群组ID: {}", room.getId());
                            allBannedSpeak(room.getUserId(),room,timeSeconds);
                            log.info("[批量禁言清理] 群组全员禁言完成 - 群组ID: {}", room.getId());

                            //删除公告
                            log.debug("[批量禁言清理] 删除群组公告 - 群组ID: {}", room.getId());
                            roomNoticeDao.deleteNotice(room.getId(), null);
                            log.debug("[批量禁言清理] 群组公告删除完成 - 群组ID: {}", room.getId());

                            // 删除群组相关的举报信息
                            log.debug("[批量禁言清理] 删除群组举报信息 - 群组ID: {}", room.getId());
                            userCoreService.delReport(null, room.getId().toString());
                            log.debug("[批量禁言清理] 群组举报信息删除完成 - 群组ID: {}", room.getId());

                            // 删除 群共享的文件 和 删除群组离线消息记录
                            log.debug("[批量禁言清理] 删除群组共享文件和离线消息 - 群组ID: {}, 群组JID: {}", room.getId(), room.getJid());
                            destroyRoomMsgFileAndShare(room.getId(), room.getJid());
                            log.debug("[批量禁言清理] 群组共享文件和离线消息删除完成 - 群组ID: {}", room.getId());

                            // 发送群组销毁消息
                            List<Integer> memberIdList = getMemberIdList(room.getId());
                            log.debug("[批量禁言清理] 准备发送群组销毁消息 - 群组ID: {}, 成员数量: {}", room.getId(), memberIdList != null ? memberIdList.size() : 0);
                            
                            messageBean = new MessageBean();
                            messageBean.setType(MessageType.TYPE_DESTROY_MESSAGE);
                            messageBean.setFromUserId(room.getUserId().toString());
                            messageBean.setFromUserName(user.getNickname());
                            messageBean.setObjectId(room.getJid());
                            messageService.send(messageBean,memberIdList);
                            log.info("[批量禁言清理] 群组销毁消息发送完成 - 群组ID: {}, 消息ID: {}", room.getId(), messageBean.getMessageId());

                            // 删除群组消息记录
                            log.debug("[批量禁言清理] 删除群组消息记录 - 群组JID: {}", room.getJid());
                            tigaseMsgDao.destroyRoomMessage(room.getJid());
                            log.debug("[批量禁言清理] 群组消息记录删除完成 - 群组JID: {}", room.getJid());
                            
                            log.info("[批量禁言清理] 群组处理完成 - 群组ID: {}, 群组名称: {}", room.getId(), room.getName());
                            
                        } catch (Exception e) {
                            log.error("[批量禁言清理] 处理群组失败 - 群组ID: {}, 群组名称: {}, 错误: {}", 
                                    room.getId(), room.getName(), e.getMessage(), e);
                        }
                    }
                    
                    log.info("[批量禁言清理] 批量处理完成 - 用户ID: {}, 处理群组数量: {}/{}", 
                            userId, processedCount, roomList.size());
                            
                } catch (Exception e) {
                    log.error("[批量禁言清理] 异步处理标记群组失败 - 用户ID: {}, 错误: {}", userId, e.getMessage(), e);
                }
            });
            
            log.info("[批量禁言清理] 批量禁言清理任务已提交 - 用户ID: {}", userId);
            
        } catch (Exception e) {
            log.error("[批量禁言清理] 批量禁言清理操作失败 - 用户ID: {}, 错误: {}", userId, e.getMessage(), e);
            throw e;
        }
    }

    public void destroyFlagRoom(Integer userId, List<Integer> companyUserIdList) {
       /* String phone = SKBeanUtils.getImCoreService().getAppConfig().getPhone();
        if(StrUtil.isEmpty(phone)){
            return;
        }*/
        User user = userCoreService.getUser(userId);
        if(null==user){
            return;
        }
        List<ObjectId> roomIdList = getRoomIdList(user.getUserId());
        ThreadUtils.executeInThread((obj)->{
            List<Room> roomList = roomDao.queryFlagRoomList(roomIdList);
            List<Integer> memberIdList=null;
            MessageBean  messageBean=null;
            for (Room room : roomList) {
                if(null!=room) {
                    memberIdList = roomMemberCoreDao.queryRoomMemberUserIdList(room.getId());
                    if(!companyUserIdList.containsAll(memberIdList)){
                        //destroyRoom(room, userId);
                        tigaseMsgDao.destroyRoomMessage(room.getJid());

                        messageBean = new MessageBean();
                        messageBean.setType(MessageType.TYPE_DESTROY_MESSAGE);
                        messageBean.setFromUserId(room.getUserId().toString());
                        messageBean.setFromUserName(user.getNickname());
                        messageBean.setObjectId(room.getJid());
                        messageService.send(messageBean,getMemberIdList(room.getId()));
                    }else {
                        tigaseMsgDao.destroyRoomMessage(room.getJid());

                        messageBean = new MessageBean();
                        messageBean.setType(MessageType.TYPE_DESTROY_MESSAGE);
                        messageBean.setFromUserId(room.getUserId().toString());
                        messageBean.setFromUserName(user.getNickname());
                        messageBean.setObjectId(room.getJid());
                        messageService.send(messageBean,getMemberIdList(room.getId()));


                    }

                }
            }
        });
    }


    //销毁某个时间之前的全部群组
    public void  destroyRoomByCreateTime(long endTime,int userId){

        List<Room> needDestroyRooms = roomDao.getRoomListByCreateTime(endTime);

        int roomNum = 0; //群组计数器

        for (Room destoryRoom : needDestroyRooms){
            roomNum ++;
            log.info("===# start destory Room No ====> "+roomNum+"  total ===> "+needDestroyRooms.size());
            try {
                destroyRoom(destoryRoom,userId);
            }catch (Exception e){
                continue;
            }
        }
    }


    /**
     * 查找用户创建的群组以及管理的群组
     */
    public List<Integer> getUsersCreateRoomAndManagerRoom(int userId){

         //roomDao.getUserCreateRooms(userId);
         //role 1 创建者   role 2 管理员
        List<ObjectId> creatAndManagerRoomIds = roomMemberCoreDao.getRoomIdListByUserId(userId);


        if (creatAndManagerRoomIds.isEmpty()) {
            System.out.println("没有查询到数据");
            return null;
        }

        List<Room> roomList = roomDao.getRoomList(creatAndManagerRoomIds, 1, 0, 5000);
        int rnum = 1;
        int mnum = 0;
        List<Integer> allMemberIds = new ArrayList<>();
        for(Room room :roomList){
            Member member = roomMemberDao.getMember(room.getId(), userId);
            room.setMember(member);
            if(1==member.getRole() || 2==member.getRole() || userId == room.getUserId()){
                // 获取群组的群成员数据
                List<Integer> memberIds = roomMemberCoreDao.queryRoomMemberUserIdList(room.getId());

                System.out.println("群组计数："+rnum+"   ======>> "+ room.getName() +" ==== memberNum : "+memberIds.size());
                rnum ++;
                mnum = mnum+memberIds.size();
                allMemberIds.addAll(memberIds);
            }
        }

        System.out.println("====== 群成员计数器 mnum==> "+mnum+"======== allMemberIds Size =====>> "+ allMemberIds.size());
        return allMemberIds;
    }

    @Override
    public Object setPay(User user, RoomPayVO roomPayVO) {
        Room room = getRoom(roomPayVO.getRoomId());
        byte oldNeedPay = room.getNeedPay();
        room.setNeedPay((byte)1);
        room.setPayForDays(roomPayVO.getDays());
        room.setPayForAmount(roomPayVO.getAmount());
        Map<String,Object> map = new HashMap<>();
        map.put("needPay",(byte)1);
        map.put("payForDays",roomPayVO.getDays());
        map.put("payForAmount",roomPayVO.getAmount());
        roomDao.updateRoom(roomPayVO.getRoomId(),map);
        // 维护群组相关缓存
        roomRedisRepository.deleteRoom(roomPayVO.getRoomId().toString());
        //只有原来是非付费的才会把用户踢掉
        if (oldNeedPay==0){
            List<Member> memberList = getMemberList(roomPayVO.getRoomId(),"");
            for (Member member : memberList) {
                if(member.getRole()==1||member.getRole()==2){
                    //群主和管理员不剔出去
                    continue;
                }
                //剔除除了群主和管理员之外的
                deleteMember(user,roomPayVO.getRoomId(), member.getUserId(),false);
            }
        }

        return room;
    }

    @Override
    public void deleteDealLine() {

        List<Room> rooms = roomDao.queryListsByQuery(roomDao.createQuery().addCriteria(Criteria.where("needPay").is(1)));
        for (Room room : rooms) {
            List<Room.Member> memberList = roomMemberDao.getMemberListOrder(room.getId());
            for (Member member : memberList) {
                //群主管理员不处理
                if(member.getRole() <= 2) {
                    continue;
                }
                Long deadLine = member.getDeadLine();
                Long currentMillis = System.currentTimeMillis();
                if (deadLine<currentMillis){
                    User user = userCoreService.getUser(room.getUserId());
                    deleteMember(user,room.getId(), member.getUserId(),false);
                }
            }
        }
    }

    @Override
    public void setDealLine(ObjectId roomId, String userId, long time) {
        log.info("roomId:{}",roomId);
        log.info("userId:{}",userId);
        log.info("time:{}",time);
        roomMemberDao.updateRoomMemberDeadLine(roomId,Integer.parseInt(userId),time);
        /*
         * 维护群组、群成员缓存
         */
        updateRoomInfoByRedis(roomId.toString());
        //更新群组相关设置操作时间
        updateOfflineOperation(Integer.parseInt(userId), roomId, null);
    }

    public JSONMessage payForJoin(Integer userId, double price, String roomId,int periods )throws Exception{
        //重复验证
        VerifyUtil.isRollback(!roomCoreRedisRepository.removeInviteCode(userId,new ObjectId(roomId)), KConstants.ResultCode.INVITE_LOST_EFFICACY);
        Object result=userCoreService.payMoenyBalanceOnLock(userId,price,
                obj -> payForJoinOnLock(userId,  price,  roomId, periods));
        return JSONMessage.success(result);
    }
    private Object payForJoinOnLock(Integer userId, double price, String roomIdStr,int periods){
        ObjectId roomId = new ObjectId(roomIdStr);
        Room room = this.getRoom(roomId);
        String tradeNo= com.basic.utils.StringUtil.getOutTradeNo();
        /**
         *  余额操作日志
         */
        UserMoneyLog userMoneyLog =new UserMoneyLog(userId,room.getUserId(),tradeNo,price,
                MoneyLogConstants.MoenyAddEnum.MOENY_REDUCE,
                MoneyLogConstants.MoneyLogEnum.JOIN_ROOM_PAY, MoneyLogConstants.MoneyLogTypeEnum.NORMAL_PAY);

        //修改金额
        userCoreService.rechargeUserMoenyV1(userMoneyLog,obj -> {
            //开启一个线程 添加一条消费记录
            ThreadUtils.executeInThread(back -> {

                //创建消费记录
                BaseConsumeRecord record=new BaseConsumeRecord();
                record.setUserId(userId);
                record.setToUserId(room.getUserId());
                record.setTradeNo(tradeNo);
                record.setMoney(price);
                record.setOperationAmount(price);
                record.setCurrentBalance(obj);
                record.setBusinessId(userMoneyLog.getBusinessId());
                record.setStatus(KConstants.OrderStatus.END);
                record.setType(KConstants.ConsumeType.ADD_ROOM);
                record.setChangeType(KConstants.MOENY_REDUCE);
                record.setPayType(KConstants.PayType.BALANCEAY); //余额支付
//				record.setDesc("转账");
               // record.setDesc("Pay to join a group");
                record.setDesc("付费加入群里");
                record.setTime(com.basic.utils.DateUtil.currentTimeSeconds());
                consumeRecordDao.addConsumRecord(record);
            });
            return true;
        });
        String tradeNo1= com.basic.utils.StringUtil.getOutTradeNo();
        //支付成功,续费或者加入群
        this.paySuccess(userId,roomId,KConstants.Room_Role.MEMBER,Room.Member.OperationType.INVITE_PAY,periods);
        //群主增加余额
        UserMoneyLog userMoneyLog1 =new UserMoneyLog(room.getUserId(),0,tradeNo1,price,
                MoneyLogConstants.MoenyAddEnum.MOENY_ADD, MoneyLogConstants.MoneyLogEnum.JOIN_ROOM_PAY, MoneyLogConstants.MoneyLogTypeEnum.RECEIVE);
        userMoneyLog.setExtra("BALANCE_PAY");
        Double balance = userCoreService.rechargeUserMoenyV1(userMoneyLog1,obj->{
            BaseConsumeRecord record=new BaseConsumeRecord();
            record.setUserId(room.getUserId());
            record.setToUserId(0);
            record.setTradeNo(tradeNo1);
            record.setMoney(price);
            record.setOperationAmount(price);
            record.setCurrentBalance(obj);
            record.setBusinessId(userMoneyLog.getBusinessId());
            record.setStatus(KConstants.OrderStatus.END);
            record.setType(KConstants.ConsumeType.ADD_ROOM);
            record.setChangeType(KConstants.MOENY_ADD);
            record.setPayType(KConstants.PayType.BALANCEAY); //余额支付
            record.setDesc("付费收益");
            //record.setDesc("Paid income");
            record.setTime(com.basic.utils.DateUtil.currentTimeSeconds());
            consumeRecordDao.addConsumRecord(record);
            return true;
        });

        return room;
    }

}
