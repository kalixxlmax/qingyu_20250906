package com.basic.im.api.controller;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSONObject;
import com.basic.im.api.service.base.AbstractController;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.VerifyUtil;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.comm.utils.NumberUtil;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.entity.ReadDTO;
import com.basic.im.entity.StickDialog;
import com.basic.im.friends.service.FriendsManager;
import com.basic.im.message.IMessageRepository;
import com.basic.im.message.dao.TigaseMsgDao;
import com.basic.im.room.entity.Room;
import com.basic.im.room.service.RoomRedisRepository;
import com.basic.im.room.service.impl.RoomManagerImplForIM;
import com.basic.im.user.service.StickDialogService;
import com.basic.im.user.service.impl.UserManagerImpl;
import com.basic.im.vo.JSONMessage;
import com.basic.utils.StringUtil;
import com.basic.utils.StringUtils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Tigase支持接口
 *
 * @author Administrator
 */

@Slf4j
@RestController
@Api(value = "TigaseController", tags = "消息记录及漫游接口")
@RequestMapping(value = "/tigase", method = {RequestMethod.GET, RequestMethod.POST})
public class TigaseController extends AbstractController {

    @Autowired
    private IMessageRepository messageRepository;

    @Autowired
    private TigaseMsgDao tigaseMsgDao;

    @Autowired
    private RoomManagerImplForIM roomManager;

    @Autowired
    private UserManagerImpl userManager;

    @Autowired
    private RoomRedisRepository roomRedisRepository;

    @Autowired
    private StickDialogService stickDialogService;


    // 单聊聊天记录
    @ApiOperation("单聊聊天记录")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "receiver", value = "接收者userId", dataType = "int", required = true),
            @ApiImplicitParam(paramType = "query", name = "startTime", value = "开始时间（单位：毫秒）", dataType = "long", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "endTime", value = "结束时间（单位：毫秒）", dataType = "long", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "pageIndex", value = "页码（默认：0）", dataType = "int", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "pageSize", value = "页大小（默认：50）", dataType = "int", defaultValue = "20"),
            @ApiImplicitParam(paramType = "query", name = "maxType", value = "最大类型", dataType = "int", defaultValue = "200")
    })
    @RequestMapping("/chat_msgs")
    public JSONMessage queryChatMessageRecord(@RequestParam int receiver, @RequestParam(defaultValue = "0") long startTime,
                                              @RequestParam(defaultValue = "0") long endTime, @RequestParam(defaultValue = "0") int pageIndex,
                                              @RequestParam(defaultValue = "20") int pageSize, @RequestParam(defaultValue = "200") int maxType) {

        int sender = ReqUtil.getUserId();
        if (startTime > 0) {
            startTime = (startTime / 1000) - 1;
        }
        if (endTime > 0) {
            endTime = (endTime / 1000) + 1;
        }
        List<Document> list = tigaseMsgDao.
                queryChatMessageRecord(sender, receiver, startTime, endTime, pageIndex, pageSize, maxType);
        return JSONMessage.success(list);

    }

    // 群组聊天记录
    @ApiOperation("群聊聊天记录")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "roomId", value = "房间编号", dataType = "String", required = true),
            @ApiImplicitParam(paramType = "query", name = "startTime", value = "开始时间（单位：毫秒）", dataType = "long", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "endTime", value = "结束时间（单位：毫秒）", dataType = "long", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "pageIndex", value = "页码（默认：0）", dataType = "int", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "pageSize", value = "页大小（默认：50）", dataType = "int", defaultValue = "20"),
            @ApiImplicitParam(paramType = "query", name = "maxType", value = "最大类型", dataType = "int", defaultValue = "200")
    })
    @RequestMapping("/chat_muc_msgs")
    public JSONMessage queryMucMsgs(@RequestParam String roomId, @RequestParam(defaultValue = "0") long startTime,
                                    @RequestParam(defaultValue = "0") long endTime, @RequestParam(defaultValue = "0") int pageIndex,
                                    @RequestParam(defaultValue = "20") int pageSize, @RequestParam(defaultValue = "200") int maxType) {

		/*if (startTime > 0)
			startTime = (startTime / 1000);
		if (endTime > 0)
			endTime = (endTime / 1000);*/
        /*
         * if(maxType>0) q.put("contentType",new BasicDBObject(MongoOperator.LT,
         * maxType));
         */
        boolean flag = false;
        ObjectId roomObjId = roomManager.getRoomId(roomId);
        if (null != roomObjId) {
            Room.Member member = roomManager.getMember(roomObjId, ReqUtil.getUserId());
            if (null != member && null != member.getCreateTime()) {
                if (member.getIsBlack() == 1) {
                    return JSONMessage.success(Collections.emptyList());
                }
                if ((startTime / 1000) < member.getBeginMsgTime()) {
                    startTime = member.getBeginMsgTime();
                    flag = true;
                } else if ((startTime / 1000) < member.getCreateTime()) {
                    startTime = member.getCreateTime();
                    flag = true;
                }
            }

        }
        List<Document> list = tigaseMsgDao.queryMucMsgs(roomId, startTime, endTime, pageIndex, pageSize, maxType, flag);

        /* Collections.reverse(list);//倒序 */
        return JSONMessage.success("", list);
    }

    /**
     * (一段时间内最新的聊天历史记录) startTime 开始时间 毫秒数 endTime 结束时间 毫秒数
     */
    @ApiOperation("一段时间内最新的聊天历史记录")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "startTime", value = "开始时间", dataType = "long", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "endTime", value = "结束时间", dataType = "long", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "pageSize", value = "当前页", dataType = "int", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "needId", value = "是否需要ID", dataType = "int", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "needStick", value = "是否需要置顶窗口", dataType = "int", defaultValue = "0"),
    })
    @RequestMapping("/getLastChatList")
    public JSONMessage queryLastChatList(@RequestParam(defaultValue = "0") long startTime,
                                         @RequestParam(defaultValue = "0") long endTime,
                                         @RequestParam(defaultValue = "0") int pageSize,
                                         @RequestParam(defaultValue = "0") int needId,
                                         @RequestParam(defaultValue = "0") int needStick) {
        Integer userId = VerifyUtil.verifyUserId(ReqUtil.getUserId());
        List<String> roomJidList = roomRedisRepository.queryUserRoomJidList(userId);
        List<StickDialog> stickDialogs = KConstants.ONE == needStick ? stickDialogService.findListByUserId(userId) : null;
        List<Document> resultList = messageRepository.queryLastChatList(userId.toString(), startTime, endTime, pageSize, roomJidList, needId, stickDialogs);
        log.error("getLastChatList: " + userId.toString() + " message: " + JSONObject.toJSONString(resultList));
        return JSONMessage.success(resultList);
    }


    @ApiOperation("查询用户所有置顶的聊天信息")
    @RequestMapping("/getStickDialogChatList")
    public JSONMessage queryStickDialogChatList(@RequestParam(defaultValue = "0") int needId) {
        Integer userId = VerifyUtil.verifyUserId(ReqUtil.getUserId());
        List<StickDialog> stickDialogs = stickDialogService.findListByUserId(userId);
        return JSONMessage.success(tigaseMsgDao.queryStickDialogChatList(stickDialogs, needId));
    }

    @ApiOperation("查询会话ID对应的详情信息")
    @RequestMapping("/queryChatDetails")
    public JSONMessage queryChatDetails(@RequestParam(name = "param", defaultValue = "") String param) {
        JSONObject obj = JSONObject.parseObject(param);
        List<Integer> userIds = obj.getJSONArray("userIds").toJavaList(Integer.class);
        List<String> roomIds = obj.getJSONArray("roomIds").toJavaList(String.class);
        Integer userId = VerifyUtil.verifyUserId(ReqUtil.getUserId());
        return JSONMessage.success(messageRepository.queryChatDetails(userId, userIds, roomIds));
    }

    @ApiOperation("一段时间内最新的聊天历史记录")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "startTime", value = "开始时间", dataType = "long", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "endTime", value = "结束时间", dataType = "long", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "from", value = "当前设备标识码", dataType = "String", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "to", value = "查询设备标识码", dataType = "String", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "pageSize", value = "当前页", dataType = "int", defaultValue = "0")
    })
    @RequestMapping("getMultipointChat")
    public JSONMessage queryMultipointChat(@RequestParam(defaultValue = "0") long startTime,
                                           @RequestParam(defaultValue = "0") long endTime,
                                           @RequestParam(name = "from") String from,
                                           @RequestParam(name = "to") String to,
                                           @RequestParam(defaultValue = "0") int pageSize) {
        List<Document> resultList = messageRepository.queryMultipointChat(VerifyUtil.verifyUserId(ReqUtil.getUserId()), startTime, endTime, from, to, pageSize);
        return JSONMessage.success(resultList);
    }

    /*
     * @RequestMapping(value = "/push") public JSONMessage push(@RequestParam String
     * text, @RequestParam String body) { System.out.println("push"); List<Integer>
     * userIdList = JSON.parseArray(text, Integer.class); try { //String c = Tiga
     * String(body.getBytes("iso8859-1"),"utf-8");
     * KXMPPServiceImpl.getInstance().send(userIdList,body); return
     * JSONMessage.success(); } catch (Exception e) { e.printStackTrace(); } return
     * JSONMessage.failure("推送失败"); // {userId:%1$s,toUserIdList:%2$s,body:'%3$s'} }
     */


    // 获取消息接口(阅后即焚)
    // type 1 单聊 2 群聊
    @ApiOperation("获取消息接口")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "type", value = "聊天类型（默认1）  1 单聊  2 群聊", dataType = "int"),
            @ApiImplicitParam(paramType = "query", name = "messageId", value = "消息Id", dataType = "String", required = true),
            @ApiImplicitParam(paramType = "query", name = "roomJid", value = "编号", dataType = "ObjectId", required = true)
    })
    @RequestMapping("/getMessage")
    public JSONMessage getMessage(@RequestParam(defaultValue = "1") int type, @RequestParam(defaultValue = "") String messageId,
                                  @RequestParam(defaultValue = "") String roomJid) throws Exception {

        int userId = ReqUtil.getUserId();
        logger.info("[Tigase消息查询入口] 接收到消息查询请求 - 用户ID: {}, 消息类型: {}, 消息ID: {}, 房间JID: {}", 
                   userId, type, messageId, roomJid);
        
        try {
            Object result = tigaseMsgDao.queryMessage(userId, roomJid, messageId);
            logger.info("[Tigase消息查询入口] 消息查询成功 - 用户ID: {}, 消息ID: {}, 结果类型: {}", 
                       userId, messageId, result != null ? result.getClass().getSimpleName() : "null");
            return JSONMessage.success(result);
        } catch (Exception e) {
            logger.error("[Tigase消息查询入口] 消息查询失败 - 用户ID: {}, 消息ID: {}, 异常: {}", 
                        userId, messageId, e.getMessage(), e);
            throw e;
        }
    }

    /*删除消息接口
    type 1 单聊 2 群聊
    delete 1 删除属于自己的消息记录 2：撤回 删除 整条消息记录
    */
    @ApiOperation("删除消息接口")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "type", value = "聊天类型 1 单聊  2 群聊", dataType = "int", required = true, defaultValue = "1"),
            @ApiImplicitParam(paramType = "query", name = "toUserId", value = "好友用户ID", dataType = "int", required = true, defaultValue = "1"),
            @ApiImplicitParam(paramType = "query", name = "delete", value = "delete 1： 删除属于自己的消息记录 2：撤回 删除整条消息记录", dataType = "int", required = true, defaultValue = "1"),
            @ApiImplicitParam(paramType = "query", name = "messageId", value = "要删除的消息Id", dataType = "String", required = true),
            @ApiImplicitParam(paramType = "query", name = "roomJid", value = "群组Jid", dataType = "String", required = true)
    })
    @RequestMapping("/deleteMsg")
    public JSONMessage deleteMsg(@RequestParam(defaultValue = "1") int type, @RequestParam(defaultValue = "0") int toUserId,
                                 @RequestParam(defaultValue = "1") int delete, @RequestParam(defaultValue = "") String messageId,
                                 @RequestParam(defaultValue = "") String roomJid, @RequestParam(defaultValue = "0") int companyMpId) throws Exception {
        if (StringUtil.isEmpty(messageId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
        int sender = ReqUtil.getUserId();
        if (0 != companyMpId) {
            sender = companyMpId;
        }
        tigaseMsgDao.deleteMsgUpdateLastMessage(sender, toUserId, roomJid, messageId, delete, type);
        return JSONMessage.success();

    }


    @ApiOperation("单聊清空消息")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "toUserId", value = "目标用户id", dataType = "int", required = true, defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "type", value = "类型 0 清空单个 1 清空所有", dataType = "int", required = true, defaultValue = "0")
    })
    @RequestMapping("/emptyMyMsg")
    public JSONMessage emptyMsg(@RequestParam(defaultValue = "0") int toUserId,
                                @RequestParam(defaultValue = "0") int type) {

        int sender = ReqUtil.getUserId();

        if (2 == type) {
            tigaseMsgDao.cleanFriendMessage(sender, toUserId, 0);
            tigaseMsgDao.cleanFriendMessage(toUserId, sender, 0);
        } else {
            tigaseMsgDao.cleanFriendMessage(sender, toUserId, type);
        }
        return JSONMessage.success();

    }


    @ApiOperation("修改消息的已读状态")
    @ApiImplicitParam(paramType = "query", name = "messageId", value = "消息Id", dataType = "String", required = true, defaultValue = "0")
    @RequestMapping("/changeRead")
    public JSONMessage changeRead(@RequestParam String messageId) throws Exception {
        tigaseMsgDao.changeMsgReadStatus(messageId, 0, 0);
        return JSONMessage.success();

    }


    /*删除消息接口
    type 1 单聊 2 群聊
    delete 1 删除属于自己的消息记录 2：撤回 删除 整条消息记录
    */
    @ApiOperation("修改消息接口")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "type", value = "聊天类型 1 单聊  2 群聊", dataType = "int", required = true, defaultValue = "1"),
            @ApiImplicitParam(paramType = "query", name = "toUserId", value = "好友用户ID", dataType = "int", required = true, defaultValue = "1"),
            @ApiImplicitParam(paramType = "query", name = "messageId", value = "要删除的消息Id", dataType = "String", required = true),
            @ApiImplicitParam(paramType = "query", name = "message", value = "更新的消息内容", dataType = "String", required = true),
            @ApiImplicitParam(paramType = "query", name = "roomJid", value = "群组Jid", dataType = "String", required = true)
    })
    @RequestMapping("/editMsg")
    public JSONMessage editMsg(@RequestParam(defaultValue = "1") int type,
                               @RequestParam(defaultValue = "0") Integer toUserId,
                               @RequestParam(defaultValue = "") String messageId,
                               @RequestParam(defaultValue = "") String roomJid,
                               @RequestParam(defaultValue = "") String message,
                               @RequestParam(defaultValue = "0") int companyMpId) throws Exception {
        if (StringUtil.isEmpty(messageId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
        int sender = ReqUtil.getUserId();
        if (0 != companyMpId) {
            sender = companyMpId;
        }
        try {
//            log.error("data message:" + sender + "," + toUserId + "," + roomJid + "," + messageId + "," + type + "," + message);
            tigaseMsgDao.editMsgUpdateLastMessage(sender, toUserId, roomJid, messageId, type, message);

            if (type == 1) {
                userManager.sendUpdateMessage(toUserId.toString(), type, message, messageId);
            } else {
                userManager.sendUpdateMessage(roomJid, type, message, messageId);
            }
        } catch (Exception err) {
            log.error("update message error", err);
            err.printStackTrace();
        }

        return JSONMessage.success();

    }


    @ApiOperation("根据序列号拉取单聊指定消息")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "toUserId", value = "好友用户ID", dataType = "long", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "seqNos", value = "序列号列表 ,分割", dataType = "String", defaultValue = ""),
    })
    @RequestMapping("/pullChatMessageBySeqNos")
    public JSONMessage pullChatMessageBySeqNos(@RequestParam(defaultValue = "0") int userId, @RequestParam(defaultValue = "0") int toUserId,
                                               @RequestParam(defaultValue = "") String seqNos) {
        
        if (0 == userId) {
            userId = ReqUtil.getUserId();
        }
        
        logger.info("[Tigase单聊消息拉取入口] 接收到单聊消息拉取请求 - 用户ID: {}, 目标用户ID: {}, 序列号: {}", 
                   userId, toUserId, seqNos);
        
        if (StringUtils.isEmpty(seqNos)) {
            logger.warn("[Tigase单聊消息拉取入口] 序列号为空，返回空结果 - 用户ID: {}, 目标用户ID: {}", userId, toUserId);
            return JSONMessage.success();
        }
        
        try {
            String[] splitArr = seqNos.split(",");
            Set<Long> seqNoSets = new HashSet<>();
            for (String s : splitArr) {
                if (!StringUtil.isEmpty(s)) {
                    seqNoSets.add(Long.parseLong(s));
                }
            }
            
            logger.debug("[Tigase单聊消息拉取入口] 解析序列号完成 - 用户ID: {}, 目标用户ID: {}, 序列号数量: {}", 
                        userId, toUserId, seqNoSets.size());
            
            List<Document> resultList = messageRepository.pullMessageBySeqNos(userId, toUserId, seqNoSets);
            
            logger.info("[Tigase单聊消息拉取入口] 消息拉取成功 - 用户ID: {}, 目标用户ID: {}, 返回消息数量: {}", 
                       userId, toUserId, resultList != null ? resultList.size() : 0);
            
            Map<String, Object> result = new HashMap<>(2);
            result.put("userId", toUserId);
            result.put("resultList", resultList);
            return JSONMessage.success(result);
        } catch (Exception e) {
            logger.error("[Tigase单聊消息拉取入口] 消息拉取失败 - 用户ID: {}, 目标用户ID: {}, 序列号: {}, 异常: {}", 
                        userId, toUserId, seqNos, e.getMessage(), e);
            throw new RuntimeException("消息拉取失败", e);
        }
    }

    @ApiOperation("根据序列号拉取群组指定消息")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "roomJid", value = "群组Jid", dataType = "string", defaultValue = ""),
            @ApiImplicitParam(paramType = "query", name = "seqNos", value = "序列号列表 ,分割", dataType = "String", defaultValue = ""),
    })
    @RequestMapping("/pullGroupMessageBySeqNos")
    public JSONMessage pullGroupMessageBySeqNos(@RequestParam(defaultValue = "0") String roomJid,
                                                @RequestParam(defaultValue = "") String seqNos) {
        
        int userId = ReqUtil.getUserId();
        logger.info("[Tigase群聊消息拉取入口] 接收到群聊消息拉取请求 - 用户ID: {}, 房间JID: {}, 序列号: {}", 
                   userId, roomJid, seqNos);
        
        if (StringUtils.isEmpty(seqNos)) {
            logger.warn("[Tigase群聊消息拉取入口] 序列号为空，返回空结果 - 用户ID: {}, 房间JID: {}", userId, roomJid);
            return JSONMessage.success();
        }
        
        try {
            String[] splitArr = seqNos.split(",");

            Set<Long> collect = Arrays.stream(splitArr).filter(s -> !s.isEmpty()).map(s -> {
                return Long.parseLong(s);
            }).collect(Collectors.toSet());
            
            logger.debug("[Tigase群聊消息拉取入口] 解析序列号完成 - 用户ID: {}, 房间JID: {}, 序列号数量: {}", 
                        userId, roomJid, collect.size());

            List<Document> resultList = messageRepository.pullGroupMessageBySeqNos(roomJid, collect);
            
            logger.info("[Tigase群聊消息拉取入口] 消息拉取成功 - 用户ID: {}, 房间JID: {}, 返回消息数量: {}", 
                       userId, roomJid, resultList != null ? resultList.size() : 0);

            Map<String, Object> result = new HashMap<>(2);
            result.put("userId", roomJid);
            result.put("resultList", resultList);
            return JSONMessage.success(result);
        } catch (Exception e) {
            logger.error("[Tigase群聊消息拉取入口] 消息拉取失败 - 用户ID: {}, 房间JID: {}, 序列号: {}, 异常: {}", 
                        userId, roomJid, seqNos, e.getMessage(), e);
            throw new RuntimeException("群聊消息拉取失败", e);
        }
    }

    @ApiOperation("根据序列号查询单聊漫游消息")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "toUserId", value = "好友用户ID", dataType = "String", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "startSeqNo", value = "开始序列号，小的", dataType = "String", defaultValue = ""),
            @ApiImplicitParam(paramType = "query", name = "startSeqNo", value = "结束序列号 ,大的", dataType = "String", defaultValue = ""),
            @ApiImplicitParam(paramType = "query", name = "pageIndex", value = "页码（默认：0）", dataType = "int", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "pageSize", value = "页大小（默认：20）", dataType = "int", defaultValue = "20"),
    })
    @RequestMapping("/queryChatMessageBySeqNo")
    public JSONMessage queryChatMessageBySeqNo(@RequestParam(defaultValue = "0") int userId, @RequestParam(defaultValue = "0") String toUserId,
                                               @RequestParam(defaultValue = "0") long startSeqNo, @RequestParam(defaultValue = "0") long endSeqNo,
                                               @RequestParam(defaultValue = "0") int pageIndex,
                                               @RequestParam(defaultValue = "20") int pageSize) {
        List<Document> resultList;
        if (0 == userId) {
            userId = ReqUtil.getUserId();
        }
        int handleUserId = 0;
        if (toUserId.contains("_")) {
            String[] split = toUserId.split("\\_");
            toUserId = split[0];
        }

        handleUserId = Integer.parseInt(toUserId);

        resultList = messageRepository.queryChatMessageBySeqNo(userId, handleUserId, startSeqNo, endSeqNo, pageIndex, pageSize);

        return JSONMessage.success(resultList);
    }

    @ApiOperation("根据序列号拉取群组漫游消息")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "roomJid", value = "群组Jid", dataType = "string", defaultValue = ""),
            @ApiImplicitParam(paramType = "query", name = "startSeqNo", value = "开始序列号，小的", dataType = "String", defaultValue = ""),
            @ApiImplicitParam(paramType = "query", name = "startSeqNo", value = "结束序列号 ,大的", dataType = "String", defaultValue = ""),
            @ApiImplicitParam(paramType = "query", name = "pageIndex", value = "页码（默认：0）", dataType = "int", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "pageSize", value = "页大小（默认：20）", dataType = "int", defaultValue = "20"),
    })
    @RequestMapping("/queryGroupMessageBySeqNo")
    public JSONMessage queryGroupMessageBySeqNo(@RequestParam(defaultValue = "0") String roomJid,
                                                @RequestParam(defaultValue = "0") long startSeqNo,
                                                @RequestParam(defaultValue = "0") long endSeqNo,
                                                @RequestParam(defaultValue = "0") int pageIndex,
                                                @RequestParam(defaultValue = "20") int pageSize) {
        List<Document> resultList;
        ObjectId roomObjId = roomManager.getRoomId(roomJid);
        if (null != roomObjId) {
            Room.Member member = roomManager.getMember(roomObjId, ReqUtil.getUserId());
            if (null != member && null != member.getCreateTime()) {

                if (member.getIsBlack() == 1) {
                    return JSONMessage.success(Collections.emptyList());
                }

                if (startSeqNo < member.getClearMaxSeqNo()) {
                    startSeqNo = member.getClearMaxSeqNo();
                } else if (startSeqNo < member.getJoinSeqNo()) {
                    startSeqNo = member.getJoinSeqNo();
                }
            }
        }
        resultList = messageRepository.queryGroupMessageBySeqNo(roomJid, startSeqNo, endSeqNo, pageIndex, pageSize);


        return JSONMessage.success(resultList);
    }


    @ApiOperation("查询消息已读/未读列表")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "roomJid", value = "群组Jid", dataType = "string"),
            @ApiImplicitParam(paramType = "query", name = "messageId", value = "消息id", dataType = "string"),
            @ApiImplicitParam(paramType = "query", name = "isRead", value = "0 已读/ 1 未读", dataType = "int", defaultValue = "1"),
    })
    @RequestMapping("/queryRoomMessageReadList")
    public JSONMessage queryRoomMessageReadList(@RequestParam(name = "roomJid") String roomJid,
                                                @RequestParam(name = "messageId") String messageId,
                                                @RequestParam(name = "isRead", defaultValue = "0") int isRead,
                                                @RequestParam(name = "pageIndex", defaultValue = "0") int pageIndex,
                                                @RequestParam(name = "pageSize", defaultValue = "20") int pageSize) {
        return JSONMessage.success(tigaseMsgDao.queryRoomMessageReadList(VerifyUtil.verifyUserId(ReqUtil.getUserId()), roomJid, messageId, isRead, pageIndex, pageSize));
    }

    @ApiOperation("查询消息已读列表人数")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "roomJid", value = "群组Jid", dataType = "string"),
            @ApiImplicitParam(paramType = "query", name = "messageId", value = "消息id", dataType = "string"),
            @ApiImplicitParam(paramType = "query", name = "isRead", value = "0 已读/ 1 未读", dataType = "int", defaultValue = "1"),
    })
    @RequestMapping("/queryRoomMessageReadCount")
    public JSONMessage queryRoomMessageReadCount(@RequestParam(name = "roomJid") String roomJid,
                                                 @RequestParam(name = "messageId") String messageId) {
        return JSONMessage.success(tigaseMsgDao.queryRoomMessageReadCount(VerifyUtil.verifyUserId(ReqUtil.getUserId()), roomJid, messageId));
    }


    @ApiOperation("拉取离线期间的已读人数")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "lastTime", value = "上一次退出登录时间", dataType = "long"),
    })
    @RequestMapping("/queryRoomMessageReadLastTime")
    public JSONMessage queryRoomMessageReadLastTime(@RequestParam(name = "lastTime") Long lastTime) {
        List<ReadDTO> resultList = tigaseMsgDao.queryRoomMessageReadLastTime(lastTime, VerifyUtil.verifyUserId(ReqUtil.getUserId()));
        return JSONMessage.success(resultList);
    }


    @Autowired
    private FriendsManager friendsManager;


    @ApiOperation("增加置顶会话")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "jid", value = "用户ID或者群组JID", dataType = "string"),
    })
    @RequestMapping("/dialog/add")
    public JSONMessage addStickDialog(@RequestParam(name = "jid") String jid,
                                      @RequestParam(name = "isAdaptive", defaultValue = "0") Integer isAdaptive) {
        int userId = VerifyUtil.verifyUserId(ReqUtil.getUserId());
        stickDialogService.add(new StickDialog()
                .setUserId(userId)
                .setJid(jid).setIsRoom(NumberUtil.isNum(jid) ? KConstants.ZERO : KConstants.ONE)
                .setTime(DateUtil.currentTimeSeconds())
        );
        VerifyUtil.execute(KConstants.ONE == isAdaptive, () -> updateOfflineNoPushMsg(userId, jid, KConstants.ONE));
        return JSONMessage.success();
    }

    @ApiOperation("删除置顶会话")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "jid", value = "用户ID或者群组JID", dataType = "string"),
    })
    @RequestMapping("/dialog/delete")
    public JSONMessage deleteStickDialog(@RequestParam(name = "jid") String jid,
                                         @RequestParam(name = "isAdaptive", defaultValue = "0") Integer isAdaptive) {
        Integer userId = VerifyUtil.verifyUserId(ReqUtil.getUserId());
        stickDialogService.delete(userId, jid);
        VerifyUtil.execute(KConstants.ONE == isAdaptive, () -> updateOfflineNoPushMsg(userId, jid, KConstants.ZERO));
        return JSONMessage.success();
    }


    /**
     * 更新群组或者好友聊天置顶字段
     */
    private void updateOfflineNoPushMsg(Integer userId, String jid, Integer type) {
        if (NumberUtil.isNumeric(jid)) {
            friendsManager.updateOfflineNoPushMsg(userId, Integer.parseInt(jid), type, KConstants.TWO);
        } else {
            ObjectId roomId = VerifyUtil.isEmpty(roomManager.getRoomId(jid));
            int role = roomManager.findMemberAndRole(roomId, userId);
            VerifyUtil.isRollback(KConstants.ZERO > role, KConstants.ResultCode.NO_PERMISSION);
            Room room = roomManager.getRoom(roomId);
            VerifyUtil.isRollback(ObjectUtil.isNull(room), KConstants.ResultCode.NotRoom);
            roomManager.Memberset(type, roomId, userId, KConstants.ONE);
        }
    }

    @ApiOperation("web单聊聊天记录")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "receiver", value = "对方userId", dataType = "int", required = true),
            @ApiImplicitParam(paramType = "query", name = "pageIndex", value = "当前页数", dataType = "int", defaultValue = "-1"),
            @ApiImplicitParam(paramType = "query", name = "pageSize", value = "每页数量", dataType = "int", defaultValue = "10"),
            @ApiImplicitParam(paramType = "query", name = "startTime", value = "开始时间（单位：毫秒）", dataType = "long", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "endTime", value = "结束时间（单位：毫秒）", dataType = "long", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "type", value = "消息类型", dataType = "int", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "content", value = "内容", dataType = "int", defaultValue = "")
    })
    @RequestMapping("/single/chat")
    public JSONMessage querySingleChat(@RequestParam int receiver, @RequestParam(defaultValue = "0") int pageIndex, @RequestParam(defaultValue = "10") int pageSize,
                                       @RequestParam(defaultValue = "0") long startTime, @RequestParam(defaultValue = "0") long endTime,
                                       @RequestParam(defaultValue = "0") int type, @RequestParam(defaultValue = "") String content) {
        return JSONMessage.success(tigaseMsgDao.querySingleChat(ReqUtil.getUserId(), receiver, pageIndex, pageSize, startTime, endTime, type, content));
    }

    @ApiOperation("web群聊聊天记录")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "roomJid", value = "房间编号", dataType = "String", required = true),
            @ApiImplicitParam(paramType = "query", name = "startTime", value = "开始时间（单位：毫秒）", dataType = "long", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "endTime", value = "结束时间（单位：毫秒）", dataType = "long", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "pageIndex", value = "当前页数", dataType = "int", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "pageSize", value = "每页数量", dataType = "int", defaultValue = "10"),
            @ApiImplicitParam(paramType = "query", name = "type", value = "消息类型", dataType = "int", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "content", value = "内容", dataType = "int", defaultValue = ""),
            @ApiImplicitParam(paramType = "query", name = "memberId", value = "成员Id", dataType = "int", defaultValue = ""),
    })
    @RequestMapping("/group/chat")
    public JSONMessage queryGroupChat(@RequestParam String roomJid, @RequestParam(defaultValue = "0") int pageIndex, @RequestParam(defaultValue = "10") int pageSize,
                                      @RequestParam(defaultValue = "0") long startTime, @RequestParam(defaultValue = "0") long endTime, @RequestParam(defaultValue = "0") int type,
                                      @RequestParam(defaultValue = "") String content, @RequestParam(defaultValue = "0") int memberId) {
        boolean flag = false;
		/*ObjectId roomObjId =roomManager.getRoomId(roomJid);
		if (null != roomObjId) {
			Room.Member member =roomManager.getMember(roomObjId, ReqUtil.getUserId());
			if(null!=member&&null!=member.getCreateTime()){
				if (member.getIsBlack()==1){
					return JSONMessage.success(Collections.emptyList());
				}
				if ((startTime/1000) < member.getBeginMsgTime()){
					startTime = member.getBeginMsgTime();
					flag = true;
				}else if ((startTime/1000) < member.getCreateTime()){
					startTime = member.getCreateTime();
					flag = true;
				}
			}
		}*/
        return JSONMessage.success(tigaseMsgDao.queryGroupChat(roomJid, pageIndex, pageSize, startTime, endTime, type, content, flag, memberId));
    }


}
