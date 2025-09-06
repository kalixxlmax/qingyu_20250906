package com.basic.im.api.controller;

import com.basic.commons.thread.ThreadUtils;
import com.basic.im.api.service.base.AbstractController;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.ex.VerifyUtil;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.common.MultipointSyncUtil;
import com.basic.im.entity.StickDialog;
import com.basic.im.friends.entity.Friends;
import com.basic.im.friends.service.impl.FriendsManagerImpl;
import com.basic.im.user.dao.OfflineOperationDao;
import com.basic.im.user.entity.User;
import com.basic.im.user.service.RoleCoreService;
import com.basic.im.user.service.StickDialogService;
import com.basic.im.user.service.impl.UserManagerImpl;
import com.basic.im.utils.SKBeanUtils;
import com.basic.im.vo.JSONMessage;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api(value = "FriendsController", tags = "好友管理相关接口")
@RestController
@Slf4j
@RequestMapping(value = "/friends", method = {RequestMethod.GET, RequestMethod.POST})
public class FriendsController extends AbstractController {

    @Autowired
    private UserManagerImpl userManager;

    @Autowired
    private FriendsManagerImpl friendsManager;

    @Autowired
    private OfflineOperationDao offlineOperationDao;


    /*@Autowired(required = false)
    private AllowRequestClientService allowRequestClientService;*/
    

    @Autowired(required = false)
    private RoleCoreService roleCoreService;


    @ApiOperation("增加用户邀请关系")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "userId", value = "用户Id", dataType = "int", required = true),
            @ApiImplicitParam(paramType = "query", name = "inviteUserId", value = "邀请用户Id", dataType = "int", required = true, defaultValue = "0")
    })
    @RequestMapping("/invite/add")
    public JSONMessage addInvite(@RequestParam(name = "userId", defaultValue = "0") Integer userId,
                                 @RequestParam(name = "inviteUserId", defaultValue = "0") Integer inviteUserId) {
        friendsManager.addInvite(userId == 0 ? VerifyUtil.verifyUserId(ReqUtil.getUserId()) : userId, inviteUserId);
        return JSONMessage.success();
    }

    @ApiOperation("加关注")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "toUserId", value = "被关注用户Id", dataType = "int", required = true),
            @ApiImplicitParam(paramType = "query", name = "fromAddType", value = "被关注用户Id", dataType = "int", required = true, defaultValue = "0")
    })
    @RequestMapping("/attention/add")
    public JSONMessage addAtt(@RequestParam(name = "toUserId", defaultValue = "0") Integer toUserId,
                              @RequestParam(name = "fromAddType", defaultValue = "0") Integer fromAddType) {
        try {
            int userId = ReqUtil.getUserId();
            if (userId == toUserId) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.NotAddSelf);
            }
            byte allowAddFriend = SKBeanUtils.getImCoreService().getClientConfig().getAllowAddFriend();
            if(0==allowAddFriend){
                byte role = (byte) roleCoreService.getUserRoleByUserId(userId);
                if (!(role == KConstants.Admin_Role.SUPER_ADMIN || role == KConstants.Admin_Role.ADMIN)) {
                    throw new ServiceException(KConstants.ResultCode.ProhibitAddFriends);
                }
            }

            String friendFroms = userManager.getUser(toUserId).getSettings().getFriendFromList();
            List<Integer> friendFromList = StringUtil.getIntList(friendFroms, ",");
            if (0 == friendFromList.size()) {
                // 添加失败,该用户禁止该方式添加好友
                return JSONMessage.failureByErrCode(KConstants.ResultCode.ProhibitAddFriends);
            } else {
                if (!fromAddType.equals(0) && !fromAddType.equals(6) && !friendFromList.contains(fromAddType)) {
                    int errorCode;
                    switch (fromAddType) {
                        case 1:
                            errorCode = KConstants.ResultCode.NotQRCodeAddFriends;
                            break;
                        case 2:
                            errorCode = KConstants.ResultCode.NotCardAddFriends;
                            break;
                        case 3:
                            errorCode = KConstants.ResultCode.NotFromGroupAddFriends;
                            break;
                        case 4:
                            errorCode = KConstants.ResultCode.NotTelephoneAddFriends;
                            break;
                        case 5:
                            errorCode = KConstants.ResultCode.NotNickNameAddFriends;
                            break;
                        default:
                            errorCode = KConstants.ResultCode.AddFriendsFailure;
                            break;
                    }
                    return JSONMessage.failureByErrCode(errorCode);
                }
            }
            return friendsManager.followUser(userId, toUserId, fromAddType);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
            return JSONMessage.failureByException(e);
        }
    }

    @ApiOperation("批量添加好友")
    @ApiImplicitParam(paramType = "query", name = "toUserIds", value = "被关注用户Id", dataType = "String", required = true)
    @RequestMapping("/attention/batchAdd")
    public JSONMessage addFriends(@RequestParam(value = "toUserIds") String toUserIds) {
       /* if(!allowRequestClientService.isAllowRequest(getRequestIp())){
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }*/
        /**
         * 后台授权用户オ能加好友ー一防止同行,私加客户
         *
         */
        byte allowAddFriend = SKBeanUtils.getImCoreService().getClientConfig().getAllowAddFriend();
        if(0==allowAddFriend){
            byte role = (byte) roleCoreService.getUserRoleByUserId(ReqUtil.getUserId());
            if (!(role == KConstants.Admin_Role.SUPER_ADMIN || role == KConstants.Admin_Role.ADMIN)) {
                throw new ServiceException(KConstants.ResultCode.ProhibitAddFriends);
            }
        }
        return friendsManager.batchFollowUser(ReqUtil.getUserId(), toUserIds);
    }

    @ApiOperation("添加黑名单")
    @ApiImplicitParam(paramType = "query", name = "toUserId", value = "拼接的被关注用户Id（用逗号分隔）", dataType = "int", required = true)
    @RequestMapping("/blacklist/add")
    public JSONMessage addBlacklist(@RequestParam Integer toUserId, @RequestParam(defaultValue = "0") Integer isNotify) {
        int userId = ReqUtil.getUserId();
        if (userId == toUserId) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NotOperateSelf);
        }
        if (friendsManager.isBlack(toUserId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NotRepeatOperation);
        }
        Object data = friendsManager.addBlacklist(ReqUtil.getUserId(), toUserId, isNotify);
        return JSONMessage.success(data);
    }

    @ApiOperation("加好友")
    @ApiImplicitParam(paramType = "query", name = "toUserId", value = "目标用户Id", dataType = "int", required = true)
    @RequestMapping("/add")
    public JSONMessage addFriends(@RequestParam(value = "toUserId") Integer toUserId) {
        int userId = ReqUtil.getUserId();
        if (userId == toUserId) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NotAddSelf);
        }
        /**
         * 后台授权用户オ能加好友ー一防止同行,私加客户
         */
        byte allowAddFriend = SKBeanUtils.getImCoreService().getClientConfig().getAllowAddFriend();
        if(0==allowAddFriend){
            byte role = (byte) roleCoreService.getUserRoleByUserId(ReqUtil.getUserId());
            if (!(role == KConstants.Admin_Role.SUPER_ADMIN || role == KConstants.Admin_Role.ADMIN)) {
                throw new ServiceException(KConstants.ResultCode.ProhibitAddFriends);
            }
        }
        Friends friends = friendsManager.getFriends(userId, toUserId);
        if (null != friends && Friends.Status.Friends == friends.getStatus()) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.FriendsIsExist);
        }
        friendsManager.addFriends(userId, toUserId);

        return JSONMessage.success();
    }

    @ApiOperation("修改好友 属性")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "toUserId", value = "目标用户Id", dataType = "int", required = true),
            @ApiImplicitParam(paramType = "query", name = "chatRecordTimeOut", value = "好友聊天记录删除时间 1天=1.0", dataType = "String", defaultValue = "-1")
    })
    @RequestMapping("/update")
    public JSONMessage updateFriends(@RequestParam(value = "toUserId") Integer toUserId, @RequestParam(defaultValue = "-1") String chatRecordTimeOut,@RequestParam(defaultValue = "-1") byte showMarker,@RequestParam(defaultValue = "") String markContent) {
        Integer userId = ReqUtil.getUserId();
        User user = userManager.getUser(userId);
        Friends friends = friendsManager.getFriends(ReqUtil.getUserId(), toUserId);

        if (null == friends) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.FriendsNotExist);
        }
        double recordTimeOut;
        recordTimeOut = Double.parseDouble(chatRecordTimeOut);
        if (-1 != showMarker){
            log.info("—_1",showMarker);
            log.info("_2",markContent);
            Friends friends_ = friendsManager.getFriends(toUserId,ReqUtil.getUserId());
            if (null != friends_){
                Map<String,Object> map_ = new HashMap<>();
                map_.put("toShowMarker",markContent);
                friendsManager.updateFriends(friends_.getId(),map_,friends_.getUserId());
                if (userManager.isOpenMultipleDevices(toUserId)) {
                    /*MultipointSyncUtil.multipointLoginUpdateUserInfo(toUserId, userManager.getUser(toUserId).getNickname(), null, null, 0);*/
                    offlineOperationDao.addOfflineOperation(toUserId, MultipointSyncUtil.MultipointLogin.TAG_FRIEND,String.valueOf(toUserId), DateUtil.currentTimeSeconds());
                }
            }
            Map<String,Object> map = new HashMap<>();
            map.put("showMarker",markContent);
            friendsManager.updateFriends(friends.getId(),map,friends.getUserId());

        }
        friends.setChatRecordTimeOut(recordTimeOut);
        friendsManager.updateFriends(friends);
        if (-1 != showMarker){
            friendsManager.sendModityShowMarker_single(user,friends,markContent);
        }

        return JSONMessage.success();
    }


    @ApiOperation("取消拉黑")
    @ApiImplicitParam(paramType = "query", name = "toUserId", value = "目标用户Id", dataType = "int", required = true)
    @RequestMapping("/blacklist/delete")
    @ResponseBody
    public JSONMessage deleteBlacklist(@RequestParam Integer toUserId, @RequestParam(defaultValue = "0") Integer isNotify) {
        if (!friendsManager.isBlack(toUserId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NotOnMyBlackList);
        }
        Object data = friendsManager.deleteBlacklist(ReqUtil.getUserId(), toUserId, isNotify);
        return JSONMessage.success(data);
    }

    @ApiOperation("取消关注")
    @ApiImplicitParam(paramType = "query", name = "toUserId", value = "目标用户Id", dataType = "int", required = true)
    @RequestMapping("/attention/delete")
    public JSONMessage deleteFollow(@RequestParam(value = "toUserId") Integer toUserId) {
        int userId = ReqUtil.getUserId();
        if (userId == toUserId) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NotOperateSelf);
        }
        friendsManager.unfollowUser(userId, toUserId);
        return JSONMessage.success();
    }

    @ApiOperation("删除好友")
    @ApiImplicitParam(paramType = "query", name = "toUserId", value = "目标用户Id", dataType = "int", required = true)
    @RequestMapping("/delete")
    public JSONMessage deleteFriends(@RequestParam Integer toUserId) {
        try {
            Integer userId = ReqUtil.getUserId();
            if (userId.equals(toUserId)) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.NotOperateSelf);
            }
            Friends friends = friendsManager.getFriends(userId, toUserId);
            if (null == friends) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.NotYourFriends);
            }
            ThreadUtils.executeInThread(call->{
                friendsManager.deleteFriends(userId, toUserId);
            });

        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }
        return JSONMessage.success();
    }

    @ApiOperation("修改备注")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "toUserId", value = "目标用户Id", dataType = "int", required = true, defaultValue = ""),
            @ApiImplicitParam(paramType = "query", name = "remarkName", value = "备注名", dataType = "String", required = true, defaultValue = ""),
            @ApiImplicitParam(paramType = "query", name = "describe", value = "描述", dataType = "String", required = true, defaultValue = "")
    })
    @RequestMapping("/remark")
    public JSONMessage friendsRemark(@RequestParam int toUserId, @RequestParam(defaultValue = "") String remarkName, @RequestParam(defaultValue = "") String describe) {
        try {
            friendsManager.updateRemark(ReqUtil.getUserId(), toUserId, remarkName, describe);
        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }
        return JSONMessage.success();
    }

    @ApiOperation("黑名单列表")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "pageIndex", value = "当前页", dataType = "int", required = true, defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "pageSize", value = "每页大小", dataType = "int", required = true, defaultValue = "10")
    })
    @RequestMapping("/blacklist")
    public JSONMessage queryBlacklist(@RequestParam(defaultValue = "0") int pageIndex, @RequestParam(defaultValue = "10") int pageSize) {
        List<Friends> data = friendsManager.queryBlacklist(ReqUtil.getUserId(), pageIndex, pageSize);

        return JSONMessage.success(data);
    }


    @ApiOperation("适用于web黑名单分页")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "pageIndex", value = "当前页", dataType = "int", required = true, defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "pageSize", value = "每页大小", dataType = "int", required = true, defaultValue = "10")
    })
    @RequestMapping("/queryBlacklistWeb")
    public JSONMessage queryBlacklistWeb(@RequestParam(defaultValue = "0") int pageIndex, @RequestParam(defaultValue = "10") int pageSize) {
        Object queryBlacklistWeb = friendsManager.queryBlacklistWeb(ReqUtil.getUserId(), pageIndex, pageSize);
        return JSONMessage.success(queryBlacklistWeb);
    }

    @ApiOperation("获取粉丝列表")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "userId", value = "用户Id", dataType = "String"),
            @ApiImplicitParam(paramType = "query", name = "pageIndex", value = "当前页码数", dataType = "String"),
            @ApiImplicitParam(paramType = "query", name = "pageSize", value = "每页数据条数", dataType = "String")
    })
    @RequestMapping("/fans/list")
    public JSONMessage queryFans(@RequestParam(defaultValue = "0") Integer userId) {
        return JSONMessage.success();
    }

    @ApiOperation("关注列表")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "userId", value = "用户Id", dataType = "int", defaultValue = ""),
            @ApiImplicitParam(paramType = "query", name = "status", value = "状态值，默认值0 （1=关注；2=好友；0=陌生人）", dataType = "int")
    })
    @RequestMapping("/attention/list")
    public JSONMessage queryFollow(@RequestParam(defaultValue = "") Integer userId, @RequestParam(defaultValue = "0") int status) {
        List<Friends> data = friendsManager.queryFollow(VerifyUtil.verifyUserId(ReqUtil.getUserId()), status);
        return JSONMessage.success(data);
    }

	@ApiOperation("关注列表,分页")
	@ApiImplicitParams({
			@ApiImplicitParam(paramType = "query", name = "userId", value = "用户Id", dataType = "int", defaultValue = ""),
			@ApiImplicitParam(paramType = "query", name = "status", value = "状态值，默认值0 （1=关注；2=好友；0=陌生人）", dataType = "int")
	})
	@RequestMapping("/attention/list/page")
	public JSONMessage queryFollowPage(@RequestParam(name = "keyword",defaultValue = "") String keyword,
                                       @RequestParam(defaultValue = "0")  int userType,
									   @RequestParam(defaultValue = "0")  int status,
									   @RequestParam(defaultValue = "0")  int pageIndex,
									   @RequestParam(defaultValue = "20") int pageSize) {
		return JSONMessage.success(friendsManager.queryFollow(VerifyUtil.verifyUserId(ReqUtil.getUserId()), status,userType,null, keyword,pageIndex,pageSize));
	}


    @ApiOperation("获取好友详情")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "userId", value = "用户Id", dataType = "int", required = true),
            @ApiImplicitParam(paramType = "query", name = "toUserId", value = "目标用户Id", dataType = "int", required = true)
    })
    @RequestMapping("/get")
    public JSONMessage getFriends(@RequestParam(defaultValue = "") Integer userId, int toUserId) {
        userId = ReqUtil.getUserId();
        Friends data = friendsManager.getFriends(userId, toUserId);
        return JSONMessage.success(data);
    }


    @ApiOperation("获取好友列表")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "userId", value = "用户Id", dataType = "int", required = true, defaultValue = ""),
            @ApiImplicitParam(paramType = "query", name = "keyword", value = "关键词（用于搜索）", dataType = "int", defaultValue = "")
    })
    @RequestMapping("/list")
    public JSONMessage queryFriends(@RequestParam(defaultValue = "") Integer userId, @RequestParam(defaultValue = "") String keyword) {
        userId = ReqUtil.getUserId();
        Object data = friendsManager.queryFriends(userId);
        return JSONMessage.success(data);
    }

    @ApiOperation("私密群好友列表查询")
    @RequestMapping("/list/private")
    public JSONMessage queryPrivateKey() {
        List<Friends> friendsList = friendsManager.queryFriends(VerifyUtil.verifyUserId(ReqUtil.getUserId()));
        friendsList.forEach(friends -> friends.setDhMsgPublicKey(userManager.getUser(friends.getToUserId()).getDhMsgPublicKey()));
        return JSONMessage.success(friendsList);
    }

    @ApiOperation("查找好友")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "userId", value = "用户Id", dataType = "int", required = true),
            @ApiImplicitParam(paramType = "query", name = "keyword", value = "关键词（用于搜索）", dataType = "String", defaultValue = ""),
            @ApiImplicitParam(paramType = "query", name = "status", value = "状态", dataType = "int", defaultValue = "2"),
            @ApiImplicitParam(paramType = "query", name = "pageIndex", value = "当前页", dataType = "int", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "pageSize", value = "每页数据大小", dataType = "int", defaultValue = "10"),
    })
    @RequestMapping("/page")
    public JSONMessage getFriendsPage(@RequestParam Integer userId, @RequestParam(defaultValue = "") String keyword,
                                      @RequestParam(defaultValue = "2") int status,
                                      @RequestParam(defaultValue = "0") int pageIndex,
                                      @RequestParam(defaultValue = "10") int pageSize) {
        userId = ReqUtil.getUserId();
        Object data = friendsManager.queryFriends(userId, status, keyword, pageIndex, pageSize);

        return JSONMessage.success(data);
    }


    @Autowired
    private StickDialogService stickDialogService;

    @ApiOperation("好友消息免打扰，阅后即焚，聊天置顶")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "offlineNoPushMsg", value = "是否开启消息免打扰，阅后即焚，聊天置顶", dataType = "int", required = true),
            @ApiImplicitParam(paramType = "query", name = "userId", value = "目标用户Id", dataType = "int", defaultValue = ""),
            @ApiImplicitParam(paramType = "query", name = "type", value = "type = 0  消息免打扰 ,type = 1  阅后即焚 ,type = 2 聊天置顶", dataType = "int", defaultValue = ""),
            @ApiImplicitParam(paramType = "query", name = "toUserId", value = "目标用户编号", dataType = "int", defaultValue = "0")
    })
    @RequestMapping("/update/OfflineNoPushMsg")
    public JSONMessage updateOfflineNoPushMsg(@RequestParam Integer userId, @RequestParam Integer toUserId, @RequestParam(defaultValue = "0") int offlineNoPushMsg, @RequestParam(defaultValue = "0") int type) {
        try {
            Integer reqUserId = VerifyUtil.verifyUserId(ReqUtil.getUserId());
            Friends data = friendsManager.updateOfflineNoPushMsg(reqUserId, toUserId, offlineNoPushMsg, type);

            if(1==type){
                if(1==offlineNoPushMsg){
                    stickDialogService.add(new StickDialog()
                            .setUserId(reqUserId)
                            .setJid(toUserId.toString()).setIsRoom(KConstants.ZERO)
                            .setTime(DateUtil.currentTimeSeconds())
                    );
                }else {
                    stickDialogService.delete(reqUserId,toUserId.toString());
                }
            }
            return JSONMessage.success(data);
        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }
    }


    @ApiOperation("获取好友的userId 和单向关注的userId  或黑名单的userId")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "userId", value = "用户ID", dataType = "String", required = true),
            @ApiImplicitParam(paramType = "query", name = "type", value = "类型", dataType = "String", required = true)
    })
    @RequestMapping("/friendsAndAttention") //返回好友的userId 和单向关注的userId  及黑名单的userId
    public JSONMessage getFriendsPage(@RequestParam Integer userId, @RequestParam(defaultValue = "") String type) {
        Object data = friendsManager.friendsAndAttentionUserId(ReqUtil.getUserId(), type);
        return JSONMessage.success(data);
    }


    @ApiOperation("获取新的朋友列表")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "userId", value = "用户ID", dataType = "int", required = true),
            @ApiImplicitParam(paramType = "query", name = "pageIndex", value = "当前页码数", dataType = "int", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "pageSize", value = "每页数据条数", dataType = "int", defaultValue = "10")
    })
    @RequestMapping("/newFriend/list")
    public JSONMessage newFriendList(@RequestParam Integer userId, @RequestParam(defaultValue = "0") int pageIndex,
                                     @RequestParam(defaultValue = "10") int pageSize) {
        Object data = friendsManager.newFriendList(ReqUtil.getUserId(), pageIndex, pageSize);

        return JSONMessage.success(data);
    }


    @ApiOperation("新朋友列表")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "userId", value = "用户ID", dataType = "int", required = true),
            @ApiImplicitParam(paramType = "query", name = "pageIndex", value = "当前页码数", dataType = "int", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "pageSize", value = "每页数据条数", dataType = "int", defaultValue = "10")
    })
    @RequestMapping("/newFriendListWeb")
    public JSONMessage newFriendListWeb(@RequestParam Integer userId, @RequestParam(defaultValue = "0") int pageIndex,
                                        @RequestParam(defaultValue = "10") int pageSize) {
        try {
            Object data = friendsManager.newFriendListWeb(ReqUtil.getUserId(), pageIndex, pageSize);
            return JSONMessage.success(data);
        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }
    }

    @ApiOperation("web版最近新朋友列表")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "userId", value = "用户ID", dataType = "int", required = true),
            @ApiImplicitParam(paramType = "query", name = "pageIndex", value = "当前页码数", dataType = "int", defaultValue = "0"),
            @ApiImplicitParam(paramType = "query", name = "pageSize", value = "每页数据条数", dataType = "int", defaultValue = "10")
    })
    @RequestMapping("/lastNewFriendListWeb")
    public JSONMessage lastNewFriendListWeb() {
        try {
            Object data = friendsManager.lastNewFriendListWeb(ReqUtil.getUserId());
            return JSONMessage.success(data);
        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }
    }


    @ApiOperation("H5新朋友的单条最新记录")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "toUserId", value = "对方用户ID", dataType = "int", required = true)
    })
    @RequestMapping("/newFriend/last")
    public JSONMessage newFriendListWeb(@RequestParam Integer toUserId) {
        try {
            Object data = friendsManager.newFriendLast(ReqUtil.getUserId(), toUserId);
            return JSONMessage.success(data);
        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }
    }


    @ApiOperation("修改和该好友的消息加密方式")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "toUserId", value = "目标用户ID", dataType = "int", required = true),
            @ApiImplicitParam(paramType = "query", name = "encryptType", value = "加密类型", dataType = "byte", defaultValue = "0")
    })
    @RequestMapping("/modify/encryptType")
    public JSONMessage modifyEncryptType(@RequestParam Integer toUserId, @RequestParam(defaultValue = "0") byte encryptType) {
        try {
            friendsManager.modifyEncryptType(ReqUtil.getUserId(), toUserId, encryptType);

            return JSONMessage.success();
        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }
    }

    @ApiOperation("修改好友的手机号备注")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "toUserId", value = "目标用户ID", dataType = "int", required = true),
            @ApiImplicitParam(paramType = "query", name = "phoneRemark", value = "手机号备注 ; 分割", dataType = "String", defaultValue = "")
    })
    @RequestMapping("/modify/phoneRemark")
    public JSONMessage updatePhoneRemark(@RequestParam Integer toUserId, @RequestParam(defaultValue = "") String phoneRemark) {
        try {
            friendsManager.updatePhoneRemark(ReqUtil.getUserId(), toUserId, phoneRemark);

            return JSONMessage.success();
        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }
    }

    @ApiOperation("设置好友隐藏会话")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "toUserId", value = "好友用户ID", dataType = "String"),
            @ApiImplicitParam(paramType = "query", name = "hideChatSwitch", value = "hideChatSwitch", dataType = "byte"),
    })
    @RequestMapping(value = "/member/setHideChatSwitch")
    public JSONMessage setHideChatPassword(@RequestParam(defaultValue = "") int toUserId, byte hideChatSwitch) {
        friendsManager.setHideChatSwitch(ReqUtil.getUserId(), toUserId, hideChatSwitch);
        return JSONMessage.success();
    }

    @ApiOperation("取得当前用户所有好友的备注信息")
    @RequestMapping(value = "/get/remark/name")
    public JSONMessage getAllRemarkName(@RequestParam(defaultValue = "0") int status) {
        return JSONMessage.success(friendsManager.getAllRemarkName(VerifyUtil.verifyUserId(ReqUtil.getUserId()),status));
    }

    @ApiOperation("设置好友是否发送消息已读状态")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "toUserId", value = "好友用户ID", dataType = "String"),
            @ApiImplicitParam(paramType = "query", name = "isSendMsgState", value = "发送消息已读状态,0 关闭 1 开启", dataType = "byte"),
    })
    @RequestMapping(value = "/setFriendsSendMsgState")
    public JSONMessage setFriendsSendMsgState(@RequestParam(defaultValue = "0") int toUserId,@RequestParam(defaultValue = "0") byte isSendMsgState) {

        friendsManager.setFriendsSendMsgState(ReqUtil.getUserId(),toUserId,isSendMsgState);
        return JSONMessage.success();
    }

}
