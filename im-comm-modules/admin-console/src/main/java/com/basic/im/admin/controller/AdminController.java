package com.basic.im.admin.controller;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.basic.common.model.PageResult;
import com.basic.common.model.PageVO;
import com.basic.commons.thread.ThreadUtils;
import com.basic.im.admin.dao.KeywordDAO;
import com.basic.im.admin.entity.AreaConfig;
import com.basic.im.admin.entity.CenterConfig;
import com.basic.im.admin.entity.KeyWord;
import com.basic.im.admin.entity.KeywordDenyRecord;
import com.basic.im.admin.entity.RedPacket;
import com.basic.im.admin.entity.RedReceive;
import com.basic.im.admin.entity.ServerListConfig;
import com.basic.im.admin.entity.Transfer;
import com.basic.im.admin.entity.UrlConfig;
import com.basic.im.admin.entity.WebUrlRoster;
import com.basic.im.admin.service.KeywordManager;
import com.basic.im.admin.service.WebUrlRosterManager;
import com.basic.im.admin.service.impl.AdminManagerImpl;
import com.basic.im.admin.service.impl.ErrorMessageManageImpl;
import com.basic.im.api.service.AuthServiceUtils;
import com.basic.im.api.service.base.AbstractController;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.comm.utils.LoginPassword;
import com.basic.im.comm.utils.NumberUtil;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.common.service.PaymentManager;
import com.basic.im.common.service.RedPacketsManager;
import com.basic.im.config.AppConfig;
import com.basic.im.entity.Givegift;
import com.basic.im.entity.SysApiLog;
import com.basic.im.friends.entity.Friends;
import com.basic.im.friends.service.impl.FriendsManagerImpl;
import com.basic.im.live.entity.LiveRoom;
import com.basic.im.live.service.impl.LiveRoomManagerImpl;
import com.basic.im.manual.entity.CollectionAccount;
import com.basic.im.manual.service.CollectionAccountManager;
import com.basic.im.message.IMessageRepository;
import com.basic.im.message.MessageService;
import com.basic.im.message.MessageType;
import com.basic.im.message.dao.TigaseMsgDao;
import com.basic.im.model.ErrorMessage;
import com.basic.im.model.PressureParam;
import com.basic.im.msg.entity.Comment;
import com.basic.im.msg.entity.FxSetting;
import com.basic.im.msg.entity.Msg;
import com.basic.im.msg.entity.MusicInfo;
import com.basic.im.msg.entity.Praise;
import com.basic.im.msg.service.impl.FxManagerImpl;
import com.basic.im.msg.service.impl.MsgCommentManagerImpl;
import com.basic.im.msg.service.impl.MsgManagerImpl;
import com.basic.im.msg.service.impl.MsgPraiseManagerImpl;
import com.basic.im.msg.service.impl.MusicManagerImpl;
import com.basic.im.open.entity.OfficialInfo;
import com.basic.im.open.opensdk.OfficialInfoCheckImpl;
import com.basic.im.open.opensdk.OpenAccountManageImpl;
import com.basic.im.open.opensdk.OpenAppManageImpl;
import com.basic.im.open.opensdk.OpenCheckLogManageImpl;
import com.basic.im.open.opensdk.entity.SkOpenAccount;
import com.basic.im.open.opensdk.entity.SkOpenApp;
import com.basic.im.open.opensdk.entity.SkOpenCheckLog;
import com.basic.im.pay.entity.BaseConsumeRecord;
import com.basic.im.room.entity.Room;
import com.basic.im.room.service.impl.RoomManagerImplForIM;
import com.basic.im.room.vo.RoomVO;
import com.basic.im.security.entity.ResourceInfo;
import com.basic.im.security.entity.SecurityRole;
import com.basic.im.security.service.ResourceInfoManager;
import com.basic.im.security.service.SecurityRoleManager;
import com.basic.im.support.Callback;
import com.basic.im.user.constants.MoneyLogConstants.MoenyAddEnum;
import com.basic.im.user.constants.MoneyLogConstants.MoneyLogEnum;
import com.basic.im.user.constants.MoneyLogConstants.MoneyLogTypeEnum;
import com.basic.im.user.dao.InviteCodeDao;
import com.basic.im.user.dao.ReportDao;
import com.basic.im.user.dao.UserDao;
import com.basic.im.user.dao.impl.UserStatusCountDao;
import com.basic.im.user.entity.AuthKeys;
import com.basic.im.user.entity.Certification;
import com.basic.im.user.entity.InviteCode;
import com.basic.im.user.entity.Report;
import com.basic.im.user.entity.Role;
import com.basic.im.user.entity.User;
import com.basic.im.user.entity.UserMoneyLog;
import com.basic.im.user.model.KeyPairParam;
import com.basic.im.user.model.SdkLoginInfo;
import com.basic.im.user.model.UserExample;
import com.basic.im.user.service.UserCoreRedisRepository;
import com.basic.im.user.service.impl.AuthKeysServiceImpl;
import com.basic.im.user.service.impl.RoleManagerImpl;
import com.basic.im.user.service.impl.UserManagerImpl;
import com.basic.im.user.utils.KSessionUtil;
import com.basic.im.utils.ExcelUtil;
import com.basic.im.utils.SKBeanUtils;
import com.basic.im.vo.JSONMessage;
import com.basic.utils.DateUtil;
import com.basic.utils.Md5Util;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoQueryException;
import eu.bitwalker.useragentutils.DeviceType;
import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.poi.ss.usermodel.Workbook;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import springfox.documentation.annotations.ApiIgnore;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 酷聊后台管理
 *
 * @author luorc
 */

@ApiIgnore
@RestController
@RequestMapping(value = "/console")
public class AdminController extends AbstractController {

    public static final String LOGIN_USER_KEY = "LOGIN_USER";

    protected Logger logger = LoggerFactory.getLogger(AdminController.class);

    @Autowired
    private MsgManagerImpl msgManager;

    @Autowired
    private MsgCommentManagerImpl commentManager;

    @Autowired
    private MsgPraiseManagerImpl msgPraiseManager;

    @Autowired
    private KeywordDAO keywordDAO;

    @Autowired
    private TigaseMsgDao tigaseMsgDao;

    @Autowired
    private ReportDao reportDao;

    @Autowired
    private WebUrlRosterManager webUrlRosterManager;

    @Autowired
    private OfficialInfoCheckImpl officialInfoCheck;

    @Autowired
    private RoleManagerImpl roleManager;

    @Autowired
    private LiveRoomManagerImpl liveRoomManager;

    @Autowired
    private RoomManagerImplForIM roomManager;

    @Autowired
    private UserManagerImpl userManager;

    @Autowired
    private FriendsManagerImpl friendsManager;

    @Autowired
    private AdminManagerImpl adminManager;

    @Autowired
    private ErrorMessageManageImpl errorMessageManage;

    @Autowired
    private OpenCheckLogManageImpl openCheckLogManage;

    @Autowired
    private OpenAppManageImpl openAppManage;

    @Autowired
    private MessageService messageService;

    @Autowired
    private MusicManagerImpl musicManager;

    @Autowired
    private OpenAccountManageImpl openAccountManage;

    @Autowired
    private KeywordManager keywordManager;

    @Autowired
    private IMessageRepository messageRepository;

    @Autowired
    private AppConfig appConfig;

    @Autowired(required = false)
    private PaymentManager paymentManager;

    @Autowired(required = false)
    private RedPacketsManager redPacketsManager;


    @Autowired
    private InviteCodeDao inviteCodeDao;

    @Autowired
    private UserDao userDao;

    @Autowired
    private AuthServiceUtils authServiceUtils;

    @Autowired
    private UserCoreRedisRepository userCoreRedisRepository;

    @Autowired
    private AuthKeysServiceImpl authKeysService;

    @Autowired
    private UserStatusCountDao userStatusCountDao;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private SecurityRoleManager securityRoleManager;

    @Autowired
    private ResourceInfoManager resourceInfoManager;

    @Autowired
    private FxManagerImpl fxSettingManager;

    protected ObjectId parse(String s) {
        return StringUtil.isEmpty(s) ? null : new ObjectId(s);
    }

    @RequestMapping(value = "/fxList")
    public JSONMessage fxList(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "10") Integer limit) {
        PageResult<FxSetting> result = adminManager.fxList(page, limit);
        return JSONMessage.success(result);
    }

    @RequestMapping(value = "/addFx")
    public JSONMessage addFx(@ModelAttribute FxSetting fxSetting) {
        try {
            // 权限校验
            byte role = (byte) roleManager.getUserRoleByUserId(ReqUtil.getUserId());
            if (role != KConstants.Admin_Role.SUPER_ADMIN && role != KConstants.Admin_Role.ADMIN) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION, ReqUtil.getRequestLanguage());
            }
            fxSettingManager.addMusicInfo(fxSetting);
            return JSONMessage.success();
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }

    }

    @RequestMapping(value = "/deleteFx")
    public JSONMessage deleteFx(@RequestParam(defaultValue = "") String musicInfoId) {
        try {
            // 权限校验
            byte role = (byte) roleManager.getUserRoleByUserId(ReqUtil.getUserId());
            if (role != KConstants.Admin_Role.SUPER_ADMIN && role != KConstants.Admin_Role.ADMIN) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION, ReqUtil.getRequestLanguage());
            }
            fxSettingManager.deleteMusicInfo(new ObjectId(musicInfoId));
            return JSONMessage.success();
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }

    }

    @RequestMapping(value = "/updateFx")
    public JSONMessage updateFx(@ModelAttribute FxSetting fxSetting) {
        try {
            // 权限校验
            byte role = (byte) roleManager.getUserRoleByUserId(ReqUtil.getUserId());
            if (role != KConstants.Admin_Role.SUPER_ADMIN && role != KConstants.Admin_Role.ADMIN) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION, ReqUtil.getRequestLanguage());
            }
            fxSettingManager.updateMusicInfo(fxSetting);
            return JSONMessage.success();
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }
    }


    @RequestMapping(value = "/chat_logs_all")
    public JSONMessage chat_logs_all(@RequestParam(defaultValue = "0") long startTime, @RequestParam(defaultValue = "0") long endTime,
                                     @RequestParam(defaultValue = "") String documents, @RequestParam(defaultValue = "") String sender,
                                     @RequestParam(defaultValue = "") String receiver, @RequestParam(defaultValue = "0") int page,
                                     @RequestParam(defaultValue = "10") int limit, @RequestParam(defaultValue = "") String keyWord) throws Exception {
        int userId = 0;
        int toUserId = 0;
        if (!StringUtil.isEmpty(sender.trim())) {
            userId = Integer.parseInt(sender);
            documents = String.valueOf(userId / KConstants.DB_INDEX_COUNT);
        }
        if (!StringUtil.isEmpty(receiver.trim())) {
            toUserId = Integer.parseInt(receiver);
        }
        PageResult<Document> pageResult = tigaseMsgDao.chat_logs_all(startTime, endTime, userId, toUserId, page, limit, keyWord, documents);
        return JSONMessage.success(pageResult);
    }


    @RequestMapping(value = "/chat_logs_all/count")
    public JSONMessage chat_logs_all_count(@RequestParam(defaultValue = "0") long startTime, @RequestParam(defaultValue = "0") long endTime,
                                           @RequestParam(defaultValue = "") String documents, @RequestParam(defaultValue = "") String sender,
                                           @RequestParam(defaultValue = "") String receiver, @RequestParam(defaultValue = "0") int page,
                                           @RequestParam(defaultValue = "10") int limit, @RequestParam(defaultValue = "") String keyWord) throws Exception {
        int userId = 0;
        int toUserId = 0;
        if (!StringUtil.isEmpty(sender.trim())) {
            userId = Integer.parseInt(sender);
        }
        if (!StringUtil.isEmpty(receiver.trim())) {
            toUserId = Integer.parseInt(receiver);
        }
        long count = tigaseMsgDao.chat_logs_all_count(startTime, endTime, userId, toUserId, page, limit, keyWord, documents);
        return JSONMessage.success(count);
    }

    @RequestMapping(value = "/chat_logs_all/del", method = {RequestMethod.POST})
    public JSONMessage chat_logs_all_del(@RequestParam(defaultValue = "0") long startTime,
                                         @RequestParam(defaultValue = "0") long endTime, @RequestParam(defaultValue = "0") int sender,
                                         @RequestParam(defaultValue = "0") int receiver, @RequestParam(defaultValue = "0") int pageIndex,
                                         @RequestParam(defaultValue = "25") int pageSize, HttpServletRequest request, HttpServletResponse response)
            throws Exception {
        tigaseMsgDao.chat_logs_all_del(startTime, endTime, sender, receiver, pageIndex, pageSize);
        return JSONMessage.success();

    }


    @RequestMapping(value = "/deleteChatMsgs")
    public JSONMessage deleteChatMsgs(@RequestParam(defaultValue = "") String msgId, @RequestParam(defaultValue = "0") int type,
                                      @RequestParam(defaultValue = "") String collectionName, @RequestParam(defaultValue = "") String snederUserId) {

        if (StringUtil.isEmpty(msgId) && type != 1 && type != 2) {
            return JSONMessage.failure("参数有误");
        }

        try {

            tigaseMsgDao.deleteChatMsgs(msgId, type, collectionName);

        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }
        return JSONMessage.success();
    }

    //    @ApiOperation("修改消息接口")
//    @ApiImplicitParams({
//            @ApiImplicitParam(paramType = "query", name = "type", value = "聊天类型 1 单聊  2 群聊", dataType = "int", required = true, defaultValue = "1"),
//            @ApiImplicitParam(paramType = "query", name = "toUserId", value = "好友用户ID", dataType = "int", required = true, defaultValue = "1"),
//            @ApiImplicitParam(paramType = "query", name = "messageId", value = "要删除的消息Id", dataType = "String", required = true),
//            @ApiImplicitParam(paramType = "query", name = "message", value = "更新的消息内容", dataType = "String", required = true),
//            @ApiImplicitParam(paramType = "query", name = "roomJid", value = "群组Jid", dataType = "String", required = true)
//    })
    @RequestMapping(value = "/editChatMsg", method = {RequestMethod.POST})
    public JSONMessage editChatMsgs(@RequestParam(defaultValue = "1") int type,
                                    @RequestParam(defaultValue = "0") Integer sender,
                                    @RequestParam(defaultValue = "0") Integer toUserId,
                                    @RequestParam(defaultValue = "") String messageId,
                                    @RequestParam(defaultValue = "") String roomJid,
                                    @RequestParam(defaultValue = "") String message) throws Exception {
        if (com.basic.utils.StringUtil.isEmpty(messageId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
        try {
            logger.error("admin edit message:" + sender + "," + toUserId + "," + roomJid + "," + messageId + "," + type + "," + message);
            tigaseMsgDao.editMsgUpdateLastMessage(sender, toUserId, roomJid, messageId, type, message);
            if (type == 1) {
                userManager.sendUpdateMessage(sender.toString(), type, message, messageId, toUserId);
                userManager.sendUpdateMessage(toUserId.toString(), type, message, messageId, sender);
            } else {
                userManager.sendUpdateMessage(roomJid, type, message, messageId, sender);
            }
        } catch (Exception err) {
            logger.error("update message error", err);
            err.printStackTrace();
        }
        return JSONMessage.success();
    }

    @RequestMapping(value = "/deleteRoom")
    public JSONMessage deleteRoom(@RequestParam(defaultValue = "") String roomId, @RequestParam(defaultValue = "0") Integer userId) {
        try {
            roomManager.delete(new ObjectId(roomId), userId);
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }
    }

    // 邀请用户加入群成员
    @RequestMapping(value = "/inviteJoinRoom")
    public JSONMessage inviteJoinRoom(@RequestParam(defaultValue = "") String roomId, @RequestParam(defaultValue = "") String userIds, @RequestParam(defaultValue = "") Integer inviteUserId) {
        try {
            if (StringUtil.isEmpty(userIds)) {
                return JSONMessage.failure("请选择邀请的人");
            }
            Room room = roomManager.getRoom(new ObjectId(roomId));
            if (null == room) {
                return JSONMessage.failure("群组不存在 或已解散!");
            } else if (-1 == room.getS()) {
                return JSONMessage.failure("该群组已被后台锁定!");
            } else {
                List<Integer> userIdList = StringUtil.getIntList(userIds, ",");
                if (room.getMaxUserSize() < room.getUserSize() + userIdList.size()) {
                    return JSONMessage.failure("群人数已满 不能加入!");
                }
                User user = new User();
                user.setUserId(inviteUserId);
                user.setNickname(KConstants.SystemNo.CONSOLE_SYSTEM_NAME);
                roomManager.consoleJoinRoom(user, new ObjectId(roomId), userIdList);
                return JSONMessage.success();
            }
        } catch (ServiceException e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 删除房间成员
     *
     * @param roomId
     * @param userId
     * @param pageIndex
     * @return
     */
    @RequestMapping(value = "/deleteMember")
    public JSONMessage deleteMember(@RequestParam String roomId, @RequestParam String userId,
                                    @RequestParam(defaultValue = "0") int pageIndex, @RequestParam String adminUserId) {
        try {
            if (StringUtil.isEmpty(userId) || StringUtil.isEmpty(adminUserId)) {
                return JSONMessage.failure("参数有误");
            } else {
                User user = userManager.getUser(Integer.parseInt(adminUserId));
                if (null != user) {
                    String[] userIds = StringUtil.getStringList(userId);
                    for (String strUserids : userIds) {
                        Integer strUserId = Integer.valueOf(strUserids);
                        roomManager.deleteMember(user, new ObjectId(roomId), strUserId, false);
                    }
                } else {
                    return JSONMessage.failure("用户不存在");
                }
            }
            return JSONMessage.success();
        } catch (ServiceException e) {
            return JSONMessage.failureByErrCode(e.getResultCode());
        }
    }

    /**
     * 设置付费群聊成员过期时间
     *
     * @param roomId
     * @param userId
     * @param deadLine
     * @return
     */
    @RequestMapping(value = "/setMemberDeadline")
    public JSONMessage setMemberDeadline(@RequestParam String roomId, @RequestParam String userId,
                                         @RequestParam String deadLine) {
        try {
            long time = cn.hutool.core.date.DateUtil.parse(deadLine).getTime();
            roomManager.setDealLine(new ObjectId(roomId), userId, time);
            return JSONMessage.success();
        } catch (ServiceException e) {
            return JSONMessage.failureByErrCode(e.getResultCode());
        }
    }

    @RequestMapping(value = "/deleteUser")
    public JSONMessage deleteUser(@RequestParam(defaultValue = "") String userId) {
        try {
            if (!StringUtil.isEmpty(userId)) {
                String[] strUserIds = StringUtil.getStringList(userId, ",");
                userManager.deleteUser(ReqUtil.getUserId(), strUserIds);
                Arrays.stream(strUserIds).forEach(str -> {
                    adminManager.deleteMallUser(Long.valueOf(str));
                });
            }
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    public User getUser() {
        Object obj = RequestContextHolder.getRequestAttributes().getAttribute(LOGIN_USER_KEY,
                RequestAttributes.SCOPE_SESSION);
        return null == obj ? null : (User) obj;
    }

    /**
     * 群聊记录
     *
     * @param startTime
     * @param endTime
     * @param room_jid_id
     * @param page
     * @param limit
     * @return
     */
    @RequestMapping(value = "/groupchat_logs_all")
    public JSONMessage groupchat_logs_all(@RequestParam(defaultValue = "0") long startTime,
                                          @RequestParam(defaultValue = "0") long endTime, @RequestParam(defaultValue = "") String room_jid_id,
                                          @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "10") int limit,
                                          @RequestParam(defaultValue = "") String keyWord, @RequestParam(defaultValue = "") String sender) {
        PageResult<Document> result = null;

        try {
            result = tigaseMsgDao.groupchat_logs_all(startTime, endTime, room_jid_id, page, limit, keyWord, sender);

        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }
        return JSONMessage.success(result);
    }

    @RequestMapping(value = "/groupchat_logs_all/del")
    public JSONMessage groupchat_logs_all_del(@RequestParam(defaultValue = "0") long startTime,
                                              @RequestParam(defaultValue = "0") long endTime, @RequestParam(defaultValue = "") String msgId,
                                              @RequestParam(defaultValue = "") String room_jid_id) throws Exception {
        if (StringUtil.isEmpty(msgId)) {
            return JSONMessage.failure("参数有误");
        }
        tigaseMsgDao.groupchat_logs_all_del(startTime, endTime, msgId, room_jid_id);
        return JSONMessage.success();
    }

    @RequestMapping(value = "/groupchat_logs_all/edit")
    public JSONMessage groupchat_logs_all_edit(@RequestParam(defaultValue = "") String msgId,
                                               @RequestParam(defaultValue = "") String room_jid_id,
                                               @RequestParam(defaultValue = "") String content
    ) throws Exception {
        if (StringUtil.isEmpty(msgId) || StringUtil.isEmpty(room_jid_id)) {
            return JSONMessage.failure("参数有误");
        }
        tigaseMsgDao.groupchat_logs_all_edit(msgId, room_jid_id, content);
        return JSONMessage.success();
    }

    @RequestMapping(value = "/groupchatMsgDel")
    public JSONMessage groupchatMsgDel(@RequestParam(defaultValue = "") String roomJid,
                                       @RequestParam(defaultValue = "0") int type) {

        try {
            tigaseMsgDao.groupchatMsgDel(roomJid, type);
        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }
        return JSONMessage.success();
    }

    @ApiIgnore
    @RequestMapping(value = "/login", method = {RequestMethod.GET})
    public void openLogin(HttpServletRequest request, HttpServletResponse response) {

        String path = request.getContextPath() + "/pages/console/login.html";
        try {
            response.sendRedirect(path);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * @param request
     * @param response
     * @return
     * @throws Exception
     * @Description: 后台管理登录：允许超级管理员，管理员，游客，客服，财务登录
     **/
    @ApiIgnore
    @RequestMapping(value = "/login", method = {RequestMethod.POST})
    public JSONMessage login(HttpServletRequest request, HttpServletResponse response) throws Exception {
        final Integer code = 86;
        String account = request.getParameter("account");
        String password = request.getParameter("password");
        String areaCode = request.getParameter("areaCode");
        HashMap<String, Object> map = new HashMap<>();
        User user = userManager.getUser((StringUtil.isEmpty(areaCode) ? (code + account) : (areaCode + account)));
        if (null == user) {
            return JSONMessage.failure("账号不存在");
        }
        Role userRole = roleManager.getUserRole(user.getUserId(), null, 5);
        if (null == userRole) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }
        if (-1 == userRole.getStatus()) {
            return JSONMessage.failure("您的账号已被禁用");
        }

        if (!password.equals(user.getPassword())) {
            String dbpassword = authKeysService.queryLoginPassword(user.getUserId());
            password = LoginPassword.encodeFromOldPassword(password);
            if (!password.equals(dbpassword)) {
                return JSONMessage.failure("帐号或密码错误！");
            }
        }
        Map<String, Object> tokenMap = KSessionUtil.adminLoginSaveToken(user.getUserId().toString(), null);
        map.put("access_Token", tokenMap.get("access_Token"));
        map.put("adminId", user.getTelephone());
        map.put("account", user.getUserId() + "");
        map.put("apiKey", appConfig.getApiKey());
        map.put("role", userRole.getRole() + "");
        map.put("nickname", user.getNickname());
        map.put("registerInviteCode", adminManager.getConfig().getRegisterInviteCode());
        // 维护最后登录时间
        updateLastLoginTime(user.getUserId());
        return JSONMessage.success(map);
    }

    private void updateLastLoginTime(Integer userId) {
        Role role = new Role(userId);
        roleManager.modifyRole(role);
    }

    @RequestMapping(value = "/logout")
    public JSONMessage logout(HttpServletRequest request, HttpServletResponse response) {
        try {
            request.getSession().removeAttribute(LOGIN_USER_KEY);
            KSessionUtil.removeAdminToken(ReqUtil.getUserId());
            return JSONMessage.success();
        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }
    }

    @RequestMapping(value = "/pushToAll")
    public void pushToAll(HttpServletResponse response, @RequestParam int fromUserId, @RequestParam String body) {

        MessageBean mb = JSON.parseObject(body, MessageBean.class);
        mb.setFromUserId(fromUserId + "");

        mb.setMsgType(0);
        mb.setMessageId(StringUtil.randomUUID());
        ThreadUtils.executeInThread(new Callback() {

            @Override
            public void execute(Object obj) {
                List<Integer> allUserId = userManager.getUserDao().getAllUserId();
                allUserId.forEach(userId -> {
                    try {
                        mb.setToUserId(String.valueOf(userId));
                        messageService.send(mb);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

            }
        });
        try {
            response.setContentType("text/html; charset=UTF-8");
            PrintWriter writer = response.getWriter();
            writer.write(
                    "<script type='text/javascript'>alert('\u6279\u91CF\u53D1\u9001\u6D88\u606F\u5DF2\u5B8C\u6210\uFF01');window.location.href='/pages/qf.jsp';</script>");
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @RequestMapping(value = "/roomList")
    public JSONMessage roomList(@RequestParam(defaultValue = "") String keyWorld,
                                @RequestParam(defaultValue = "") String isSecretGroup,
                                @RequestParam(defaultValue = "0") int page,
                                @RequestParam(defaultValue = "15") int limit, @RequestParam(defaultValue = "0") int leastNumbers) {

        PageResult<Room> result = new PageResult<Room>();

        Query query = roomManager.getRoomDao().createQuery();

        if (!StringUtil.isEmpty(keyWorld)) {
            query.addCriteria(new Criteria().orOperator(
                    Criteria.where("name").regex(keyWorld),
                    Criteria.where("jid").is(keyWorld),
                    Criteria.where("_id").is(keyWorld)
            ));
        }
        if (!StringUtil.isEmpty(isSecretGroup)) {
            query.addCriteria(Criteria.where("isSecretGroup").is(Integer.valueOf(isSecretGroup)));
        }
        if (leastNumbers > 0) {
            query.addCriteria(Criteria.where("userSize").gt(leastNumbers));
        }
        result.setCount(roomManager.getRoomDao().count(query));
        query.with(Sort.by(Sort.Order.desc("createTime")));
        query.with(PageRequest.of(page - 1, limit));
        result.setData(roomManager.getRoomDao().queryListsByQuery(query));

        return JSONMessage.success(result);
    }

    @RequestMapping(value = "/activeRoomList")
    public JSONMessage activeRoomList(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "15") int limit) {

        PageResult<Room> result = roomManager.getRoomDao()
                .queryActiveRoomList(page, limit);


        return JSONMessage.success(result);
    }

    @RequestMapping(value = "/getRoomMember")
    public JSONMessage getRoom(@RequestParam(defaultValue = "") String roomId) {
        Room room = roomManager.consoleGetRoom(new ObjectId(roomId));
        return JSONMessage.success(null, room);
    }

    @RequestMapping(value = "/room/config")
    public JSONMessage roomConfig(@RequestParam(defaultValue = "") String roomId) {
        Room room = roomManager.findRoom(new ObjectId(roomId));
        return JSONMessage.success(null, room);
    }

    /**
     * 直播间聊天记录
     *
     * @param page
     * @param limit
     * @param room_jid_id
     * @return
     */
    @RequestMapping(value = "/roomMsgDetail")
    public JSONMessage roomDetail(@RequestParam(defaultValue = "0") int page,
                                  @RequestParam(defaultValue = "50") int limit, @RequestParam(defaultValue = "") String room_jid_id) {

        PageResult<Document> result = tigaseMsgDao.roomDetail(page, limit, room_jid_id);
        return JSONMessage.success(result);
    }

    /**
     * 直播间收到的礼物列表
     *
     * @param userId
     * @param startDate
     * @param endDate
     * @param page
     * @param limit
     * @return
     */
    @RequestMapping(value = "/getGiftList")
    public JSONMessage getGiftList(@RequestParam(defaultValue = "") String userId, @RequestParam(defaultValue = "") String startDate,
                                   @RequestParam(defaultValue = "") String endDate, @RequestParam(defaultValue = "0") int page,
                                   @RequestParam(defaultValue = "10") int limit) {
        try {
            PageResult<Givegift> result = liveRoomManager.getGiftList(Integer.valueOf(userId), startDate, endDate, page,
                    limit);
            return JSONMessage.success(result);
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }

    }

    @RequestMapping(value = "/userList", method = {RequestMethod.POST, RequestMethod.GET})
    public JSONMessage userList(@RequestParam(defaultValue = "0") int page,
                                @RequestParam(defaultValue = "10") int limit, @RequestParam(defaultValue = "") String onlinestate,
                                @RequestParam(defaultValue = "") String keyWord, @RequestParam(defaultValue = "") String userIp,
                                @RequestParam(defaultValue = "") String startDate, @RequestParam(defaultValue = "") String endDate) {
        PageResult<User> result = new PageResult<User>();
        Query query = userManager.getUserDao().createQuery();
        if (!StringUtil.isEmpty(keyWord)) {
            // Integer 最大值2147483647
            boolean flag = NumberUtil.isNum(keyWord);
            if (flag) {
                Integer length = keyWord.length();
                if (length > 9) {
                    query.addCriteria(
                            new Criteria().orOperator(Criteria.where("nickname").regex(keyWord),
                                    Criteria.where("telephone").regex(keyWord),
                                    Criteria.where("account").regex(keyWord)));

                } else {
                    query.addCriteria(
                            new Criteria().orOperator(Criteria.where("nickname").regex(keyWord),
                                    Criteria.where("telephone").regex(keyWord),
                                    Criteria.where("_id").is(Integer.valueOf(keyWord)), Criteria.where("account").regex(keyWord)));

                }
            } else {
                query.addCriteria(
                        new Criteria().orOperator(Criteria.where("nickname").regex(keyWord),
                                Criteria.where("telephone").regex(keyWord), Criteria.where("account").regex(keyWord)));
            }
        }
        if (!StringUtil.isEmpty(onlinestate)) {

            query.addCriteria(Criteria.where("onlinestate").is(Integer.valueOf(onlinestate)));
        }
        if (!StringUtil.isEmpty(startDate) && !StringUtil.isEmpty(endDate)) {
            long startTime = 0; //开始时间（秒）
            long endTime = 0; //结束时间（秒）,默认为当前时间
            startTime = StringUtil.isEmpty(startDate) ? 0 : DateUtil.toDate(startDate).getTime() / 1000;
            endTime = StringUtil.isEmpty(endDate) ? DateUtil.currentTimeSeconds() : DateUtil.toDate(endDate).getTime() / 1000;
            long formateEndtime = DateUtil.getOnedayNextDay(endTime, 1, 0);
            query.addCriteria(Criteria.where("createTime").gt(startTime).lte(formateEndtime));
        }

        if (!StringUtil.isEmpty(userIp)) {
            List<User.UserLoginLog> userLoginLogs = mongoTemplate.find(new Query().addCriteria(Criteria.where("loginLog.ipAddress").is(userIp)), User.UserLoginLog.class);
            List<Integer> userIds = userLoginLogs.stream().map(User.UserLoginLog::getUserId).collect(Collectors.toList());
            query.addCriteria(Criteria.where("userId").in(userIds));
        }

        //总数量
        result.setCount(userManager.getUserDao().count(query));
        query.with(PageRequest.of(page - 1, limit));
        query.with(Sort.by(Sort.Order.desc("createTime")));
        List<User> pageData = userManager.getUserDao().queryListsByQuery(query);
        // 排序、分页
        pageData.forEach(userInfo -> {
            Query logLogQuery = userManager.getUserDao().createQuery("userId", userInfo.getUserId());
            User.UserLoginLog loginLog = userManager.getUserDao().getDatastore().findOne(logLogQuery, User.UserLoginLog.class);
            if (null != loginLog) {
                userInfo.setLoginLog(loginLog.getLoginLog());
                if (null != loginLog.getLoginLog()) {
                    userInfo.setIp(loginLog.getLoginLog().getIpAddress());
                }
                userInfo.setDeviceInfo(loginLog.getDeviceMap());
            }
            try {
                userInfo.setBalance(userManager.getUserMoenyV1(userInfo.getUserId()));
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
            userInfo.setPassword("");
            userInfo.setIsOpenResetPwd(SKBeanUtils.getImCoreService().getClientConfig().getIsOpenSecureChat());
        });
        result.setData(pageData);
        return JSONMessage.success(result);
    }

    @ApiOperation("所有用户总余额")
    @RequestMapping(value = "/user/balance")
    public JSONMessage userBalance() {
        List<User> users = userDao.allUser();
        BigDecimal totle = new BigDecimal(0.0);
        for (User user : users) {
            try {
                Double moenyV1 = userManager.getUserMoenyV1(user.getUserId());
                BigDecimal param = new BigDecimal(moenyV1);
                totle = totle.add(param);
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
        return JSONMessage.success(totle.toString());
    }


    /**
     * 最新注册用户
     *
     * @param pageIndex
     * @param pageSize
     * @return
     */
    @SuppressWarnings("deprecation")
    @RequestMapping(value = "/newRegisterUser")
    public JSONMessage newRegisterUser(@RequestParam(defaultValue = "0") int pageIndex,
                                       @RequestParam(defaultValue = "10") int pageSize) {
        Query query = userManager.getUserDao().createQuery();
        long total = userManager.getUserDao().count(query);
        query.with(PageRequest.of(pageIndex, pageSize));
        query.with(Sort.by(Sort.Order.desc("createTime")));
        List<User> pageData = userManager.getUserDao().queryListsByQuery(query);
        PageVO page = new PageVO(pageData, total, pageIndex, pageSize);
        return JSONMessage.success(null, page);
    }

    /**
     * 重置密码
     *
     * @param userId
     * @return
     */
    @RequestMapping(value = "/restPwd")
    public JSONMessage restPwd(@RequestParam(defaultValue = "0") Integer userId) {
        // 权限校验
        if (KConstants.systemAccount.ADMIN_CONSOLE_ACCOUNT == userId) {
            return JSONMessage.failure("不支持 修改该用户密码");
        }
        if (KConstants.ZERO < userId) {
            userManager.resetPassword(userId, Md5Util.md5Hex("123456"));
        }
        return JSONMessage.success();
    }

    /**
     * 修改admin密码
     *
     * @param userId
     * @return
     */
    @RequestMapping(value = "/updateAdminPassword")
    public JSONMessage updatePassword(@RequestParam(defaultValue = "") String oldPassword, @RequestParam(defaultValue = "0") Integer userId, String password) {
        try {
            User user = userManager.getUser(userId);
            if (oldPassword.equals(user.getPassword()) || LoginPassword.encodeFromOldPassword(oldPassword).equals(user.getPassword())) {
                userManager.resetPassword(userId, password);
                return JSONMessage.success();
            } else {
                return JSONMessage.failure("旧密码输入错误");
            }
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 修改用户密码
     *
     * @param userId
     * @param password
     * @return
     */
    @RequestMapping(value = "/updateUserPassword")
    public JSONMessage updateUserPassword(@RequestParam(defaultValue = "0") Integer userId, String password) {
        try {
            if (KConstants.systemAccount.ADMIN_CONSOLE_ACCOUNT == userId) {
                return JSONMessage.failure("不支持 修改该用户密码");
            }
            userManager.resetPassword(userId, password);
            return JSONMessage.success();
        } catch (Exception e) {

            return JSONMessage.failure(e.getMessage());
        }

    }

    /**
     * 好友列表
     *
     * @param userId
     * @return
     */
    @RequestMapping(value = "/friendsList")
    public JSONMessage friendsList(@RequestParam(defaultValue = "0") Integer userId,
                                   @RequestParam(defaultValue = "0") Integer toUserId, @RequestParam(defaultValue = "0") int status,
                                   @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "10") int limit) {
        try {
            PageResult<Friends> friendsList = friendsManager.consoleQueryFollow(userId, toUserId,
                    status, page, limit);
            return JSONMessage.success(friendsList);
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 删除好友
     *
     * @param userId
     * @param toUserIds
     * @return
     */
    @RequestMapping("/deleteFriends")
    public JSONMessage deleteFriends(@RequestParam(defaultValue = "0") Integer userId,
                                     @RequestParam(defaultValue = "") String toUserIds, @RequestParam(defaultValue = "") Integer adminUserId) {
        try {

            if (StringUtil.isEmpty(toUserIds)) {
                JSONMessage.failure("参数为空");
            } else {
                String[] toUserId = StringUtil.getStringList(toUserIds, ",");

                friendsManager.consoleDeleteFriends(userId, adminUserId, toUserId);
            }
            return JSONMessage.success();

        } catch (ServiceException e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    @RequestMapping(value = "/getUpdateUser")
    public JSONMessage updateUser(@RequestParam(defaultValue = "0") Integer userId) {
        if (KConstants.ZERO == userId) {
            return JSONMessage.success(new User());
        }

        User user = userManager.getUser(userId);
        List<Integer> userRoles = roleManager.getUserRoles(userId);

        Optional.ofNullable(userRoles).orElse(new ArrayList<>()).forEach(role -> {
            if (role.equals(2)) {
                user.setUserType(2);
            } else {
                user.setUserType(0);
            }
        });

        return JSONMessage.success(user);
    }

    @RequestMapping(value = "/updateUser")
    public JSONMessage saveUserMsg(HttpServletRequest request, HttpServletResponse response,
                                   @RequestParam(defaultValue = "0") Integer userId, @ModelAttribute UserExample example) throws Exception {
        try {
            boolean flag = keywordDAO.queryByWord(example.getNickname());
            if (flag) {
                throw new ServiceException(KConstants.ResultCode.NicknameWrongful);
            }

            if (!StringUtil.isEmpty(example.getTelephone())) {
                example.setPhone(example.getTelephone());
                example.setTelephone(example.getAreaCode() + example.getTelephone());
            }
            // 后台注册用户(后台注册传的密码没有加密，这里进行加密)
            if (!StringUtil.isEmpty(example.getPassword())) {
                example.setPassword(Md5Util.md5Hex(example.getPassword()));
            }

            // 保存到数据库
            if (0 == userId) {
                // 生成userId
                Integer id = userManager.createUserId();
                if (example.getUserType() == 2) {
                    if (userDao.exists("_id", id)) {
                        throw new ServiceException(KConstants.ResultCode.SystemIsBusy);
                    }
                    example.setUserId(id);
                }
                userManager.registerIMUser(example);
                if (example.getUserType() == 2) {
                    SecurityRole securityRole = securityRoleManager.querySecurityRoleByRoleName("公众号");
                    roleManager.updateUserRole(id, String.valueOf(securityRole.getRoleId()));

                    OfficialInfo officialInfo = officialInfoCheck.queryOne("userId", id);
                    if (officialInfo == null) {
                        OfficialInfo info = new OfficialInfo();
                        info.setTelephone(example.getTelephone());
                        info.setUserId(example.getUserId());
                        info.setAreaCode(example.getAreaCode());
                        info.setCreateTime(com.basic.im.comm.utils.DateUtil.currentTimeSeconds());
                        info.setAdminTelephone(example.getPhone());
                        info.setOfficialType(2);
                        info.setVerify(1);
                        info.setAdminName(example.getNickname());
                        SKBeanUtils.getDatastore().save(info);
                    }
                }
            } else {
                //判断该添加的用户是否是公众号,
                if (example.getUserType() == 2) {
                    SecurityRole securityRole = securityRoleManager.querySecurityRoleByRoleName("公众号");
                    Optional.ofNullable(securityRole)
                            .ifPresent(role -> {
                                example.setRoleId(String.valueOf(securityRole.getRoleId()));
                            });
                }
                userManager.updateUser(userId, example);
                // 修改好友关系表中的toUserType
//				roleManager.updateFriend(userId, example.getUserType());
            }
        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }
        return JSONMessage.success();
    }

    /**
     * @param page
     * @param limit
     * @return
     * @Description:（红包记录）
     **/
    @RequestMapping("/redPacketList")
    public JSONMessage getRedPacketList(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "10") int limit,
                                        @RequestParam(defaultValue = "") String userName, @RequestParam(defaultValue = "") String redPacketId,
                                        @RequestParam(defaultValue = "") String status) {
        if (redPacketsManager == null) {
            return JSONMessage.success(KConstants.ResultCode.CLOSEPAY, null);
        }
        try {
            if (page != 0) {
                page = page - 1;
            }
            PageResult<RedPacket> result = (PageResult<RedPacket>) redPacketsManager.getRedPacketList(userName, page, limit, redPacketId, status);
            return JSONMessage.success(result);
        } catch (ServiceException e) {
            return JSONMessage.failure(e.getErrMessage());
        }
    }

    @RequestMapping("/receiveWater")
    public JSONMessage receiveWater(@RequestParam(defaultValue = "") String redId,
                                    @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "10") int limit) {
        if (redPacketsManager == null) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.CLOSEPAY);
        }
        try {
            PageResult<RedReceive> result = (PageResult<RedReceive>) redPacketsManager.receiveWater(redId, page - 1, limit);
            return JSONMessage.success(result);
        } catch (ServiceException e) {
            return JSONMessage.failure(e.getErrMessage());
        }
    }

    @RequestMapping(value = "/addRoom")
    public JSONMessage addRomm(HttpServletRequest request, HttpServletResponse response, @ModelAttribute Room room,
                               @RequestParam(defaultValue = "") String ids) throws Exception {

        List<Integer> idList = StringUtil.isEmpty(ids) ? null : JSON.parseArray(ids, Integer.class);
        if (null == room.getId()) {
            User user = userManager.getUser(room.getUserId());
            String jid = com.basic.utils.StringUtil.randomUUID();
            messageService.createMucRoomToIMServer(jid, user.getPassword(), user.getUserId().toString(),
                    room.getName());
            room.setJid(jid);
            roomManager.add(user, room, idList, null);
        }

        return JSONMessage.success();
    }

    @RequestMapping(value = "/updateRoom")
    public JSONMessage updateRoom(HttpServletRequest request, HttpServletResponse response,
                                  @ModelAttribute RoomVO roomVo) throws Exception {
        try {
            User user = userManager.getUserDao().get(roomVo.getUserId());
            if (ObjectUtil.isEmpty(user)) {
                return JSONMessage.failure("操作失败");
            }
            roomManager.update(user, roomVo, 1, 1);
        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }

        return JSONMessage.success();
    }

    @RequestMapping(value = "/roomUserManager")
    public JSONMessage roomUserManager(@RequestParam(defaultValue = "0") int page,
                                       @RequestParam(defaultValue = "10") int limit, @RequestParam(defaultValue = "") String id) throws Exception {

        try {
            PageResult<Room.Member> result = null;
            if (!StringUtil.isEmpty(id)) {
                result = roomManager.getMemberListByPage(new ObjectId(id), page, limit);
            }
            return JSONMessage.success(result);
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }

    }

    @RequestMapping(value = "/roomMemberList")
    public JSONMessage roomMemberList(@RequestParam String id) {
        Object data = roomManager.getMemberList(new ObjectId(id), "");
        return JSONMessage.success(null, data);
    }

    @ApiOperation("合并群组")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "roomIds", value = "群组Id,分割", dataType = "int", defaultValue = "10"),
            @ApiImplicitParam(paramType = "query", name = "userId", value = "群主用户ID", dataType = "int", defaultValue = "10"),
            @ApiImplicitParam(paramType = "query", name = "roomName", value = "新群组名称 ", dataType = "String")
    })
    @RequestMapping("/mergeRoom")
    public JSONMessage mergeRoom(@RequestParam(name = "roomIds", defaultValue = "") String roomIds,
                                 @RequestParam(name = "userId", defaultValue = "0") Integer userId,
                                 @RequestParam(name = "roomName", defaultValue = "10") String roomName) {
        if (StrUtil.isEmpty(roomIds)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
        List<String> roomIdList = StringUtil.getListBySplit(roomIds, ",");
        if (2 > roomIdList.size()) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
        List<Room> roomList = new ArrayList<>();
        int roomUserCount = 0;
        int roomMaxUserSize = SKBeanUtils.getSystemConfig().getMaxUserSize();
        for (String roomId : roomIdList) {
            if (StrUtil.isEmpty(roomId) || !ObjectId.isValid(roomId) || !ObjectId.isValid(roomId)) {
                continue;
            }

            Room room = roomManager.getRoom(new ObjectId(roomId));
            if (room == null) {
                continue;
            }
            roomUserCount += room.getUserSize();
            if (roomUserCount > roomMaxUserSize) {
                throw new ServiceException(KConstants.ResultCode.RoomMemberAchieveMax);
            }
            roomList.add(room);
        }

        if (2 > roomList.size()) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }


        Room roomNew = roomManager.mergeRoom(roomList, roomName, userId);

        return JSONMessage.success(roomNew);
    }

    @RequestMapping(value = "/sendMessage", method = {RequestMethod.POST})
    public JSONMessage sendMssage(@RequestParam String body, Integer from, Integer to, Integer count) {
        
        logger.info("[管理员消息发送入口] 接收到管理员发送消息请求 - 发送者ID: {}, 接收者ID: {}, 消息内容长度: {}, 批量发送数量: {}", 
                   from, to, body != null ? body.length() : 0, count);
        logger.debug("[管理员消息发送入口] 消息内容: {}", body);
        
        try {
            if (null == from) {
                logger.info("[管理员消息发送入口] 批量发送模式 - 目标用户: {}, 发送数量: {}", to, count);
                List<Friends> uList = friendsManager.queryFriendsList(to, 0, 0, count);
                logger.info("[管理员消息发送入口] 查询到好友列表数量: {}", uList.size());
                
                ThreadUtils.executeInThread(obj -> {
                    User user = null;
                    MessageBean messageBean = null;
                    int successCount = 0;
                    int failCount = 0;

                    for (Friends friends : uList) {
                        try {
                            user = userManager.getUser(friends.getToUserId());
                            messageBean = new MessageBean();
                            messageBean.setType(1);
                            messageBean.setContent(body);
                            messageBean.setFromUserId(user.getUserId() + "");
                            messageBean.setFromUserName(user.getNickname());
                            messageBean.setToUserId(to.toString());
                            messageBean.setMsgType(0);
                            String messageId = StringUtil.randomUUID();
                            messageBean.setMessageId(messageId);
                            
                            logger.debug("[管理员消息发送入口] 批量发送消息 - 消息ID: {}, 发送者: {}, 接收者: {}", 
                                       messageId, user.getUserId(), to);
                            messageService.send(messageBean);
                            successCount++;
                            logger.debug("[管理员消息发送入口] 批量发送成功 - 消息ID: {}", messageId);
                        } catch (Exception e) {
                            failCount++;
                            logger.error("[管理员消息发送入口] 批量发送失败 - 发送者: {}, 接收者: {}, 异常: {}", 
                                       user != null ? user.getUserId() : "未知", to, e.getMessage(), e);
                        }
                    }
                    logger.info("[管理员消息发送入口] 批量发送完成 - 成功: {}, 失败: {}, 总数: {}", 
                               successCount, failCount, uList.size());
                });
            } else {
                logger.info("[管理员消息发送入口] 单条发送模式 - 发送者ID: {}, 接收者ID: {}", from, to);
                ThreadUtils.executeInThread(obj -> {
                    try {
                        User user = userManager.getUserDao().get(from);
                        if (user == null) {
                            logger.error("[管理员消息发送入口] 发送者用户不存在 - 用户ID: {}", from);
                            return;
                        }
                        
                        MessageBean messageBean = new MessageBean();
                        messageBean.setContent(body);
                        messageBean.setFromUserId(user.getUserId().toString());
                        messageBean.setFromUserName(user.getNickname());
                        messageBean.setToUserId(to.toString());
                        messageBean.setMsgType(0);
                        String messageId = StringUtil.randomUUID();
                        messageBean.setMessageId(messageId);
                        
                        logger.info("[管理员消息发送入口] 开始发送单条消息 - 消息ID: {}, 发送者: {}, 接收者: {}", 
                                   messageId, from, to);
                        messageService.send(messageBean);
                        logger.info("[管理员消息发送入口] 单条消息发送成功 - 消息ID: {}", messageId);
                    } catch (Exception e) {
                        logger.error("[管理员消息发送入口] 单条消息发送失败 - 发送者: {}, 接收者: {}, 异常: {}", 
                                   from, to, e.getMessage(), e);
                    }
                });
            }
            logger.info("[管理员消息发送入口] 消息发送请求处理完成 - 发送者: {}, 接收者: {}", from, to);
            return JSONMessage.success();
        } catch (Exception e) {
            logger.error("[管理员消息发送入口] 消息发送请求处理异常 - 发送者: {}, 接收者: {}, 异常: {}", 
                        from, to, e.getMessage(), e);
        }
        return JSONMessage.success();
    }


    private byte[] generateId(String username) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        return md.digest(username.getBytes());
    }


    /**
     * 直播间列表
     *
     * @param name
     * @param nickName
     * @param userId
     * @param page
     * @param limit
     * @param status
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/liveRoomList")
    public JSONMessage liveRoomList(@RequestParam(defaultValue = "") String name, @RequestParam(defaultValue = "") String nickName,
                                    @RequestParam(defaultValue = "0") Integer userId, @RequestParam(defaultValue = "0") Integer page,
                                    @RequestParam(defaultValue = "10") Integer limit, @RequestParam(defaultValue = "-1") Integer status) throws Exception {
        if (paymentManager == null) {
            return JSONMessage.success(KConstants.ResultCode.CLOSEPAY, null);
        }
        PageResult<LiveRoom> result;
        try {
            result = liveRoomManager.findConsoleLiveRoomList(name, nickName, userId, page, limit, status, 1);
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
        return JSONMessage.success(result);
    }

    /**
     * 查询直播间
     *
     * @param userId
     * @param page
     * @param limit
     * @return
     */
    @RequestMapping(value = "/find/liveroom")
    public JSONMessage findLiveRoomListByUserId(@RequestParam(defaultValue = "") String userId, @RequestParam(defaultValue = "1") Integer page, @RequestParam(defaultValue = "10") Integer limit) {
        return JSONMessage.success(liveRoomManager.findLiveRoomListByUserId(userId, page, limit));
    }

    /**
     * 保存新增直播间
     *
     * @param liveRoom
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/saveNewLiveRoom", method = {RequestMethod.POST})
    public JSONMessage saveNewLiveRoom(LiveRoom liveRoom) {
        try {
            liveRoomManager.createLiveRoom(liveRoom);
        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }

        return JSONMessage.success();
    }

    /**
     * 删除直播间
     *
     * @param liveRoomId
     * @return
     */
    @RequestMapping(value = "/deleteLiveRoom", method = {RequestMethod.POST})
    public JSONMessage deleteLiveRoom(@RequestParam String liveRoomId) {
        try {
            liveRoomManager.deleteLiveRoom(new ObjectId(liveRoomId));
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }
    }

    /**
     * 锁定、解锁直播间
     *
     * @param liveRoomId
     * @param currentState
     * @return
     */
    @RequestMapping(value = "/operationLiveRoom")
    public JSONMessage operationLiveRoom(@RequestParam int liveRoomId,
                                         @RequestParam(defaultValue = "0") int currentState) {
        try {
            liveRoomManager.operationLiveRoom(liveRoomId, currentState);
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }
    }

    /**
     * 修改直播间推流地址
     *
     * @param liveRoomId
     * @param newRtmpUrl
     * @return
     */
    @RequestMapping(value = "/updateLiveRoomRtmpUrl")
    public JSONMessage modifyLiveRoomRtmpUrl(@RequestParam String liveRoomId, @RequestParam String newRtmpUrl) {
        try {

            liveRoomManager.modifyRtmpUrl(new ObjectId(liveRoomId), newRtmpUrl);
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }
    }

    /**
     * 查询直播间人员
     *
     * @param pageIndex
     * @param name
     * @param nickName
     * @param userId
     * @param pageSize
     * @param roomId
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/liveRoomUserManager")
    public JSONMessage liveRoomManager(@RequestParam(defaultValue = "0") Integer pageIndex,
                                       @RequestParam(defaultValue = "") String name, @RequestParam(defaultValue = "") String nickName,
                                       @RequestParam(defaultValue = "0") Integer userId, @RequestParam(defaultValue = "10") Integer pageSize,
                                       @RequestParam(defaultValue = "") String roomId) throws Exception {
        List<LiveRoom.LiveRoomMember> pageData = Lists.newArrayList();
        pageData = liveRoomManager.findLiveRoomMemberList(new ObjectId(roomId), pageIndex, pageSize);
        PageResult<LiveRoom.LiveRoomMember> result = new PageResult<LiveRoom.LiveRoomMember>();
        result.setData(pageData);
        result.setCount(pageData.size());
        return JSONMessage.success(result);
    }

    /**
     * 删除直播间成员
     *
     * @param userId
     * @param liveRoomId
     * @param response
     * @param pageIndex
     * @return
     */
    @RequestMapping(value = "/deleteRoomUser")
    public JSONMessage deleteliveRoomUserManager(@RequestParam Integer userId,
                                                 @RequestParam(defaultValue = "") String liveRoomId, HttpServletResponse response,
                                                 @RequestParam(defaultValue = "0") int pageIndex) {
        try {
            liveRoomManager.kick(userId, new ObjectId(liveRoomId));
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }
    }


    /**
     * 礼物列表
     *
     * @param name
     * @param page
     * @param limit
     * @return
     */
    @SuppressWarnings("unchecked")
    @RequestMapping(value = "/giftList")
    public JSONMessage giftList(@RequestParam(defaultValue = "") String name,
                                @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "10") int limit) {
        if (ObjectUtil.isEmpty(paymentManager)) {
            return JSONMessage.failureByErrCodeAndData(KConstants.ResultCode.CLOSEPAY, null);
        }
        return JSONMessage.success(liveRoomManager.findGiftList(name, page, limit));
    }

    /**
     * 添加礼物
     *
     * @param request
     * @param response
     * @param name
     * @param photo
     * @param price
     * @param type
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/add/gift", method = {RequestMethod.POST})
    public JSONMessage addGift(HttpServletRequest request, HttpServletResponse response, @RequestParam String name,
                               @RequestParam String photo, @RequestParam double price, @RequestParam(defaultValue = "0") int type) throws IOException {
        try {
            liveRoomManager.addGift(name, photo, price, type);
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }
    }

    /**
     * 删除礼物
     *
     * @param giftId
     * @return
     */
    @RequestMapping(value = "/delete/gift")
    public JSONMessage deleteGift(@RequestParam String giftId) {
        try {
            liveRoomManager.deleteGift(new ObjectId(giftId));
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }

    }


    /**
     * 查询提示信息列表
     *
     * @param keyword
     * @param page
     * @param limit
     * @return
     */
    @RequestMapping(value = "/hitiInfoList")
    public JSONMessage hitiInfoList(String keyword, @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "10") int limit) {
        AtomicLong totalNum = new AtomicLong();
        PageResult<ErrorMessage> result = new PageResult<ErrorMessage>();
        Map<Long, List<ErrorMessage>> errorMessageList = errorMessageManage.findErrorMessage(keyword, page - 1, limit);

        Optional.ofNullable(errorMessageList.keySet()).orElse(new HashSet<>()).forEach(total -> {
            totalNum.set(total);
            result.setData(errorMessageList.get(total));
        });
        result.setCount(totalNum.get());
        return JSONMessage.success(result);
    }

    /**
     * 新增提示消息
     *
     * @param errorMessage
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/saveErrorMessage")
    public JSONMessage saveErrorMessage(ErrorMessage errorMessage) throws IOException {
        try {
            if (ObjectUtil.isEmpty(errorMessage)) {
                return JSONMessage.failure("参数有误");
            }
            return errorMessageManage.saveErrorMessage(errorMessage);
        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }
    }

    /**
     * 修改提示消息
     *
     * @param errorMessage
     * @param id
     * @return
     */
    @RequestMapping(value = "/hitiInfoUpdate", method = {RequestMethod.POST})
    public JSONMessage hitiInfoUpdate(ErrorMessage errorMessage, String id) {
        if (ObjectUtil.isEmpty(errorMessage)) {
            return JSONMessage.failure("参数有误： errorMessage " + errorMessage);
        }
        ErrorMessage data = errorMessageManage.updataErrorMessage(id, errorMessage);
        if (ObjectUtil.isEmpty(data)) {
            return JSONMessage.failure("修改提示消息失败");
        }
        return JSONMessage.success("修改提示消息成功", data);
    }

    /**
     * 删除提示消息
     *
     * @param code
     * @return
     */
    @RequestMapping(value = "/deleteErrorMessage")
    public JSONMessage deleteErrorMessage(@RequestParam(defaultValue = "") String code) {
        if (StringUtil.isEmpty(code)) {
            return JSONMessage.failure("参数有误,code: " + code);
        }
        boolean falg = errorMessageManage.deleteErrorMessage(code);
        if (!falg) {
            return JSONMessage.failure("删除提示消息失败");
        }
        return JSONMessage.success();
    }

    /**
     * 关键词(敏感词)列表
     *
     * @param word
     * @param pageIndex
     * @param pageSize
     * @return
     */
    @RequestMapping(value = "/keywordfilter")
    public JSONMessage keywordfilter(@RequestParam(defaultValue = "") String word, @RequestParam(defaultValue = "0") int pageIndex,
                                     @RequestParam(defaultValue = "10") int pageSize) {
        PageResult<KeyWord> pageResult = keywordDAO.queryKeywordPageResult(word, pageIndex + 1, pageSize);
        return JSONMessage.success(new PageVO(pageResult.getData(), pageResult.getCount(), pageIndex, pageSize));
    }

    /**
     * 消息拦截记录列表
     *
     * @param userId
     * @param toUserId
     * @param page
     * @param limit
     * @param type
     * @param content
     * @return
     */
    @RequestMapping(value = "/keywordDenyRecordList")
    public JSONMessage keywordDenyRecordList(@RequestParam(defaultValue = "") Integer userId, @RequestParam(defaultValue = "") String toUserId,
                                             @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "10") int limit, @RequestParam(defaultValue = "0") int type,
                                             @RequestParam(defaultValue = "") String content) {
        PageResult<KeywordDenyRecord> data = keywordManager.webQueryKeywordDenyRecordList(userId, toUserId, page, limit, type, content);
        return JSONMessage.success(data);
    }

    /**
     * 删除消息拦截记录
     *
     * @param id
     * @return
     */
    @RequestMapping(value = "/deleteMsgIntercept")
    public JSONMessage deleteKeywordIntercept(@RequestParam(defaultValue = "") String id) {
        keywordManager.deleteMsgIntercept(new ObjectId(id));
        return JSONMessage.success();
    }

    @RequestMapping("/sendMsg")
    public JSONMessage sendMsg(@RequestParam(defaultValue = "") String jidArr,
                               @RequestParam(defaultValue = "1") int userId, @RequestParam(defaultValue = "1") int type,
                               @RequestParam(defaultValue = "") String content) {
        
        logger.info("[管理端群发消息入口] 接收到群发消息请求 - 发送者ID: {}, 消息类型: {}, 目标群组: {}, 内容长度: {}", 
                   userId, type, jidArr, content != null ? content.length() : 0);
        logger.debug("[管理端群发消息入口] 消息内容: {}", content);
        
        if (StringUtil.isEmpty(jidArr)) {
            logger.warn("[管理端群发消息入口] 参数验证失败 - 群组JID列表为空");
            return JSONMessage.failure("群组JID列表不能为空");
        }
        
        if (StringUtil.isEmpty(content)) {
            logger.warn("[管理端群发消息入口] 参数验证失败 - 消息内容为空");
            return JSONMessage.failure("消息内容不能为空");
        }
        
        try {
            String[] split = jidArr.split(",");
            logger.info("[管理端群发消息入口] 解析群组列表完成 - 目标群组数量: {}, 群组列表: {}", 
                       split.length, Arrays.toString(split));
            
            logger.info("[管理端群发消息入口] 开始调用RoomManager.sendMsgToRooms - 发送者: {}, 群组数量: {}", 
                       userId, split.length);
            roomManager.sendMsgToRooms(split, userId, type, content);
            logger.info("[管理端群发消息入口] RoomManager.sendMsgToRooms调用成功 - 发送者: {}, 群组数量: {}", 
                       userId, split.length);
            
            logger.info("[管理端群发消息入口] 群发消息处理完成 - 发送者: {}, 成功发送到 {} 个群组", 
                       userId, split.length);
            return JSONMessage.success();
            
        } catch (Exception e) {
            logger.error("[管理端群发消息入口] 群发消息处理异常 - 发送者: {}, 目标群组: {}, 异常信息: {}", 
                        userId, jidArr, e.getMessage(), e);
            return JSONMessage.failure("群发消息失败: " + e.getMessage());
        }
    }

    @RequestMapping("/sendUserMsg")
    public JSONMessage sendUserMsg(@RequestParam(defaultValue = "") int toUserId, @RequestParam(defaultValue = "1") int type,
                                   @RequestParam(defaultValue = "") String content) {
        try {
            adminManager.sendMsgToUser(toUserId, type, content);
            return JSONMessage.success();
        } catch (ServiceException e) {
            return JSONMessage.failureByErrCode(e.getResultCode());
        }
    }


    /**
     * 添加敏感词
     *
     * @param words
     * @param type
     * @throws IOException
     * @throws ServletException
     */
    @RequestMapping(value = "/addkeyword", method = {RequestMethod.POST})
    public JSONMessage addkeyword(@RequestParam String words, @RequestParam Short type) throws IOException {
        String[] keyWordArray = words.split("，|,");
        for (int i = 0; i < keyWordArray.length; i++) {
            KeyWord word = keywordDAO.findOne(KeyWord.class, "word", keyWordArray[i]);
            if (!ObjectUtil.isEmpty(word)) {
                continue;
            }
            KeyWord keyword = new KeyWord();
            keyword.setType(type);
            keyword.setCreateTime(DateUtil.currentTimeSeconds());
            keyword.setWord(keyWordArray[i]);
            keywordDAO.getDatastore().save(keyword);
        }
        keywordManager.sendRefreshSysConfig();
        return JSONMessage.success("添加成功");
    }

    /**
     * 删除敏感词
     *
     * @param id
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/deletekeyword", method = {RequestMethod.POST})
    public JSONMessage deletekeyword(@RequestParam String id) {
        keywordManager.deleteKeyword(new ObjectId(id));
        return JSONMessage.success();
    }

    /**
     * 删除聊天记录
     *
     * @param request
     * @param response
     * @param startTime
     * @param endTime
     * @param room_jid_id
     * @throws Exception
     */
    @RequestMapping(value = "/deleteMsgGroup", method = {RequestMethod.POST})
    public void deleteMsgGroup(HttpServletRequest request, HttpServletResponse response,
                               @RequestParam(defaultValue = "0") long startTime, @RequestParam(defaultValue = "0") long endTime,
                               @RequestParam(defaultValue = "") String room_jid_id) throws Exception {
        tigaseMsgDao.deleteGroupMsgBytime(startTime, endTime, room_jid_id);
    }

    /**
     * 后台自动创建用户
     *
     * @param userNum 需要生成的数量
     * @param roomId
     */
    @RequestMapping(value = "/autoCreateUser")
    public JSONMessage autoCreateUser(@RequestParam(defaultValue = "10") int userNum, @RequestParam(defaultValue = "") String roomId) {
        if (userNum > 0) {
            if (!StringUtil.isEmpty(roomId)) {
                ObjectId objId = parse(roomId);
                Room room = roomManager.getRoomDao().get(objId);
                if (ObjectUtil.isEmpty(room)) {
                    return JSONMessage.failureByErrCode(KConstants.ResultCode.NotRoom);
                }
                if (room.getMaxUserSize() < room.getUserSize() + userNum) {
                    return JSONMessage.failure("群人数快到上限  最多还可以邀请  " + (room.getMaxUserSize() - room.getUserSize()) + "人");
                }
            }

            SecurityRole securityRole = securityRoleManager.querySecurityRoleByRoleName("机器人");
            if (!ObjectUtil.isEmpty(securityRole)) {
                userManager.autoCreateUserOrRoom(userNum, roomId, ReqUtil.getUserId(), String.valueOf(securityRole.getRoleId()));
            } else {
                userManager.autoCreateUserOrRoom(userNum, roomId, ReqUtil.getUserId(), "");
            }
            return JSONMessage.success();
        }

        return JSONMessage.failure("至少输入1个");
    }

    /**
     * 导出自动创建的用户数据到 excel
     */
    @RequestMapping(value = "/exportData", method = RequestMethod.POST)
    public JSONMessage exportData(HttpServletRequest request, HttpServletResponse response,
                                  @RequestParam(defaultValue = "3") short userType) {
        String fileName = "users.xlsx";

        int maxNum = 30000; // 最多导出3万条数据
        short onlinestate = -1;

        List<User> userList = userManager.findUserList(0, maxNum, "", onlinestate, userType);

        String name = "系统自动创建的账号";
        List<String> titles = Lists.newArrayList();
        titles.add("userId");
        titles.add("nickname");
        titles.add("telephone");
        titles.add("password");
        titles.add("sex");
        titles.add("createTime");

        List<Map<String, Object>> values = Lists.newArrayList();
        for (User user : userList) {
            Map<String, Object> map = Maps.newHashMap();
            map.put("userId", user.getUserId());
            map.put("nickname", user.getNickname());
            map.put("telephone", user.getTelephone());
            map.put("password", user.getUserType() == 3 ? "" + (user.getUserId() - 1000) / 2 : user.getPassword());
            map.put("sex", user.getSex() == 1 ? "女" : "男");
            map.put("createTime", Calendar.getInstance());
            values.add(map);
        }

        Workbook workBook = ExcelUtil.generateWorkbook(name, "xlsx", titles, values);
        try {
            response.reset();
            response.setHeader("Content-Disposition",
                    "attachment;filename=" + new String(fileName.getBytes(), "utf-8"));
            ServletOutputStream out = response.getOutputStream();
            workBook.write(out);
            // 弹出下载对话框
            out.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return JSONMessage.success();
    }

    @RequestMapping(value = "/exportExcelByFriends", method = RequestMethod.POST)
    public JSONMessage exportExcelByFriends(HttpServletRequest request, HttpServletResponse response,
                                            @RequestParam(defaultValue = "0") Integer userId) {
        try {
            Workbook workBook = adminManager.exprotExcelFriends(userId, request, response);
            ServletOutputStream out = response.getOutputStream();
            workBook.write(out);
            // 弹出下载对话框
            out.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return JSONMessage.success();
    }

    /**
     * @param request
     * @param response
     * @return
     * @Description: 导出手机号明细
     */
    @RequestMapping(value = "/exportExcelByPhone", method = RequestMethod.POST)
    public JSONMessage exportExcelByPhone(HttpServletRequest request, HttpServletResponse response, @RequestParam(defaultValue = "") String startDate,
                                          @RequestParam(defaultValue = "") String endDate, @RequestParam(defaultValue = "") String onlinestate, @RequestParam(defaultValue = "") String keyWord) {
        try {
            Workbook workBook = adminManager.exprotExcelPhone(startDate, endDate, onlinestate, keyWord, request, response);
            ServletOutputStream out = response.getOutputStream();
            workBook.write(out);
            // 弹出下载对话框
            out.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return JSONMessage.success();
    }


    /**
     * @param request
     * @param response
     * @param roomId
     * @return
     * @Description:导出群成员
     **/
    @RequestMapping(value = "/exportExcelByGroupMember", method = RequestMethod.POST)
    public JSONMessage exportExcelByGroupMember(HttpServletRequest request, HttpServletResponse response,
                                                @RequestParam(defaultValue = "") String roomId) {
        try {
            Workbook workBook = adminManager.exprotExcelGroupMembers(roomId, request, response);
            ServletOutputStream out = response.getOutputStream();
            workBook.write(out);
            // 弹出下载对话框
            out.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return JSONMessage.success();
    }

    /**
     * 统计用户注册信息
     */
    @RequestMapping(value = "/getUserRegisterCount")
    public JSONMessage getUserRegisterCount(@RequestParam(defaultValue = "2") short timeUnit, @RequestParam(defaultValue = "") String startDate,
                                            @RequestParam(defaultValue = "") String endDate) {
        try {
            Object data = userManager.getUserRegisterCount(startDate.trim(), endDate.trim(), timeUnit);
            return JSONMessage.success(null, data);
        } catch (MongoCommandException e) {
            return JSONMessage.success(0);
        } catch (ServiceException e) {
            return JSONMessage.failure(e.getMessage());
        }

    }

    /**
     * 用户，群组，单聊消息，好友关系数量 统计
     */
    @RequestMapping(value = "/countNum")
    public JSONMessage countNum(HttpServletRequest request, HttpServletResponse response) {
        try {
            long userNum = userManager.getUserDao().count();
            long roomNum = roomManager.countRoomNum();
            long msgNum = messageRepository.getMsgCountNum();
            long friendsNum = friendsManager.getFriendsDao().queryAllFriendsCount();
            long servenActiveCount = userManager.getUserDao().servenUserActiveCount();
            /*long maxOnlineCount = userStatusCountDao.maxOnlineCount();*/
            /*long servenNewUser = userDao.servenNewUser_();*/
            long todayLoginNumber = userDao.todayLoginNumber();
            long liveNum = liveRoomManager.liveCount();

            Map<String, Long> dataMap = new HashMap<String, Long>();
            dataMap.put("userNum", userNum);
            dataMap.put("roomNum", roomNum);
            dataMap.put("msgNum", msgNum);
            dataMap.put("friendsNum", friendsNum);
            dataMap.put("servenActiveCount", servenActiveCount);
            /*dataMap.put("maxOnlineCount", maxOnlineCount);*/
            /*dataMap.put("servenNewUser", servenNewUser);*/
            dataMap.put("todayLoginNumber", todayLoginNumber);
            dataMap.put("liveNum", liveNum);
            return JSONMessage.success(null, dataMap);

        } catch (MongoCommandException e) {
            return JSONMessage.success(0);
        } catch (ServiceException e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 查询七日用户活跃量 ， 七日用户新增
     */
    @RequestMapping(value = "/serven/statistics")
    public JSONMessage servenStatistics() {
        Map<String, Object> result = new HashMap<>();
        //七日用户活跃量
        Map<String, Object> servenUserActive = userDao.servenUserActive();
        //七日用户新增
        Map<String, Object> servenNew = userDao.servenNewUser();
        result.put("servenUserActive", servenUserActive);
        result.put("servenNew", servenNew);
        return JSONMessage.success(result);
    }

    /**
     * 在线人数
     *
     * @return
     */
    @RequestMapping(value = "/online/user/count")
    public JSONMessage onlimeUserCount() {
        Map<String, Object> result = new HashMap<>();
        //当前在线数量
        long todayLoginNumber = userDao.todayLoginNumber();
        //最高在线数量
        long maxOnlineCount = userStatusCountDao.maxOnlineCount();
        result.put("todayLoginNumber", todayLoginNumber);
        result.put("maxOnlineCount", maxOnlineCount);
        return JSONMessage.success(result);
    }


    /**
     * 统计单聊消息数量
     */
    @RequestMapping(value = "/chatMsgCount")
    public JSONMessage chatMsgCount(@RequestParam(defaultValue = "2") short timeUnit,
                                    @RequestParam(defaultValue = "") String startDate, @RequestParam(defaultValue = "") String endDate) {

        try {
            Object data = messageRepository.getChatMsgCount(startDate.trim(), endDate.trim(), timeUnit);
            return JSONMessage.success(data);

        } catch (MongoCommandException e) {
            return JSONMessage.success();
        } catch (ServiceException e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 统计群聊聊消息数量
     */
    @RequestMapping(value = "/groupMsgCount")
    public JSONMessage groupMsgCount(@RequestParam String roomId, @RequestParam(defaultValue = "0") int pageIndex,
                                     @RequestParam(defaultValue = "100") int pageSize, @RequestParam(defaultValue = "2") short timeUnit,
                                     @RequestParam(defaultValue = "") String startDate, @RequestParam(defaultValue = "") String endDate) {
        try {
            Object data = messageRepository.getGroupMsgCount(roomId, startDate.trim(), endDate.trim(),
                    timeUnit);
            return JSONMessage.success(data);
        } catch (MongoCommandException e) {
            return JSONMessage.success(0);
        } catch (ServiceException e) {
            return JSONMessage.failure(e.getMessage());
        }

    }

    /**
     * 统计添加好友数量
     */
    @RequestMapping(value = "/addFriendsCount")
    public JSONMessage addFriendsCount(@RequestParam(defaultValue = "0") int pageIndex,
                                       @RequestParam(defaultValue = "100") int pageSize, @RequestParam(defaultValue = "2") short timeUnit,
                                       @RequestParam(defaultValue = "") String startDate, @RequestParam(defaultValue = "") String endDate) {
        try {

            Object data = friendsManager.getAddFriendsCount(startDate.trim(), endDate.trim(),
                    timeUnit);
            return JSONMessage.success(null, data);
        } catch (MongoCommandException e) {
            return JSONMessage.success(0);
        } catch (ServiceException e) {
            return JSONMessage.failure(e.getMessage());
        }

    }

    /**
     * 统计创建群组数量
     */
    @RequestMapping(value = "/addRoomsCount")
    public JSONMessage addRoomsCount(@RequestParam(defaultValue = "0") int pageIndex,
                                     @RequestParam(defaultValue = "100") int pageSize, @RequestParam(defaultValue = "2") short timeUnit,
                                     @RequestParam(defaultValue = "") String startDate, @RequestParam(defaultValue = "") String endDate) {

        try {
            Object data = roomManager.addRoomsCount(startDate.trim(), endDate.trim(), timeUnit);
            return JSONMessage.success(data);
        } catch (MongoCommandException e) {
            return JSONMessage.success(0);
        } catch (ServiceException e) {
            return JSONMessage.failure(e.getMessage());
        }

    }

    /**
     * 用户在线数量统计
     *
     * @param pageIndex
     * @param pageSize
     * @param startDate
     * @param endDate
     * @param timeUnit
     * @throws Exception
     */
    @RequestMapping(value = "/getUserStatusCount")
    public JSONMessage getUserStatusCount(@RequestParam(defaultValue = "0") int pageIndex,
                                          @RequestParam(defaultValue = "100") int pageSize, @RequestParam(defaultValue = "2") short timeUnit,
                                          @RequestParam(defaultValue = "") String startDate, @RequestParam(defaultValue = "") String endDate) throws Exception {
        try {
            Object data = userManager.userOnlineStatusCount(startDate.trim(), endDate.trim(),
                    timeUnit);
            return JSONMessage.success(null, data);
        } catch (MongoCommandException e) {
            return JSONMessage.success(0);
        } catch (ServiceException e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * @param type (type = 0查询被举报的用户,type=1查询被举报的群主,type=2查询被举报的网页)
     * @param
     * @return
     * @Description:（被举报的用户和群组列表）
     **/
    @RequestMapping(value = "/beReport")
    public JSONMessage beReport(@RequestParam(defaultValue = "0") int type,
                                @RequestParam(defaultValue = "0") int sender, @RequestParam(defaultValue = "") String receiver,
                                @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "25") int limit) {
        PageResult<Report> pageResult;
        try {
            page = page - 1;
            pageResult = userManager.getReport(type, sender, receiver, page, limit);
            return JSONMessage.success(pageResult);
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }
    }


    @RequestMapping(value = "/webUrlRosterList")
    public JSONMessage webUrlRosterList(@RequestParam(defaultValue = "") String webUrl, @RequestParam(defaultValue = "0") byte webUrlType,
                                        @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "25") int limit) {
        try {
            PageResult<WebUrlRoster> data = webUrlRosterManager.getWebUrlRosterList(webUrl, webUrlType, page - 1, limit);
            return JSONMessage.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }
    }

    @RequestMapping(value = "/addWebUrlRoster")
    public JSONMessage addWebUrlRoster(@RequestParam(defaultValue = "") String webUrl, @RequestParam(defaultValue = "0") byte webUrlType) {
        try {
            webUrlRosterManager.addWebUrlRoster(webUrl, webUrlType);
            return JSONMessage.success(new Object());
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }
    }

    @RequestMapping(value = "/deleteWebUrlRoster")
    public JSONMessage deleteUrlRosterList(@RequestParam String id) {
        try {
            if (!ObjectId.isValid(id)) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
            }
            webUrlRosterManager.deleteWebUrlRoster(new ObjectId(id));
            return JSONMessage.success();
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }
    }

    @RequestMapping(value = "/updateWebUrlRosterStatus")
    public JSONMessage updateWebUrlRosterStatus(@RequestParam String id, @RequestParam byte webUrlType) {
        try {
            webUrlRosterManager.updateWebUrlRosterStatus(new ObjectId(id), webUrlType);
            return JSONMessage.success();
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }
    }


    @RequestMapping("/isLockWebUrl")
    public JSONMessage isLockWebUrl(@RequestParam(defaultValue = "") String webUrlId, @RequestParam(defaultValue = "-1") int webStatus) {
        if (StringUtil.isEmpty(webUrlId)) {
            return JSONMessage.failure("webUrl is null");
        }
        ObjectId objectId = new ObjectId(webUrlId);
        Report report = reportDao.getReport(objectId);
        if (ObjectUtil.isEmpty(report)) {
            return JSONMessage.failure("暂无该链接的举报数据");
        }
        //添加一条url黑名单记录
        webUrlRosterManager.addWebUrlRosterRecord(report.getWebUrl(), (byte) -1);
        return JSONMessage.success();
    }

    /**
     * 删除举报
     *
     * @param response
     * @param id
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/deleteReport")
    public JSONMessage deleteReport(HttpServletResponse response, @RequestParam String id) throws IOException {
        try {
            Report report = reportDao.getReport(parse(id));
            if (ObjectUtil.isEmpty(report)) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.DataNotExists);
            }
            reportDao.deleteReportById(report.getId());
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * controller 调用日志
     *
     * @param keyWorld
     * @param page
     * @param limit
     * @return
     */
    @RequestMapping(value = "/ApiLogList")
    public JSONMessage apiLogList(@RequestParam(defaultValue = "") String keyWorld,
                                  @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "15") int limit) {

        try {
            PageResult<SysApiLog> data = adminManager.apiLogList(keyWorld, page, limit);
            return JSONMessage.success(data);

        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }

    }

    /**
     * 删除 controller 日志
     *
     * @param apiLogId
     * @param type
     * @return
     */
    @RequestMapping(value = "/delApiLog")
    public JSONMessage delApiLog(@RequestParam(defaultValue = "") String apiLogId,
                                 @RequestParam(defaultValue = "0") int type) {

        try {

            adminManager.deleteApiLog(apiLogId, type);
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * @param limit
     * @param page
     * @return
     * @Description:（后台朋友圈列表）
     **/
    @RequestMapping(value = "/getFriendsMsgList")
    public JSONMessage getFriendsMsgList(@RequestParam(defaultValue = "0") Integer page, @RequestParam(defaultValue = "10") Integer limit,
                                         @RequestParam(defaultValue = "") String nickname, @RequestParam(defaultValue = "0") Integer userId,
                                         @RequestParam(defaultValue = "") String type) {
        try {
            PageResult<Msg> data = msgManager.getMsgList(page, limit, nickname, userId, type);
            return JSONMessage.success(data);
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * @param messageId
     * @return
     * @Description:（删除朋友圈）
     **/
    @RequestMapping(value = "/deleteFriendsMsg")
    public JSONMessage deleteMsg(@RequestParam String messageId) {
        if (StringUtil.isEmpty(messageId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        } else {
            try {
                String[] messageIds = StringUtil.getStringList(messageId);
                msgManager.delete(messageIds);
            } catch (Exception e) {
                logger.error("删除朋友圈消息失败", e);
                return JSONMessage.failure(e.getMessage());
            }
        }
        return JSONMessage.success();
    }

    /**
     * @param state
     * @param msgId
     * @return
     * @Description:（锁定朋友圈）
     **/
    @RequestMapping(value = "/lockingMsg")
    public JSONMessage lockingMsg(@RequestParam String msgId, @RequestParam int state) {
        try {
            msgManager.lockingMsg(new ObjectId(msgId), state);
            return JSONMessage.success();
        } catch (ServiceException e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * @return
     * @Description:（朋友圈评论）
     **/
    @RequestMapping(value = "/commonListMsg")
    public JSONMessage commonListMsg(@RequestParam String msgId, @RequestParam(defaultValue = "0") Integer page,
                                     @RequestParam(defaultValue = "10") Integer limit) {
        try {
            PageResult<Comment> result = commentManager.commonListMsg(new ObjectId(msgId), page, limit);
            return JSONMessage.success(result);
        } catch (ServiceException e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * @param page
     * @param limit
     * @param msgId
     * @return
     * @Description:（朋友圈点赞）
     **/
    @RequestMapping(value = "/praiseListMsg")
    public JSONMessage praiseListMsg(@RequestParam String msgId, @RequestParam(defaultValue = "0") Integer page,
                                     @RequestParam(defaultValue = "10") Integer limit) {
        try {
            PageResult<Praise> result = msgPraiseManager.praiseListMsg(new ObjectId(msgId), page, limit);
            return JSONMessage.success(result);
        } catch (ServiceException e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * @param messageId
     * @param commentId
     * @return
     * @Description:（删除评论）
     **/
    @RequestMapping(value = "/comment/delete")
    public JSONMessage deleteComment(@RequestParam String messageId, @RequestParam String commentId) {
        if (StringUtil.isEmpty(messageId) || StringUtil.isEmpty(commentId)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
        commentManager.delete(new ObjectId(messageId), commentId);
        return JSONMessage.success();
    }

    /**
     * @param userId
     * @param status
     * @return
     * @Description:（用户账号锁定解锁）
     **/
    @RequestMapping("/changeStatus")
    public JSONMessage changeStatus(@RequestParam int userId, @RequestParam int status) {
        userManager.changeStatus(ReqUtil.getUserId(), userId, status);
        return JSONMessage.success();
    }

    /**
     * @param userId
     * @param superAdmin
     * @return
     * @Description:（用户账号锁定解锁）
     **/
    @RequestMapping("/changeSuperAdmin")
    public JSONMessage changeSuperAdmin(@RequestParam int userId, @RequestParam int superAdmin) {
        userManager.changeSuperAdmin(ReqUtil.getUserId(), userId, superAdmin);
        return JSONMessage.success();
    }

    /**
     * @param userId
     * @param type
     * @return
     * @Description:（系统充值记录）
     **/
    @RequestMapping("/systemRecharge")
    public JSONMessage systemRecharge(@RequestParam(defaultValue = "0") int userId,
                                      @RequestParam(defaultValue = "0") int type, @RequestParam(defaultValue = "0") int page,
                                      @RequestParam(defaultValue = "10") int limit, @RequestParam(defaultValue = "") String startDate,
                                      @RequestParam(defaultValue = "") String endDate, @RequestParam(defaultValue = "") String tradeNo) {

        if (ObjectUtil.isEmpty(paymentManager)) {
            return JSONMessage.success(KConstants.ResultCode.CLOSEPAY, null);
        }
        try {
            PageResult<BaseConsumeRecord> result = (PageResult<BaseConsumeRecord>) paymentManager.recharge(userId, type, page, limit, startDate, endDate, tradeNo);
            return JSONMessage.success(result);
        } catch (ServiceException e) {
            return JSONMessage.failure(e.getErrMessage());
        }
    }


    /**
     * 用户账单
     *
     * @param type
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/userBill")
    public JSONMessage userBill(@RequestParam int userId, int page, int limit, @RequestParam(defaultValue = "") String startDate,
                                @RequestParam(defaultValue = "") String endDate, @RequestParam(defaultValue = "0") int type) throws Exception {
        if (paymentManager == null) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.CLOSEPAY);
        }
        try {
            // 核验用户是否存在
            if (null == userManager.getUser(userId)) {
                return JSONMessage.failure("用户不存在!");
            }
            //PageResult<BaseConsumeRecord> result = consumeRecordManager.consumeRecordList(userId, page, limit, (byte) 1,startDate,endDate,type);
            PageResult<BaseConsumeRecord> result = (PageResult<BaseConsumeRecord>) paymentManager.consumeRecordList(userId, page, limit, (byte) 1, startDate, endDate, type);
            return JSONMessage.success(result);

        } catch (MongoCommandException e) {
            return JSONMessage.success(0);
        } catch (MongoQueryException e) {
            return JSONMessage.success(0);
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }

    }

    @RequestMapping(value = "/consumeRecordInfo")
    public JSONMessage consumeRecordInfo(String tradeNo) {
        if (paymentManager == null) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.CLOSEPAY);
        }
        try {
            //PageResult<BaseConsumeRecord> recordInfo = consumeRecordManager.getConsumeRecordByTradeNo(tradeNo);
            PageResult<BaseConsumeRecord> recordInfo = (PageResult<BaseConsumeRecord>) paymentManager.getConsumeRecordByTradeNo(tradeNo);
            return JSONMessage.success(recordInfo);
        } catch (MongoCommandException e) {
            return JSONMessage.success(0);
        } catch (MongoQueryException e) {
            return JSONMessage.success(0);
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 添加服务器
     *
     * @param server
     * @return
     */
    @RequestMapping(value = "/addServerList")
    public JSONMessage addServerList(@ModelAttribute ServerListConfig server) {
        try {
            adminManager.addServerList(server);
            return JSONMessage.success();
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 获取服务器列表
     *
     * @param id
     * @param pageIndex
     * @param limit
     * @return
     */
    @RequestMapping(value = "/serverList")
    public JSONMessage serverList(@RequestParam(defaultValue = "") String id,
                                  @RequestParam(defaultValue = "0") int pageIndex, @RequestParam(defaultValue = "10") int limit) {
        PageResult<ServerListConfig> result = adminManager
                .getServerList((!StringUtil.isEmpty(id) ? new ObjectId(id) : null), pageIndex, limit);
        return JSONMessage.success(null, result);
    }

    @RequestMapping(value = "/findServerByArea")
    public JSONMessage findServerByArea(@RequestParam(defaultValue = "") String area) {
        PageResult<ServerListConfig> result = adminManager.findServerByArea(area);
        return JSONMessage.success(null, result);
    }

    /**
     * 修改服务器
     *
     * @param server
     * @return
     */
    @RequestMapping(value = "/updateServer")
    public JSONMessage updateServer(@ModelAttribute ServerListConfig server) {
        try {
            adminManager.updateServer(server);
            return JSONMessage.success();
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 删除服务器
     *
     * @param id
     * @return
     */
    @RequestMapping(value = "/deleteServer")
    public JSONMessage deleteServer(@RequestParam String id) {
        try {
            adminManager.deleteServer(new ObjectId(id));
            return JSONMessage.success();
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 地区配置列表
     *
     * @param area
     * @param page
     * @param limit
     * @return
     */
    @RequestMapping(value = "/areaConfigList")
    public JSONMessage areaConfigList(@RequestParam(defaultValue = "") String area,
                                      @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "10") int limit) {
        PageResult<AreaConfig> result = adminManager.areaConfigList(area, page, limit);
        return JSONMessage.success(result);
    }

    /**
     * 添加地区配置
     *
     * @param area
     * @return
     */
    @RequestMapping(value = "/addAreaConfig")
    public JSONMessage addAreaConfig(@ModelAttribute AreaConfig area) {
        try {
            adminManager.addAreaConfig(area);
            return JSONMessage.success();
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }

    }

    /**
     * 修改地区配置
     *
     * @param area
     * @return
     */
    @RequestMapping(value = "/updateAreaConfig")
    public JSONMessage updateAreaConfig(@ModelAttribute AreaConfig area) {
        try {
            adminManager.updateAreaConfig(area);
            return JSONMessage.success();
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 删除地区配置
     *
     * @param id
     * @return
     */
    @RequestMapping(value = "/deleteAreaConfig")
    public JSONMessage deleteAreaConfig(@RequestParam String id) {
        try {
            adminManager.deleteAreaConfig(new ObjectId(id));
            return JSONMessage.success();
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 添加入口配置
     *
     * @param urlConfig
     * @return
     */
    @RequestMapping(value = "/addUrlConfig")
    public JSONMessage addUrlConfig(@ModelAttribute UrlConfig urlConfig) {
        try {
            UrlConfig data = adminManager.addUrlConfig(urlConfig);
            return JSONMessage.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 查询入口配置
     *
     * @param id
     * @param type
     * @return
     */
    @RequestMapping(value = "/findUrlConfig")
    public JSONMessage findUrlConfig(@RequestParam(defaultValue = "") String id,
                                     @RequestParam(defaultValue = "") String type) {
        PageResult<UrlConfig> result = adminManager
                .findUrlConfig((!StringUtil.isEmpty(id) ? new ObjectId(id) : null), type);
        return JSONMessage.success(null, result);

    }

    /**
     * 删除入口
     *
     * @param id
     * @return
     */
    @RequestMapping(value = "/deleteUrlConfig")
    public JSONMessage deleteUrlConfig(@RequestParam(defaultValue = "") String id) {
        try {
            adminManager.deleteUrlConfig(!StringUtil.isEmpty(id) ? new ObjectId(id) : null);
            return JSONMessage.success();
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }
    }


    /**
     * 中心服务器
     *
     * @param centerConfig
     * @return
     */
    @RequestMapping(value = "/addcenterConfig")
    public JSONMessage addCenterConfig(@ModelAttribute CenterConfig centerConfig) {
        try {
            CenterConfig data = adminManager.addCenterConfig(centerConfig);
            return JSONMessage.success(data);
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 查询中心服务器
     *
     * @param type
     * @param id
     * @return
     */
    @RequestMapping(value = "/findCenterConfig")
    public JSONMessage findCentConfig(@RequestParam(defaultValue = "") String type, @RequestParam(defaultValue = "") String id) {
        PageResult<CenterConfig> result = adminManager.findCenterConfig(type,
                (!StringUtil.isEmpty(id) ? new ObjectId(id) : null));
        return JSONMessage.success(null, result);
    }

    /**
     * 删除中心服务器
     *
     * @param id
     * @return
     */
    @RequestMapping(value = "/deleteCenter")
    public JSONMessage deleteCenter(@RequestParam String id) {
        try {
            adminManager.deleteCenter(new ObjectId(id));
            return JSONMessage.success();
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.success();
        }
    }


    @RequestMapping(value = "/addAdmin")
    public JSONMessage addAdmin(@RequestParam(defaultValue = "86") Integer areaCode, @RequestParam String telePhone,
                                @RequestParam byte role, @RequestParam(defaultValue = "0") Integer type) {

        try {
            // 核验账号是否重复
            // User user = userManager.getUser(areaCode+account);
            /*
             * Admin admin =
             * adminManager.findAdminByAccount(account);
             * if(admin!=null) { return JSONMessage.failure("该账号已存在"); }
             * adminManager.addAdmin(account, password, role);
             */
            //获取角色编号
            areaCode = 86;
            SecurityRole securityRole = null;
            if (role == 0) {
                securityRole = securityRoleManager.querySecurityRoleByRoleName("普通用户");
            } else if (role == 1) {
                securityRole = securityRoleManager.querySecurityRoleByRoleName("游客");
            } else if (role == 4) {
                securityRole = securityRoleManager.querySecurityRoleByRoleName("客服");
            } else if (role == 5) {
                securityRole = securityRoleManager.querySecurityRoleByRoleName("普通管理员");
            } else if (role == 6) {
                securityRole = securityRoleManager.querySecurityRoleByRoleName("超级管理员");
                //超级管理员有全部权限
                List<String> resourceIdList = new ArrayList<>();
                List<ResourceInfo> resourceInfos = resourceInfoManager.queryResourceInfo();
                for (ResourceInfo resourceInfo : resourceInfos) {
                    resourceIdList.add(String.valueOf(resourceInfo.getId()));
                }
                securityRole.setRoleResourceList(resourceIdList);
            } else if (role == 7) {
                securityRole = securityRoleManager.querySecurityRoleByRoleName("财务");
            }

            if (securityRole != null) {
                roleManager.addAdmin(telePhone, role, type, String.valueOf(securityRole.getRoleId()), securityRole.getRoleResourceList());
            } else {
                roleManager.addAdmin(telePhone, role, type, "", new ArrayList<>());
            }

            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }

    }

    @RequestMapping(value = "/adminList")
    public JSONMessage adminList(@RequestParam String adminId, @RequestParam(defaultValue = "") String keyWorld,
                                 @RequestParam(defaultValue = "1") int page, @RequestParam(defaultValue = "10") int limit,
                                 @RequestParam(defaultValue = "0") int type, @RequestParam(defaultValue = "0") Integer userId) {
        Role userRole = roleManager.getUserRole(userId, null, 5);

        if (ObjectUtil.isEmpty(userRole)) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION, ReqUtil.getRequestLanguage());
        }

        if (userRole.getRole() == KConstants.Admin_Role.ADMIN || userRole.getRole() == KConstants.Admin_Role.SUPER_ADMIN ||
                userRole.getRole() == KConstants.Admin_Role.TOURIST || userRole.getRole() == KConstants.Admin_Role.CUSTOMER ||
                userRole.getRole() == KConstants.Admin_Role.FINANCE) {
            PageResult<Role> result = roleManager.adminList(keyWorld, page, limit, type, userId, "");
            return JSONMessage.success(result);
        }

        return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION, ReqUtil.getRequestLanguage());
    }

    /**
     * 后台角色禁用解禁
     *
     * @param adminId
     * @param password
     * @param role
     * @return
     **/
    @RequestMapping(value = "/modifyAdmin")
    public JSONMessage modifyAdmin(@RequestParam Integer adminId, @RequestParam(defaultValue = "") String password,
                                   @ModelAttribute Role role) {
        if (!StringUtil.isEmpty(password)) {
            User user = userManager.getUser(adminId);
            if (!password.equals(user.getPassword())) {
                return JSONMessage.failure("密码有误");
            }
        }

        Role userRole = roleManager.getUserRole(adminId, null, 5);
        if (!ObjectUtil.isEmpty(userRole)) {
            if (userRole.getRole() == KConstants.SIX || userRole.getRole() == KConstants.FIVE) {
                // 1 是 超级管理员
                Object result = roleManager.modifyRole(role);
                return JSONMessage.success(result);
            }
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION, ReqUtil.getRequestLanguage());
        }
        return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION, ReqUtil.getRequestLanguage());
    }

    /**
     * 删除管理角色
     *
     * @param adminId
     * @return
     **/
    @RequestMapping(value = "/delAdmin")
    public JSONMessage deleteAdmin(@RequestParam String adminId, @RequestParam Integer type) {
        try {
            roleManager.delAdminById(adminId, type, ReqUtil.getUserId());
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }

    }

    /**
     * 好友的聊天记录
     *
     * @param userId
     * @param toUserId
     * @return
     **/
    @RequestMapping("/friendsChatRecord")
    public JSONMessage friendsChatRecord(@RequestParam(defaultValue = "0") Integer userId,
                                         @RequestParam(defaultValue = "0") Integer toUserId, @RequestParam(defaultValue = "0") int page,
                                         @RequestParam(defaultValue = "10") int limit) {
        try {
            PageResult<Document> result = friendsManager.chardRecord(userId, toUserId, page, limit);
            return JSONMessage.success(result);
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 删除好友间的聊天记录
     *
     * @param messageId
     * @return
     **/
    @RequestMapping("/delFriendsChatRecord")
    public JSONMessage delFriendsChatRecord(@RequestParam(defaultValue = "") String messageId) {
        try {
            if (StringUtil.isEmpty(messageId)) {
                return JSONMessage.failure("参数有误");
            }
            String[] strMessageIds = StringUtil.getStringList(messageId);
            friendsManager.delFriendsChatRecord(strMessageIds);
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 黑名单操作
     *
     * @param toUserId
     * @param type     0 ： 加入黑名单， 1：移除黑名单
     * @return
     **/
    @SuppressWarnings("static-access")
    @RequestMapping("/blacklist/operation")
    public JSONMessage blacklistOperation(@RequestParam Integer userId, @RequestParam Integer toUserId,
                                          @RequestParam(defaultValue = "0") Integer type, @RequestParam(defaultValue = "") Integer adminUserId) {
        JSONMessage jsonMessage = null;
        try {
            if (0 == type) {
                if (friendsManager.isBlack(userId, toUserId)) {
                    return JSONMessage.failure("不能重复拉黑好友");
                }
                Friends data = friendsManager.consoleAddBlacklist(userId, toUserId, adminUserId);
                jsonMessage = JSONMessage.success("加入黑名单成功", data);
            } else if (1 == type) {
                if (!friendsManager.isBlack(userId, toUserId)) {
                    return JSONMessage.failure("好友：" + toUserId + "不在我的黑名单中");
                }
                Friends data = friendsManager.consoleDeleteBlacklist(userId, toUserId, adminUserId);
                jsonMessage = JSONMessage.success("取消拉黑成功", data);
            }
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
        return jsonMessage;

    }

    /**
     * 开放平台app列表
     *
     * @param page
     * @param limit
     * @return
     */
    @RequestMapping(value = "/openAppList")
    public JSONMessage openAppList(@RequestParam(defaultValue = "-2") int status, @RequestParam(defaultValue = "0") int type,
                                   @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "10") int limit,
                                   @RequestParam(defaultValue = "") String keyWorld) {
        try {
            PageResult<SkOpenApp> list = adminManager.openAppList(status, type, page, limit, keyWorld);
            return JSONMessage.success(list);
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }

    }

    /**
     * 开放平台app详情
     *
     * @param id
     * @return
     */
    @RequestMapping(value = "/openAppDetail")
    public JSONMessage openAppDetail(@RequestParam(defaultValue = "") String id) {
        try {
            Object data = openAppManage.appInfo(new ObjectId(id));
            return JSONMessage.success(data);
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 开放平台通过审核、禁用应用
     *
     * @param id
     * @param userId
     * @param status
     * @return
     */
    @RequestMapping(value = "/approvedAPP")
    public JSONMessage approved(@RequestParam(defaultValue = "") String id,
                                @RequestParam(defaultValue = "") String userId, @RequestParam(defaultValue = "0") int status,
                                @RequestParam(defaultValue = "") String reason) {
        try {
            openAppManage.approvedAPP(id, status, userId, reason);
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 开放平台通过审核app权限
     *
     * @param skOpenApp
     * @return
     */
    @RequestMapping(value = "/checkPermission")
    public JSONMessage checkPermission(@ModelAttribute SkOpenApp skOpenApp) {
        try {
            openAppManage.openAccess(skOpenApp);
            return JSONMessage.success();
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 开放平台删除应用
     *
     * @param id
     * @param accountId
     * @return
     */
    @RequestMapping(value = "/deleteOpenApp")
    public JSONMessage deleteOpenApp(@RequestParam(defaultValue = "") String id,
                                     @RequestParam(defaultValue = "") String accountId) {
        try {
            openAppManage.deleteAppById(new ObjectId(id), accountId);
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 开放平台审核日志列表
     *
     * @param page
     * @param limit
     * @return
     */
    @RequestMapping(value = "/checkLogList")
    public JSONMessage checkLogList(@RequestParam(defaultValue = "0") int page,
                                    @RequestParam(defaultValue = "10") int limit, @RequestParam(defaultValue = "") String keyWorld) {
        try {
            PageResult<SkOpenCheckLog> data = openCheckLogManage.getOpenCheckLogList(page, limit, keyWorld);
            return JSONMessage.success(data);
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 开放平台删除日志
     *
     * @param id
     * @return
     */
    @RequestMapping(value = "/delOpenCheckLog")
    public JSONMessage delOpenCheckLog(@RequestParam(defaultValue = "") String id) {
        try {
            openCheckLogManage.delOpenCheckLog(new ObjectId(id));
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 开放平台开发者列表
     *
     * @param page
     * @param limit
     * @return
     */
    @RequestMapping(value = "/developerList")
    public JSONMessage developerList(@RequestParam(defaultValue = "0") int page,
                                     @RequestParam(defaultValue = "10") int limit, @RequestParam(defaultValue = "-2") int status,
                                     @RequestParam(defaultValue = "") String keyWorld) {
        try {
            PageResult<SkOpenAccount> data = openAccountManage.developerList(page, limit, status,
                    keyWorld);
            return JSONMessage.success(data);
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 开放平台开发者详情
     *
     * @param userId
     * @return
     */
    @RequestMapping(value = "/developerDetail")
    public JSONMessage developerDetail(@RequestParam(defaultValue = "") Integer userId) {
        try {
            Object data = openAccountManage.getOpenAccount(userId);
            return JSONMessage.success(data);
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 开放平台删除开发者
     *
     * @param id
     * @return
     */
    @RequestMapping(value = "/deleteDeveloper")
    public JSONMessage deleteDeveloper(@RequestParam(defaultValue = "") String id) {
        try {
            openAccountManage.deleteDeveloper(new ObjectId(id));
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 开放平台审核开发者、禁用
     *
     * @param id
     * @param userId
     * @param status
     * @return
     */
    @RequestMapping(value = "/checkDeveloper")
    public JSONMessage checkDeveloper(@RequestParam(defaultValue = "") String id,
                                      @RequestParam(defaultValue = "") String userId, @RequestParam(defaultValue = "0") int status) {
        try {
            openAccountManage.checkDeveloper(new ObjectId(id), status);
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 校验第三方网站是否有对应权限
     *
     * @param appId
     * @param type
     * @return
     */
    @RequestMapping(value = "/authInterface")
    public JSONMessage authInterface(@RequestParam(defaultValue = "") String appId, @RequestParam(defaultValue = "1") int type) {
        openAppManage.authInterfaceWeb(appId, type);
        return JSONMessage.success();
    }

    /**
     * 生成邀请码
     */
    @RequestMapping(value = "/create/inviteCode")
    public JSONMessage createInviteCode(@RequestParam(defaultValue = "20") int nums, @RequestParam int userId, @RequestParam short type) throws IOException {
        adminManager.createInviteCode(nums, userId);
        return JSONMessage.success();
    }

    /**
     * 删除邀请码
     */
    @RequestMapping(value = "/inviteCodeList")
    public JSONMessage inviteCodeList(@RequestParam(defaultValue = "0") int userId, @RequestParam(defaultValue = "") String keyworld,
                                      @RequestParam(defaultValue = "-1") short state, @RequestParam(defaultValue = "0") int page,
                                      @RequestParam(defaultValue = "10") int limit) {
        PageResult<InviteCode> data = adminManager.inviteCodeList(userId, keyworld, state, page,
                limit);
        return JSONMessage.success(data);
    }

    /**
     * 删除邀请码
     *
     * @param userId
     * @param inviteCodeId
     * @return
     */
    @RequestMapping(value = "/delInviteCode")
    public JSONMessage delInviteCode(@RequestParam(defaultValue = "") int userId, @RequestParam(defaultValue = "") String inviteCodeId) {
        boolean data = adminManager.delInviteCode(userId, inviteCodeId);
        return JSONMessage.success(data);
    }

    /**
     * @return
     * @Description:（压力测试）
     **/
    @RequestMapping("/pressureTest")
    public JSONMessage pressureTest(@ModelAttribute PressureParam param, HttpServletRequest request) {
        try {
            if (param.getSendMsgNum() < 100) {
                param.setSendMsgNum(100);
            }
            if (param.getTimeInterval() < 30) {
                param.setTimeInterval(30);
            }
            if (param.getCheckNum() > 100) {
                param.setCheckNum(100);
            }
            System.out.println("pressureTest ====> " + request.getSession().getCreationTime() + " "
                    + request.getSession().getId());
            List<String> jids = StringUtil.getListBySplit(param.getRoomJid(), ",");
            param.setJids(jids);
            param.setSendAllCount(param.getSendMsgNum() * jids.size());
            JSONMessage result = messageService.pressureMucTest(param, ReqUtil.getUserId());
            if (null == result) {
                return JSONMessage.failure("已有压测 任务 运行中  请稍后 请求 。。。。。。");
            }
            return result;
        } catch (ServiceException e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 短视频音乐列表
     *
     * @param page
     * @param limit
     * @param keyword
     * @return
     */
    @RequestMapping(value = "/musicList")
    public JSONMessage queryMusicList(@RequestParam(defaultValue = "0") int page,
                                      @RequestParam(defaultValue = "10") Integer limit, @RequestParam(defaultValue = "") String keyword) {
        PageResult<MusicInfo> result = adminManager.queryMusicInfo(page,
                limit, keyword);
        return JSONMessage.success(result);
    }

    /**
     * 添加短视频音乐
     *
     * @param musicInfo
     * @return
     */
    @RequestMapping(value = "/addMusic")
    public JSONMessage addMusic(@ModelAttribute MusicInfo musicInfo) {
        musicManager.addMusicInfo(musicInfo);
        return JSONMessage.success();
    }

    /**
     * 删除短视频音乐
     *
     * @param musicInfoId
     * @return
     */
    @RequestMapping(value = "/deleteMusic")
    public JSONMessage deleteMusic(@RequestParam(defaultValue = "") String musicInfoId) {
        musicManager.deleteMusicInfo(new ObjectId(musicInfoId));
        return JSONMessage.success();
    }

    /**
     * 修改短视频音乐
     *
     * @param musicInfo
     * @return
     */
    @RequestMapping(value = "/updateMusic")
    public JSONMessage updateMusic(@ModelAttribute MusicInfo musicInfo) {
        musicManager.updateMusicInfo(musicInfo);
        return JSONMessage.success();
    }

    /**
     * 转账记录
     *
     * @param userId
     * @param page
     * @param limit
     * @return
     */
    @RequestMapping(value = "/transferList")
    public JSONMessage transferList(@RequestParam(defaultValue = "") String userId,
                                    @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "15") int limit,
                                    @RequestParam(defaultValue = "") String startDate, @RequestParam(defaultValue = "") String endDate) {
        if (ObjectUtil.isEmpty(paymentManager)) {
            return JSONMessage.success(KConstants.ResultCode.CLOSEPAY, null);
        }
        PageResult<Transfer> result = adminManager.queryTransfer(page, limit, userId, startDate,
                endDate);
        return JSONMessage.success(result);
    }

    /**
     * 付款记录
     *
     * @param userId
     * @param page
     * @param limit
     * @return
     */
    @RequestMapping(value = "/paymentCodeList")
    public JSONMessage paymentCodeList(@RequestParam(defaultValue = "0") int userId,
                                       @RequestParam(defaultValue = "0") int type, @RequestParam(defaultValue = "0") int page,
                                       @RequestParam(defaultValue = "15") int limit, @RequestParam(defaultValue = "") String startDate,
                                       @RequestParam(defaultValue = "") String endDate) {
        if (ObjectUtil.isEmpty(paymentManager)) {
            return JSONMessage.success(KConstants.ResultCode.CLOSEPAY, null);
        }
        PageResult<BaseConsumeRecord> result = (PageResult<BaseConsumeRecord>) paymentManager.payment(userId, type, page, limit, startDate, endDate);
        return JSONMessage.success(result);
    }

    /**
     * 获取第三方绑定列表
     *
     * @param userId
     * @param page
     * @param limit
     * @return
     */
    @RequestMapping(value = "/getSdkLoginInfoList")
    public JSONMessage getSdkLoginInfoList(@RequestParam(defaultValue = "") String userId,
                                           @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "15") int limit) {
        try {
            PageResult<SdkLoginInfo> result = adminManager.getSdkLoginInfoList(page, limit, userId);
            return JSONMessage.success(result);
        } catch (NumberFormatException e) {
            logger.info("error : {}" + e.getMessage());
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 解除第三方绑定
     *
     * @param id
     * @return
     */
    @RequestMapping(value = "/deleteSdkLoginInfo")
    public JSONMessage deleteSdkLoginInfo(@RequestParam(defaultValue = "") String id) {
        adminManager.deleteSdkLoginInfo(new ObjectId(id));
        return JSONMessage.success();
    }

    /**
     * @param appId
     * @param callbackUrl
     * @param response
     * @Description:授权登录
     **/
    @RequestMapping(value = "/oauth/authorize")
    public void authorizeUrl(String appId, String callbackUrl, HttpServletResponse response, HttpServletRequest request) {
        try {
            Map<String, String> webInfo = openAppManage.authorizeUrl(appId, callbackUrl);
            String webAppName = webInfo.get("webAppName");
            webAppName = URLEncoder.encode(webAppName, "UTF-8");
            UserAgent ua = UserAgent.parseUserAgentString(request.getHeader("User-Agent"));
            OperatingSystem os = ua.getOperatingSystem();
            if (DeviceType.MOBILE.equals(os.getDeviceType())) {//移动端
                // 校验app内操作
                if (request.getHeader("User-Agent").toLowerCase().contains("app-chatimapp")) {
                    logger.info("移动端,应用内操作");
                    callbackUrl = URLEncoder.encode(callbackUrl);
                } else {
                    logger.info("移动端,应用外操作");
                    callbackUrl = URLEncoder.encode(URLEncoder.encode(callbackUrl));
                }
            } else {
                callbackUrl = URLEncoder.encode(URLEncoder.encode(callbackUrl));
            }
            logger.info("authorizeUrl callbackUrl : {} , webAppName : {}", callbackUrl, webAppName);
            response.sendRedirect("/pages/websiteAuthorh/index.html" + "?" + "appId=" + appId + "&" + "callbackUrl=" + callbackUrl + "&webAppName=" + webAppName + "&webAppsmallImg=" + webInfo.get("webAppsmallImg"));
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

    }


    /**
     * @param page
     * @param limit
     * @return 公众号审核列表
     */
    @RequestMapping(value = "/checkOfficialInfo")
    public JSONMessage getOfficialInfo(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "10") int limit, String status, String keyword) {
        PageResult<OfficialInfo> data = officialInfoCheck.getOfficialInfoList(page, limit, status, keyword);
        return JSONMessage.success(data);
    }

    /**
     * 删除公众号日志
     */
    @RequestMapping(value = "/delOfficialInfo")
    public JSONMessage delOfficialInfo(String id) {
        officialInfoCheck.delOfficialInfoLog(new ObjectId(id));
        return JSONMessage.success();
    }

    /**
     * 获取审核详情
     */
    @RequestMapping(value = "/getOfficialInfo")
    public JSONMessage getOfficialInfo(String id) {
        OfficialInfo officialInfo = officialInfoCheck.getOfficialInfo(new ObjectId(id));
        return JSONMessage.success(officialInfo);
    }

    /**
     * 修改审核内容
     */
    @RequestMapping(value = "/updateOfficialInfo")
    public JSONMessage updateOfficialInfo(String id, int verify, String feedback) {
        //审核通过
        //设置审核通过
        OfficialInfo officialInfo = officialInfoCheck.updateOfficialInfo(new ObjectId(id), verify, feedback);
        //verify = 1 权限
        if (verify == KConstants.ONE) {
            OfficialInfo officialInfo1 = officialInfoCheck.getOfficialInfo(new ObjectId(id));
            roleManager.addOff(officialInfo1.getUserId(), officialInfo.getTelephone(), officialInfo.getPhone(), (byte) 2, 1);
        }
        return JSONMessage.success(officialInfo);
    }

    /**
     * 禁用邀请码
     *
     * @param userId
     * @param status
     * @return
     */
    @RequestMapping(value = "/prohibitInviteCode")
    public JSONMessage prohibitInviteCode(@RequestParam(defaultValue = "") String userId, @RequestParam(defaultValue = "") String status) {
        try {
            int sta = Integer.parseInt(status);
            InviteCode inviteCode = inviteCodeDao.findUserInviteCode(Integer.parseInt(userId));

            short code = (short) (sta == 1 ? sta : -1);
            inviteCode.setStatus(code);

            inviteCodeDao.addInviteCode(inviteCode);
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }


    /**
     * 实人认证信息
     *
     * @param
     * @return
     */
    @RequestMapping(value = "/queryRealNameCertified")
    public JSONMessage queryRealNameCertified(int type, Integer userId) {
        try {
            Certification certification = adminManager.queryCertification(type, userId);
            return JSONMessage.success(certification);
        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }

    }

    @ApiOperation(value = "查询是否支持消息加密", notes = "只能本人能调用")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "areaCode", value = "区号", dataType = "String", required = true, defaultValue = "86"),
            @ApiImplicitParam(paramType = "query", name = "telephone", value = "手机号", dataType = "String", required = true)
    })
    @RequestMapping(value = "/authkeys/isSupportSecureChat")
    public JSONMessage isSupportSecureChat(@RequestParam(defaultValue = "86") String areaCode, @RequestParam String telephone) {
        Map<String, Object> result = new HashMap<>();
        try {
            areaCode = "86";
            User user = userManager.getUser(areaCode + telephone);
            if (ObjectUtil.isEmpty(user)) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.UserNotExist);
            }
            //根据用户是否存在dh公钥，返回给客户端是否支持消息加密
            AuthKeys authKeys = authKeysService.getAuthKeys(user.getUserId());
            result.put("isSupportSecureChat", (authKeys != null && authKeys.getMsgDHKeyPair() != null) ? 1 : 0);
            return JSONMessage.success(result);
        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }
    }

    @ApiOperation("后台管理中重置密码端对端版本")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "areaCode", value = "区号", dataType = "String", defaultValue = "86"),
            @ApiImplicitParam(paramType = "query", name = "randcode", value = "验证码", dataType = "String", required = true),
            @ApiImplicitParam(paramType = "query", name = "telephone", value = "手机号码", dataType = "String", required = true),
            @ApiImplicitParam(paramType = "query", name = "newPassword", value = "新密码", dataType = "String", required = true)
    })
    @RequestMapping("/password/reset/v1")
    public JSONMessage resetPassword_v1(@RequestParam(defaultValue = "86") String areaCode,
                                        @RequestParam(defaultValue = "") String telephone, @RequestParam(defaultValue = "") String newPassword,
                                        @ModelAttribute KeyPairParam param) {
        try {
            areaCode = "86";
            if (com.basic.utils.StringUtil.isEmpty(telephone) || com.basic.utils.StringUtil.isEmpty(newPassword)) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsLack);
            }
            //验签
            if (!authServiceUtils.checkUserUploadMsgKeySign(param.getMac(), telephone, newPassword)) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.AUTH_FAILED);
            }

            telephone = areaCode + telephone;

            Integer userId = userManager.resetPassword(telephone, newPassword);

            //更新用户公钥私钥
            authKeysService.uploadMsgKey(userId, param);
            authKeysService.updateLoginPassword(userId, newPassword);
            /**
             * 删除属于自己的好友聊天记录
             */
            ThreadUtils.executeInThread(call -> {
                messageRepository.cleanUserFriendHistoryMsg(userId);
            });
            userCoreRedisRepository.deleteUserByUserId(userId);
        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }
        return JSONMessage.success();
    }

    @ApiOperation("销毁单聊双方聊天记录")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "userId", value = "userId", dataType = "int", required = true),
            @ApiImplicitParam(paramType = "query", name = "toUserId", value = "toUserId", dataType = "int", required = true),
    })
    @RequestMapping("/destroyFriendMessage")
    public JSONMessage destroyFriendMessage(int userId, int toUserId) {
        ThreadUtils.executeInThread(obj -> {
            tigaseMsgDao.destroyFriendMessage(userId, toUserId);
        });

        /**
         * 发消息给好友双方
         */
        MessageBean messageBean = new MessageBean();
        messageBean.setType(MessageType.TYPE_DESTROY_MESSAGE);
        messageBean.setFromUserId(toUserId + "");
        messageBean.setFromUserName(KConstants.SystemNo.CONSOLE_SYSTEM_NAME);
        messageBean.setToUserId(userId + "");
        messageBean.setMsgType(0);
        messageBean.setMessageId(StringUtil.randomUUID());
        messageService.send(messageBean);

        messageBean = new MessageBean();
        messageBean.setType(MessageType.TYPE_DESTROY_MESSAGE);
        messageBean.setFromUserId(userId + "");
        messageBean.setFromUserName(KConstants.SystemNo.CONSOLE_SYSTEM_NAME);
        messageBean.setToUserId(toUserId + "");
        messageBean.setMsgType(0);
        messageBean.setMessageId(StringUtil.randomUUID());

        messageService.send(messageBean);
        return JSONMessage.success();
    }

    @ApiOperation("销毁群组聊天记录")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "jids", value = "jids", dataType = "int", required = true),
    })
    @RequestMapping("/destroyRoomMessage")
    public JSONMessage destroyRoomMessage(String jids) {
        if (StringUtil.isEmpty(jids)) {
            return JSONMessage.success();
        }
        String[] listBySplit = StringUtil.getStringList(jids, ",");
        tigaseMsgDao.destroyRoomMessage(listBySplit);

        Integer userId = ReqUtil.getUserId();


        ThreadUtils.executeInThread(call -> {
            List<Integer> memberIdList = null;
            MessageBean messageBean;
            for (String jid : listBySplit) {
                memberIdList = roomManager.getMemberIdList(roomManager.getRoomId(jid));
                for (Integer integer : memberIdList) {
                    messageBean = new MessageBean();
                    messageBean.setType(MessageType.TYPE_DESTROY_MESSAGE);
                    messageBean.setFromUserId(userId.toString());
                    messageBean.setFromUserName(KConstants.SystemNo.CONSOLE_SYSTEM_NAME);
                    messageBean.setToUserId(integer.toString());
                    messageBean.setObjectId(jid);
                    messageBean.setTo(integer.toString());
                    messageBean.setMessageId(StringUtil.randomUUID());
                    messageService.send(messageBean);
                }

            }
        });

        return JSONMessage.success();
    }

    @ApiOperation("销毁系统所有聊天记录")
    @RequestMapping("/destroyAllSystemMessage")
    public JSONMessage destroyAllSystemMessage() {
        tigaseMsgDao.destroyAllSystemMessage();
        Integer userId = ReqUtil.getUserId();

        /**
         * 发送广播消息
         */
        MessageBean messageBean = new MessageBean();
        messageBean.setType(MessageType.TYPE_DESTROY_MESSAGE);
        messageBean.setFromUserId(userId.toString());
        messageBean.setFromUserName(KConstants.SystemNo.CONSOLE_SYSTEM_NAME);
        messageBean.setContent("claer_all");
        messageBean.setMsgType(2);
        messageBean.setMessageId(StringUtil.randomUUID());
        messageService.send(messageBean);
        return JSONMessage.success();
    }


    @ApiOperation("销毁指定时间之前的群组")
    @RequestMapping("/destroyRoomByTime")
    public JSONMessage destroyAllSystemMessage(@RequestParam(defaultValue = "") String endTimeStr) {

        long endTime = DateUtil.toDate(endTimeStr).getTime() / 1000; //秒

        roomManager.destroyRoomByCreateTime(endTime, ReqUtil.getUserId());
        return JSONMessage.success();
    }

    @ApiOperation("清除用户和全部好友的双向聊天记录")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "query", name = "userId", value = "userId", dataType = "int", required = true)
    })
    @RequestMapping("/delete/userAllFriends/msgRecord")
    public JSONMessage deleteUserAllFriendsMsgRecord(int userId) {
        if (0 == userId) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
        friendsManager.deleteUserAllFriendsMsgRecord(userId);
        return JSONMessage.success(KConstants.ResultCode.DeleteTaskSubmitSuccess);
    }


    /**
     * 获取tigase库下的所有文档
     **/
    @RequestMapping("/find/tigase/document")
    public JSONMessage findTigaseDocuments() {
        Set<String> tigaseDocuments = tigaseMsgDao.findTigaseDocuments();
        return JSONMessage.success(tigaseDocuments);
    }

    /**
     * 获取chatMsgs库下的所有文档
     **/
    @RequestMapping("/find/chat/msg/document")
    public JSONMessage findChatMsgDocuments() {
        Set<String> tigaseDocuments = tigaseMsgDao.findChatMsgsDocuments();
        tigaseDocuments.remove("chat_msgs");
        return JSONMessage.success(tigaseDocuments);
    }

    /**
     * 计算红包金额
     **/
    @RequestMapping("/get/redpacket/money")
    public JSONMessage calculateRedPacketMoney() {
        Map<String, Object> data = redPacketsManager.calculateRedPacketMoney();
        return JSONMessage.success(data);
    }


    /**
     * 清除创建者不存在的群组
     */
    @RequestMapping("/clear/rooms")
    public JSONMessage clearRooms() {
        //清除创建者不存在的群组

        //遍历群组
        Query query = roomManager.getRoomDao().createQuery();

        query.addCriteria(Criteria.where("createTime").gt(0));

        System.out.println("=====匹配的群组总数==> " + roomManager.getRoomDao().count(query));

        List<Room> rooms = roomManager.getRoomDao().queryListsByQuery(query);

        if (null != rooms) {
            for (Room room : rooms) {
                try {
                    int createUserId = room.getUserId();
                    if (null == userDao.getUser(createUserId)) {
                        System.out.println("====Room Create not Data start destory Room ===> " + room.getName());
                        roomManager.destroyRoom(room, 1000);
                    }
                } catch (Exception e) {
                    continue;
                }
            }
        }

        return JSONMessage.success();
    }

    /**
     * 后台充值
     *
     * @param money
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/Recharge")
    public JSONMessage Recharge(Double money, int userId) throws Exception {
        if (paymentManager == null) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.CLOSEPAY);
        }
        // 权限校验
        byte role = (byte) roleManager.getUserRoleByUserId(ReqUtil.getUserId());
        if (role != KConstants.Admin_Role.SUPER_ADMIN) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }
        double consoleMaxRechargeAmount = SKBeanUtils.getImCoreService().getPayConfig().getConsoleMaxRechargeAmount();
        if (money > consoleMaxRechargeAmount) {
            return JSONMessage.failure("后台管理单笔最大充值金额为：" + consoleMaxRechargeAmount);
        }
        // 核验用户是否存在
        if (null == userManager.getUser(userId)) {
            return JSONMessage.failure("充值失败, 用户不存在!");
        }

        String tradeNo = StringUtil.getOutTradeNo();


        try {

            UserMoneyLog userMoneyLog = new UserMoneyLog(userId, userId, tradeNo, money,
                    MoenyAddEnum.MOENY_ADD, MoneyLogEnum.ADMIN_RECHARGE, MoneyLogTypeEnum.RECEIVE);

            Double balance = userManager.rechargeUserMoeny(userMoneyLog);
            // 创建充值记录

            BaseConsumeRecord record = new BaseConsumeRecord();
            record.setUserId(userId);
            record.setTradeNo(tradeNo);
            record.setMoney(money);
            record.setStatus(KConstants.OrderStatus.END);
            record.setType(KConstants.ConsumeType.SYSTEM_RECHARGE);
            record.setChangeType(KConstants.MOENY_ADD);
            record.setPayType(KConstants.PayType.SYSTEMPAY); // type = 3 ：管理后台充值
            record.setDesc("Backstage balance recharge");
            record.setTime(DateUtil.currentTimeSeconds());
            record.setOperationAmount(money);
            record.setCurrentBalance(balance);
            record.setBusinessId(userMoneyLog.getBusinessId());
            paymentManager.savaConsumeRecor(record);
//			data.put("balance", balance);
            return JSONMessage.success(balance);
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }

    }

    /**
     * @param money
     * @param userId
     * @return
     * @Description:手工提现
     **/
    @RequestMapping("/handCash")
    public JSONMessage handCash(Double money, int userId) {
        if (paymentManager == null) {
            return JSONMessage.success(KConstants.ResultCode.CLOSEPAY, null);
            //return JSONMessage.failureByErrCodeAndData(KConstants.ResultCode.CLOSEPAY,null);
        }
        // 权限校验
        byte role = (byte) roleManager.getUserRoleByUserId(ReqUtil.getUserId());
        if (role != KConstants.Admin_Role.SUPER_ADMIN) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
        }
        double consoleMaxCodePaymentAmount = SKBeanUtils.getImCoreService().getPayConfig().getConsoleMaxCodePaymentAmount();
        if (money > consoleMaxCodePaymentAmount) {
            return JSONMessage.failure("后台管理单笔最大手工提现金额为：" + consoleMaxCodePaymentAmount);
        }
        // 核验用户是否存在
        if (null == userManager.getUser(userId)) {
            return JSONMessage.failure("提现失败, 用户不存在!");
        } else {
            Double balance = userManager.getUserMoeny(userId);
            if (balance < money) {
                return JSONMessage.failure("余额不足");
            }
        }
        String tradeNo = StringUtil.getOutTradeNo();

        try {

            UserMoneyLog userMoneyLog = new UserMoneyLog(userId, userId, tradeNo, money,
                    MoenyAddEnum.MOENY_REDUCE, MoneyLogEnum.ADMIN_RECHARGE, MoneyLogTypeEnum.NORMAL_PAY);
            userMoneyLog.setExtra("后台手工提现");

            Double balance = userManager.rechargeUserMoeny(userMoneyLog);
            // 创建充值记录
            BaseConsumeRecord record = new BaseConsumeRecord();
            record.setUserId(userId);
            record.setTradeNo(tradeNo);
            record.setMoney(money);
            record.setStatus(KConstants.OrderStatus.END);
            record.setType(KConstants.ConsumeType.SYSTEM_HANDCASH);
            record.setChangeType(KConstants.MOENY_REDUCE);
            record.setPayType(KConstants.PayType.SYSTEMPAY); // type = 3 ：管理后台充值
            record.setDesc("Backstage manual withdrawal");
            record.setTime(DateUtil.currentTimeSeconds());
            record.setOperationAmount(money);
            record.setCurrentBalance(balance);
            record.setBusinessId(userMoneyLog.getBusinessId());
            paymentManager.savaConsumeRecor(record);
            return JSONMessage.success(balance);
        } catch (MongoCommandException e) {
            return JSONMessage.success(0);
        } catch (MongoQueryException e) {
            return JSONMessage.success(0);
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    @Autowired
    private CollectionAccountManager collectionAccountManager;

    @RequestMapping(value = "/listCollectionAccount")
    public JSONMessage listCollectionAccount(String page, String limit) {
        PageResult<CollectionAccount> data = collectionAccountManager.queryCollectionAccountList(Integer.parseInt(page), Integer.valueOf(limit));
        return JSONMessage.success(data);
    }

    @RequestMapping(value = "/deleteCollectionAccount")
    public JSONMessage deleteCollectionAccount(String id) {
        collectionAccountManager.deleteCollectionAccount(new ObjectId(id));
        return JSONMessage.success();
    }

    @RequestMapping(value = "/saveCollectionAccount")
    public JSONMessage saveCollectionAccount(CollectionAccount lineConfig) {
        return JSONMessage.success(collectionAccountManager.sava(lineConfig));
    }

}