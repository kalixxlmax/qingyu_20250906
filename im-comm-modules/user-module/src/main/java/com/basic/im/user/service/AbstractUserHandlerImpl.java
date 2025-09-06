package com.basic.im.user.service;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.basic.commons.thread.Callback;
import com.basic.commons.thread.ThreadUtils;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.comm.utils.RandomUtil;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.company.service.CompanyManager;
import com.basic.im.config.AppConfig;
import com.basic.im.entity.Config;
import com.basic.im.friends.service.impl.AddressBookManagerImpl;
import com.basic.im.friends.service.impl.FriendGroupManagerImpl;
import com.basic.im.friends.service.impl.FriendsManagerImpl;
import com.basic.im.message.IMessageRepository;
import com.basic.im.message.MessageService;
import com.basic.im.room.dao.RoomDao;
import com.basic.im.room.dao.RoomNoticeDao;
import com.basic.im.room.entity.Room;
import com.basic.im.room.service.RoomManager;
import com.basic.im.room.service.impl.RoomManagerImplForIM;
import com.basic.im.user.dao.InviteCodeDao;
import com.basic.im.user.entity.InviteCode;
import com.basic.im.user.entity.Role;
import com.basic.im.user.entity.User;
import com.basic.im.user.event.DeleteUserEvent;
import com.basic.im.user.event.KeyPairChageEvent;
import com.basic.im.user.event.UserChageNameEvent;
import com.basic.im.user.model.KSession;
import com.basic.im.user.model.UserExample;
import com.basic.im.user.service.impl.RoleManagerImpl;
import com.basic.im.utils.SKBeanUtils;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public abstract class AbstractUserHandlerImpl implements UserHandler {

    private final Logger logger = LoggerFactory.getLogger("userHandler");

    @Autowired
    @Lazy
    protected IMessageRepository messageRepository;

    @Autowired
    protected InviteCodeDao inviteCodeDao;

    @Autowired
    protected RoleManagerImpl roleManager;

    @Autowired
    protected FriendsManagerImpl friendsManager;

    @Autowired
    protected RoomManager roomManager;

    @Autowired
    protected RoomDao roomDao;


    @Autowired
    protected CompanyManager companyManager;


    @Autowired
    protected AddressBookManagerImpl addressBookManager;


    @Autowired
    protected FriendGroupManagerImpl friendGroupManager;

    @Autowired
    protected UserCoreService userCoreService;

    @Autowired
    @Lazy
    protected MessageService messageService;

    @Autowired
    protected AppConfig appConfig;



    @Override
    public void registerHandler(String userId, String pwd, String nickname) {

    }

    @Override
    public int registerBeforeHandler(int userId, UserExample example) {
        // 核验邀请码,及相关操作
        InviteCode inviteCode = checkInviteCode(example,userId);
        if(inviteCode != null){
            return inviteCode.getUserId();
        }
        return -1;
    }

    private void sendMsg(int userId,int codeUserId){
        Config config = SKBeanUtils.getSystemConfig();
        if (!StringUtil.isEmpty(config.getRegisterInviteCodeSetting())) {
                JSONObject setting = JSONObject.parseObject(config.getRegisterInviteCodeSetting());
                String open = setting.get("open").toString().trim();
                if (!StringUtil.isEmpty(open) && new Boolean(open)) {
                    //发送消息
                    String upperToLowerMsg = setting.get("upperToLowerMsg").toString().trim();
                    if (!StringUtil.isEmpty(upperToLowerMsg)) {
                        User upperUser = userCoreService.getUser(codeUserId);
                        User user = userCoreService.getUser(userId);
                        MessageBean messageBean = new MessageBean();
                        messageBean.setFromUserId(String.valueOf(codeUserId));
                        messageBean.setFromUserName(upperUser.getNickname());
                        messageBean.setToUserId(String.valueOf(user.getUserId()));
                        messageBean.setToUserName(user.getNickname());
                        messageBean.setType(1);
                        messageBean.setImSys((short) 0);
                        messageBean.setContent(upperToLowerMsg);
                        messageBean.setMessageId(StringUtil.randomUUID());
                        messageService.send(messageBean);
                    }
                    String lowerToUpperMsg = setting.get("lowerToUpperMsg").toString().trim();
                    if (!StringUtil.isEmpty(lowerToUpperMsg)) {
                        User upperUser = userCoreService.getUser(codeUserId);
                        User user = userCoreService.getUser(userId);
                        MessageBean messageBean = new MessageBean();
                        messageBean.setFromUserId(String.valueOf(user.getUserId()));
                        messageBean.setFromUserName(user.getNickname());
                        messageBean.setToUserId(String.valueOf(codeUserId));
                        messageBean.setToUserName(upperUser.getNickname());
                        messageBean.setType(1);
                        messageBean.setImSys((short) 0);
                        messageBean.setContent(lowerToUpperMsg);
                        messageBean.setMessageId(StringUtil.randomUUID());
                        messageService.send(messageBean);
                    }
                }
        }
    }

    @Override
    public void registerAfterHandler(int userId, UserExample example,int codeUserId){

        if(codeUserId > -1){
            friendsManager.addFriends(userId, codeUserId);
            sendMsg(userId,codeUserId);

        }

        friendsManager.addFriends(userId, 10000);
        int inviteCodeMode = SKBeanUtils.getImCoreService().getConfig().getRegisterInviteCode();
        if(inviteCodeMode == 0){
            // 默认成为好友
            defaultTelephones(example, userId);
        }
        // 调用组织架构功能示例方法
        companyManager.autoJoinCompany(userId);
        // 自动创建 好友标签
        friendGroupManager.autoCreateGroup(userId);

        joinDefaultGroup(userId);

     //   roomManager.join(userId, new ObjectId("5a2606854adfdc0cd071485e"),3,Room.Member.OperationType.SYSTEM_INVITE,0);


        /*if(example.getUserType()!=null){
            if(example.getUserType()==3){
                roomManager.join(userId, new ObjectId("5a2606854adfdc0cd071485e"),3,Room.Member.OperationType.SYSTEM_INVITE);
            }
        }*/
        //更新通讯录好友
        long time = DateUtil.currentTimeSeconds();
        addressBookManager.notifyBook(example.getTelephone(), userId, example.getNickname(),time);
        // 清除redis中没有系统号的表

        // 维护公众号角色
        if (example.getUserType()!=null && example.getUserType() == 2) {
            Role role = new Role(userId, example.getTelephone(), (byte)2, (byte)1, 0);
            roleManager.getRoleDao().addRole(role);
            roleManager.updateFriend(userId, 2);
        }


        /**
         * 用于新用户注册后发送演示菜单功能(演示demo 客服模块)
         */
        //关注用于客服演示的企业号
        //friendsManager.addFriends(userId, 10041452);


        /*JSONObject fromData = new JSONObject();
        //fromData.put("companyMpId",10021951);
        fromData.put("visitorId", example.getUserId());
        fromData.put("visitorName",example.getNickname());
        MqMessageSendUtil.sendMessage(TopicConstant.CUSTOMER_MENU_TASK_TOPIC, fromData.toString(), false);*/
        /** 演示demo END */


        /**
         * 统计当天注册人数
         */
        int registerUserNoticeNum = appConfig.getRegisterUserNoticeNum();
        if( 0 == registerUserNoticeNum ){
            return;
        }


        long registerUserCount = userCoreService.queryTodayRegisterUserCount();

        logger.info("todayRegisterUserCount  ====> {}",registerUserCount);

        List<Integer> defFriendUserIdList = userCoreService.queryDefFriendUserIdList();
        if(null==defFriendUserIdList||0==defFriendUserIdList.size()){
            return;
        }
        if(registerUserCount>= registerUserNoticeNum){
            if(registerUserCount==registerUserNoticeNum){
                sendRegisterUserCountNotice(registerUserCount,defFriendUserIdList);
            }else if(0==registerUserCount%10){
                sendRegisterUserCountNotice(registerUserCount,defFriendUserIdList);
            }
        }

    }


    private void sendRegisterUserCountNotice(long registerUserCount,List<Integer> defFriendUserIdList){
        MessageBean messageBean = new MessageBean();
        messageBean.setType(1);
        messageBean.setFromUserId("10000");
        messageBean.setFromUserName(userCoreService.getNickName(10000));
        messageBean.setMessageId(StringUtil.randomUUID());
        messageBean.setContent("温馨提示,今天注册用户统计:"+registerUserCount);
        for (Integer userId : defFriendUserIdList) {
            messageBean.setToUserId(userId.toString());
            messageService.send(messageBean);
        }

    }


    @Override
    public void registerToIM(String userId, String pwd) {
        messageRepository.registerAndXmppVersion(userId,pwd);
    }

    @Override
    public void changePasswordHandler(User user, String oldPwd, String newPwd) {
        messageRepository.changePassword(user.getUserId()+"", user.getPassword(), newPwd);
    }


    private void joinDefaultGroup(int userId) {
        String groupsIds = SKBeanUtils.getSystemConfig().getDefaultGroups();
        if(StrUtil.isBlank(groupsIds)){
            return;
        }
        List<String> groupList = Arrays.asList(StringUtil.getStringList(groupsIds));
        for (String groupId : groupList){
            try {
                roomManager.join(userId, new ObjectId(groupId),3,Room.Member.OperationType.SYSTEM_INVITE,0);
            }catch (Exception e){
                logger.error(e.getMessage(),e);
            }
        }

    }

    /** @Description:（注册默认成为好友）
     * @param example
     * @param userId
     **/
    private void defaultTelephones(UserExample example, Integer userId) {
        List<Integer> idList = null;
        Room createRoom = null;
        JSONObject userKeys = null;

        //获取开关
        byte registerCreateRoom = appConfig.getRegisterCreateRoom();
        if (1==registerCreateRoom){
            //创建群
            createRoom = new Room();
            //设置群昵称
            createRoom.setName(userCoreService.getNickName(userId)+"的群组");
            //设置群成员
            idList = new ArrayList<>();
            //设置userKeys
            userKeys =  JSON.parseObject("");
        }

        // 注册默认成为好友
        //String telephones =SKBeanUtils.getSystemConfig().getDefaultTelephones();
        //log.info(" config defaultTelephones : " + telephones);

        List<Integer> defFriendUserList = userCoreService.queryDefFriendUserIdList();
        if (null!=defFriendUserList&&0<defFriendUserList.size()) {
            for (Integer defUserId : defFriendUserList) {
                friendsManager.addFriends(userId, defUserId);// 过滤好友验证直接成为好友
                sendMsg(userId,defUserId);
                //加入成员
                if (1==registerCreateRoom && null != defUserId){
                    idList.add(defUserId);
                }
            }
            //创建群
            if (1==registerCreateRoom){
                String roomNotice =SKBeanUtils.getImCoreService().getConfig().getRoomNotice();
                /**
                 * 默认建群公告提示
                 * ####
                 */
                if(!StringUtil.isEmpty(roomNotice)){
                    final String[] noticeArr = roomNotice.split("####");

                    createRoom.setId(ObjectId.get());
                    Room.Notice notice=new Room.Notice();
                    notice.setText(noticeArr[0]);
                    notice.setTime(DateUtil.currentTimeSeconds());
                    notice.setUserId(idList.get(0));
                    notice.setNickname(userCoreService.getNickName(idList.get(0)));
                    notice.setId(ObjectId.get());
                    notice.setRoomId(createRoom.getId());
                    createRoom.setJid(StringUtil.randomUUID());
                    createRoom.setNotice(notice);

                    roomManager.add(userCoreService.getUser(userId), createRoom, idList,userKeys);
                    final List<Integer> list = idList;
                    final String roomJid = createRoom.getJid();
                    ThreadUtils.executeInThread(new Callback() {
                        @Override
                        public void execute(Object obj) {
                            sendNoticeMessageToGroup(list,notice,roomJid,noticeArr);
                        }
                    },5);
                }else {
                    createRoom=roomManager.add(userCoreService.getUser(userId), createRoom, idList,userKeys);
                }
                if(null!=createRoom) {
                    roomManager.updateRoomAvatarUserIds(createRoom);
                }

            }
        }
    }

    @Autowired
    private RoomNoticeDao roomNoticeDao;
    private void sendNoticeMessageToGroup(List<Integer> idList,Room.Notice notice,String jid,final String[] noticeArr){
        if(null==idList||idList.size()==0){
           return;
        }
        Integer sendUserId = idList.get(0);

        roomNoticeDao.addNotice(notice);
        try {
            roomManager.updateNotice(notice.getRoomId(),notice.getId(),notice.getText(),sendUserId);
        }catch (Exception e){
            e.printStackTrace();
        }


        ThreadUtils.executeInThread(new Callback() {
            @Override
            public void execute(Object obj) {
                MessageBean messageBean =null;
                int sendId;
                for (int i = 0; i < noticeArr.length; i++) {
                    messageBean = new MessageBean();
                    messageBean.setType(1);

                    sendId=idList.get(i%idList.size());

                    messageBean.setFromUserId(sendId+"");
                    messageBean.setFromUserName(userCoreService.getNickName(sendId));


                    messageBean.setContent(noticeArr[i]);
                    messageBean.setMessageId(StringUtil.randomUUID());
                    messageBean.setTo(jid);
                    messageBean.setToUserId(jid);
                    messageService.syncSendMsgToGroupByJid(jid,messageBean);
                }


            }
        },1);
     }

    @Override
    public void updateNickNameHandler(int userId,String oldNickName, String newNickName) {
        this.publishEvent(new UserChageNameEvent(userId,oldNickName,newNickName));

    }
    @Override
    public void userOnlineHandler(int userId) {

    }

    @Override
    public void refreshUserSessionHandler(int userId, KSession session) {

    }

    @Override
    public void clearUserSessionHandler(String accessToken) {

    }

    @Override
    public void deleteUserHandler(int adminUserId,int userId) {
        this.publishEvent(new DeleteUserEvent(adminUserId,userId));
        // 删除用户关系
        friendsManager.deleteFansAndFriends(userId);
        // 删除通讯录好友
        addressBookManager.delete(null, null, userId);
        // 删除用户的角色信息
        roleManager.deleteAllRoles(userId);
        // 删除用户组织架构相关信息
        companyManager.delCompany(userId);

    }

    @Override
    public void updateKeyPairHandler(KeyPairChageEvent keyPairChageEvent) {
        this.publishEvent(keyPairChageEvent);

    }

    /**
     * 检查注册邀请码的及相关处理
     * @return
     */
    private InviteCode checkInviteCode(UserExample example,int userId){

        //获取系统当前的邀请码模式 0:关闭   1:开启一对一邀请(一码一用)    2:开启一对多邀请(一码多用)
        int inviteCodeMode = SKBeanUtils.getImCoreService().getConfig().getRegisterInviteCode();

        if(inviteCodeMode==0) { //关闭
            return null;
        }
        if(StringUtil.isEmpty(example.getInviteCode())) {
            throw new ServiceException("请填写邀请码");
        }
        InviteCode inviteCode =null;
        boolean isNeedUpdateInvateCode = false; // 是否需要更新邀请码数据

        if(inviteCodeMode==1) { // 开启一对一邀请
            //该模式下邀请码为必填项
            if(StringUtil.isEmpty(example.getInviteCode())) {
                throw new ServiceException("请填写邀请码");
            }
            inviteCode=inviteCodeDao.findInviteCodeByCode(example.getInviteCode());
            //检查用户填写的邀请码的合法性
            if(inviteCode == null || !(inviteCode.getTotalTimes()==1 && inviteCode.getStatus()==0) ){ //status = 0; //状态值 0,为初始状态未使用   1:已使用  -1 禁用
                throw new ServiceException("邀请码无效或已被使用");
            }
            isNeedUpdateInvateCode = true;
            //给注册用户生成一个自己的一对一邀请码 DateUtil.currentTimeSeconds()+ userCoreService.createInviteCodeNo(1)+1
            String inviteCodeStr = "";
            do{
                try {
                    Thread.sleep(300);
                    inviteCodeStr = cn.hutool.core.util.RandomUtil.randomNumbers(6);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }while (inviteCodeDao.findInviteCodeByCode(inviteCodeStr) != null);


//            String inviteCodeStr = RandomUtil.idToSerialCode(userId); //生成邀请码
            inviteCodeDao.addInviteCode(new InviteCode(userId, inviteCodeStr, System.currentTimeMillis(), 1));
            example.setMyInviteCode(inviteCodeStr);


        }else if(inviteCodeMode==2) { //开启一对多邀请
            // 该模式下邀请码为选填项,不强制要求填写
            inviteCode=inviteCodeDao.findInviteCodeByCode(example.getInviteCode());

            if(inviteCode==null || inviteCode.getStatus()==-1 ){ //status = 0; //状态值 0,为初始状态未使用   1:已使用  -1 禁用
                throw new ServiceException("邀请码无效或已被使用");
            }


//            if (example.getInviteCode() != null ){
//                if(example.getInviteCode().equals("")){
//                }else{
//                    //检查用户填写的邀请码的合法性
//                    if(inviteCode==null || inviteCode.getStatus()==-1 ){ //status = 0; //状态值 0,为初始状态未使用   1:已使用  -1 禁用
//                        throw new ServiceException("邀请码无效或已被使用");
//                    }
//                }
//            }
            // 检查用户填写的邀请码的合法性
            if(inviteCode!=null && inviteCode.getTotalTimes()!=1 ){ // 邀请码合法
                isNeedUpdateInvateCode = true;
            }

            // 如果用户没有邀请码，则生成一个不限次数的邀请码
            if (StringUtil.isEmpty(example.getMyInviteCode())){
                // 生成邀请码
               // String inviteCodeStr = RandomUtil.idToSerialCode(userId);
                String inviteCodeStr = "";
                do{
                    try {
                        Thread.sleep(300);
                        inviteCodeStr = cn.hutool.core.util.RandomUtil.randomNumbers(6);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                }while (inviteCodeDao.findInviteCodeByCode(inviteCodeStr) != null);


                InviteCode mutInviteCode = new InviteCode();
                mutInviteCode.setId(ObjectId.get());
                mutInviteCode.setUserId(example.getUserId());
                mutInviteCode.setUsedTimes(0);
                mutInviteCode.setTotalTimes(-1);
                mutInviteCode.setStatus((short) 0);
                mutInviteCode.setInviteCode(inviteCodeStr);
                mutInviteCode.setCreateTime(DateUtil.currentTimeSeconds());
                inviteCodeDao.addInviteCode(mutInviteCode);
                example.setMyInviteCode(inviteCodeStr);
            }

        }

        // 更新邀请码数据
        if(isNeedUpdateInvateCode) {
            // 将邀请码的使用次数加1
            inviteCode.setUsedTimes(inviteCode.getUsedTimes()+1);
            inviteCode.setStatus((short)1);
            inviteCode.setLastuseTime(System.currentTimeMillis());
            inviteCodeDao.addInviteCode(inviteCode);

            /**
             * 自动添加邀请人为好友
             */
            InviteCode finalInviteCode = inviteCode;
//            ThreadUtils.executeInThread(obj -> {
//                friendsManager.addFriends(userId, finalInviteCode.getUserId());
//            },5);
        }

        return inviteCode;
    }

}
