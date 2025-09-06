package com.basic.im.common;

import cn.hutool.core.util.StrUtil;
import com.basic.commons.thread.ThreadUtils;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.message.MessageService;
import com.basic.im.message.MessageType;
import com.basic.im.support.Callback;
import com.basic.im.user.dao.OfflineOperationDao;
import com.basic.im.user.entity.OfflineOperation;
import com.basic.im.user.service.UserCoreService;
import com.basic.im.utils.SpringBeansUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Function;

@Slf4j
public class MultipointSyncUtil {

    private static UserCoreService userCoreService;
    private static MessageService messageService;
    private static OfflineOperationDao offlineOperationDao;

    static {
        userCoreService          = SpringBeansUtils.getBean(UserCoreService.class);
        messageService           = SpringBeansUtils.getBean(MessageService.class);
        offlineOperationDao      = SpringBeansUtils.getBean(OfflineOperationDao.class);
    }

    // 多点登录下操作类型
    public interface MultipointLogin {
        /**
         * 修改密码
         */
        String SYNC_LOGIN_PASSWORD = "sync_login_password";

        /**
         * 支付密码
         */
        String SYNC_PAY_PASSWORD = "sync_pay_password";

        /**
         * 隐私设置
         */
        String SYNC_PRIVATE_SETTINGS = "sync_private_settings";

        /**
         * 好友标签
         */
        String SYNC_LABEL = "sync_label";

        /**
         * 易宝支付账号设置
         */
        String SYNC_YOP_OPEN_ACCOUNT = "sync_yop_open_account";

        /**
         * 实人认证
         */
        String SYNC_REALNAME_CERTIFICATION = "sync_realname_certification";

        /**
         * VIP 充值
         */
        String SYNC_VIP_RECHARGE = "sync_vip_recharge";

        /**
         * 单群聊的隐藏会话
         */
        String HIDE_CHAT_SWITCH = "hideChatSwitch";

        /**
         * 设置隐藏会话密码
         */
        String SYNC_PRIVATE_CHAT_PASSWORD = "sync_private_chat_password";


        /**
         * 设置发送消息已读状态
         */
        String SEND_MSG_STATE = "send_msg_state";


        /**
         * 好友相关
         */
        String TAG_FRIEND = "friend";

        /**
         * 群组标签相关
         */
        String TAG_ROOM = "room";

        /**
         * 好友分组操作相关
         */
        String TAG_LABLE = "label";
    }

    /**
     * 多点登录同步
     */
    public static void multipointLoginDataSync(Integer userId, String nickName, String operationType){
        if (StrUtil.isBlank(nickName)){
            nickName = userCoreService.getNickName(userId);
        }
        multipointLoginDataSync(userId, nickName, operationType,null);
    }

    public static void multipointLoginDataSync(Integer userId, String operationType){
        multipointLoginDataSync(userId, userCoreService.getNickName(userId), operationType,null);
    }

    public static void multipointLoginDataSync(Integer userId, String operationType,Function<MessageBean,MessageBean> function){
        multipointLoginDataSync(userId, userCoreService.getNickName(userId), operationType,function);
    }

    public static void multipointLoginDataSync(Integer userId, String nickName, String operationType, Function<MessageBean,MessageBean> function){
        ThreadUtils.executeInThread(obj -> {
            MessageBean messageBean=new MessageBean();
            messageBean.setType(MessageType.multipointLoginDataSync);
            messageBean.setFromUserId(String.valueOf(userId));
            messageBean.setFromUserName(nickName);
            messageBean.setToUserId(String.valueOf(userId));
            messageBean.setToUserName(nickName);
            messageBean.setObjectId(operationType);
            messageBean.setMessageId(StringUtil.randomUUID());
            if (null!=function){
                messageBean = function.apply(messageBean);
            }
            try {
                if (null != messageService){
                    messageService.send(messageBean);
                }else{
                    log.info("messageService is null,check your autowired");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * 多点登录下个人信息修改通知
     * @param type  type=0:修改用户信息，type=1:修改好友备注
     */
    public static void multipointLoginUpdateUserInfo(Integer userId, String nickName, Integer toUserId, String toNickName, int type){
        OfflineOperation offlineOperation = null;
        if(0 == type){
            offlineOperation = offlineOperationDao.queryOfflineOperation(userId,null,String.valueOf(userId));
        }else if (1 == type){
            offlineOperation = offlineOperationDao.queryOfflineOperation(userId,null,String.valueOf(toUserId));
        }
        if(null  == offlineOperation) {
            offlineOperationDao.addOfflineOperation(userId, MultipointLogin.TAG_FRIEND,null==toUserId?String.valueOf(userId):String.valueOf(toUserId), DateUtil.currentTimeSeconds());
        } else{
            OfflineOperation offlineOperation1Entity=new OfflineOperation();
            offlineOperation1Entity.setOperationTime(DateUtil.currentTimeSeconds());
            offlineOperationDao.updateOfflineOperation(offlineOperation.getId(),offlineOperation1Entity);
        }
        updatePersonalInfo(userId, nickName,toUserId,toNickName,type);
    }

    /**
     * 发送多点登录同步消息
     */
    public static void updatePersonalInfo(Integer userId, String nickName, Integer toUserId, String toNickName, int type){
        ThreadUtils.executeInThread((Callback) obj -> {
            MessageBean messageBean=new MessageBean();
            messageBean.setType(MessageType.updatePersonalInfo);
            messageBean.setFromUserId(String.valueOf(userId));
            messageBean.setFromUserName(nickName);
            if(1 == type){
                messageBean.setTo(String.valueOf(userId));
            }
            messageBean.setToUserId(null == toUserId ? String.valueOf(userId) : String.valueOf(toUserId));
            messageBean.setToUserName(StringUtil.isEmpty(toNickName) ? nickName : toNickName);
            messageBean.setMessageId(StringUtil.randomUUID());
            try {
                if (null != messageService){
                    messageService.send(messageBean);
                }else{
                    log.info("messageService is null,check your autowired");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
