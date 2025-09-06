package com.basic.im.push.service;

import com.google.common.collect.Maps;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.constants.MsgType;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.entity.ClientConfig;
import com.basic.im.entity.PushConfig;
import com.basic.im.message.MessageType;
import com.basic.im.push.service.hwpush.examples.SendDataMessage;
import com.basic.im.push.service.hwpush.examples.SendNotifyMessage;
import com.basic.im.push.vo.MsgNotice;
import com.basic.im.user.entity.User;
import com.basic.im.user.service.UserCoreService;
import com.basic.im.utils.SKBeanUtils;
import com.basic.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author lidaye
 * @Description: 第三方推送工具类
 * @date 2018年8月20日
 */
@Slf4j
public abstract class PushServiceUtils {

    protected static UserCoreService userCoreService;

    public static void setUserCoreService(UserCoreService service) {
        PushServiceUtils.userCoreService = service;
    };

    private static Map<String, PushConfig.AndroidPush> pushConfigMap;// 根据包名获取安卓推送配置

    private static Map<String, PushConfig.IOSPush> pushIOSConfigMap;// 根据包名获取IOS推送配置

    /**
     * 根据包名获取对应安卓推送配置
     *
     * @param
     * @return
     */
    protected static PushConfig.AndroidPush getPushConfig(String packName) {
        PushConfig.AndroidPush pushConfig = null;
        if (null != pushConfigMap) {
            pushConfig = pushConfigMap.get(packName);
            if (null != pushConfig) {
                return pushConfig;
            } else {
                pushConfigMap = Maps.newConcurrentMap();
                pushConfig = getPushAndroidConfig(packName);
                if(null!=pushConfig) {
                    pushConfigMap.put(packName, pushConfig);
                }
                return pushConfig;
            }
        } else {
            pushConfigMap = Maps.newConcurrentMap();
            pushConfig = getPushAndroidConfig(packName);
            if(null!=pushConfig) {
                pushConfigMap.put(packName, pushConfig);
            }
            return pushConfig;
        }
    }

    /**
     * 根据包名获取对应IOS推送配置
     *
     * @param
     * @return
     */
    protected static PushConfig.IOSPush getIOSPushConfig(String packName) {
        PushConfig.IOSPush pushConfig = null;
        if (null != pushIOSConfigMap) {
            pushConfig = pushIOSConfigMap.get(packName);
            if (null != pushConfig) {
                return pushConfig;
            } else {
                pushConfig = getPushIosConfig(packName);
                pushIOSConfigMap = Maps.newConcurrentMap();
                if(null!=pushConfig) {
                    pushIOSConfigMap.put(packName, pushConfig);
                }
                return pushConfig;
            }
        } else {
            pushIOSConfigMap = Maps.newConcurrentMap();
            pushConfig = getPushIosConfig(packName);
            if(null!=pushConfig) {
                pushIOSConfigMap.put(packName, pushConfig);
            }
            return pushConfig;
        }
    }


    /**
     * 校验后台是否添加推送的配置信息
     *
     * @param
     * @return
     */
    public static boolean isInitPushConfig() {
        System.out.println("推送配置信息：" + SKBeanUtils.getImCoreService().isInitPushConfig());
        return SKBeanUtils.getImCoreService().isInitPushConfig();
    };

    /**
     * 初始化安卓，ios配置
     *
     * @param
     * @return
     */
    public static boolean initPushConfig() {
       return SKBeanUtils.getImCoreService().initPushConfig();
    }

    public static List<PushConfig> getParentPushConfig() {
        return SKBeanUtils.getImCoreService().getParentPushConfig();
    }

    /**
     * 根据包名获取对应推送配置
     * @param
     * @return
     */
    private static PushConfig.AndroidPush getPushAndroidConfig(String packName) {
        return SKBeanUtils.getImCoreService().getAndroidPushConfigHandler(packName);
    }

    private static PushConfig.IOSPush getPushIosConfig(String packName) {
        return SKBeanUtils.getImCoreService().getIosPushConfigHandler(packName);
    }

    /**
     * @param @param  toUserId
     * @param @param  notice
     * @param @param  deviceInfo
     * @param @return 参数
     * @Description: 第三方推送 到  android 设备
     */
    public static boolean pushToAndroid(int toUserId, MsgNotice notice, User.DeviceInfo deviceInfo) {
        if (isInitPushConfig()) {
            log.info("请到后台管理系统 添加 推送配置！");
            return false;
        }
        boolean initFlag = false;// 初始化配置信息
        if (1 == SKBeanUtils.getImCoreService().getPushFlag()) {
            initFlag = true;
        }
        boolean flag = false;
        if (100 == notice.getType() || 110 == notice.getType() ||
                115 == notice.getType() || 120 == notice.getType()) {
            flag = true;
        }
        try {
            switch (deviceInfo.getPushServer()) {
                case KConstants.PUSHSERVER.XIAOMI:
                    XMPushService.pushToRegId(notice, notice.getFileName(), deviceInfo, flag,initFlag);
                    break;
                case KConstants.PUSHSERVER.JPUSH:
                    Map<String, String> content = Maps.newConcurrentMap();
                    content.put("msg", notice.getText());
                    content.put("regId", deviceInfo.getPushToken());
                    content.put("title", notice.getTitle());
                    JPushServices.jpushAndroid(content, deviceInfo);
                    break;
                case KConstants.PUSHSERVER.HUAWEI:
                    // 音视频消息  发送透传通知
                    if (100 == notice.getType() || 110 == notice.getType() ||
                            115 == notice.getType() || 120 == notice.getType()) {
                        // 普通通知
                        SendNotifyMessage.sendNotification(notice, deviceInfo, flag, initFlag);
                        // 透传通知
                        SendDataMessage.sendTransparent(deviceInfo, initFlag);
                    } else {
                        SendNotifyMessage.sendNotification(notice, deviceInfo, flag, initFlag);
                    }
                    break;
                case KConstants.PUSHSERVER.FCM:
                    GooglePushService.fcmPush(deviceInfo, notice, flag);
                    break;
                case KConstants.PUSHSERVER.MEIZU:
                    MZPushService.varnishedMessagePush(deviceInfo, notice);
                    break;
                case KConstants.PUSHSERVER.OPPO:
                    OPPOPushService.buildMessage(deviceInfo, notice, flag, initFlag);
                    break;
                case KConstants.PUSHSERVER.VIVO:
                    VIVOPushService.noticeColumnMessagePush(deviceInfo, notice, initFlag);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        if (initFlag) {
            SKBeanUtils.getImCoreService().updatePushFlag(0);
        }
        return true;

    }

    /**
     * @param @param  toUserId
     * @param @param  notice
     * @param @param  deviceInfo
     * @param @return 参数
     * @Description: 第三方推送 到  ios 设备
     */
    public static boolean pushToIos(int toUserId, MsgNotice notice, User.DeviceInfo deviceInfo) {
        if (isInitPushConfig()) {
            log.info("请到后台管理系统 添加 推送配置！");
            return false;
        }
        boolean initFlag = false;
        if (1 == SKBeanUtils.getImCoreService().getPushFlag()) {
            initFlag = true;
        }
        //未读消息数量
        boolean isBeta = false;
        PushConfig.IOSPush iosPushConfig = getIOSPushConfig(deviceInfo.getPackName());
        if(deviceInfo.getPackName().equals(iosPushConfig.getBetaAppId())){
            isBeta = true;
        }

        if (MsgType.TYPE_BACK == notice.getType()) {
            notice.setMsgNum(userCoreService.decrementAndGet(toUserId));
        } else if (99 < notice.getType() && 130 > notice.getType()) {
            if (isBeta) {
                // 企业版  取消音视频通话需要 加数量
                if (MsgType.TYPE_NO_CONNECT_VIDEO != notice.getType() && MsgType.TYPE_NO_CONNECT_VOICE != notice.getType()) {
                    notice.setMsgNum(userCoreService.getMsgNum(toUserId));
                } else {
                    notice.setMsgNum(userCoreService.incrementAddMsgNum(toUserId,1));
                }
            } else {
                // 正式版  取消视频通话需要 加数量
                if (MsgType.TYPE_NO_CONNECT_VIDEO != notice.getType()) {
                    notice.setMsgNum(userCoreService.getMsgNum(toUserId));
                } else {
                    notice.setMsgNum(userCoreService.incrementAddMsgNum(toUserId,1));
                }
            }
        }else if(MessageType.VISITOR_STATUS_NOTICE == notice.getType()){
            // 700 客服消息不维护角标
            notice.setMsgNum(userCoreService.getMsgNum(toUserId));
        } else {
           notice.setNumFlag(false);
        }

        //userCoreService.changeMsgNum(toUserId, notice.getMsgNum());
        try {
            switch (deviceInfo.getPushServer()) {
                case KConstants.PUSHSERVER.APNS:
                    //是否企业测试版
                    if (0 == notice.getFrom() || StringUtil.isEmpty(notice.getName())) {
                        log.info("from or name is null :{}", notice.toString());
                        return false;
                    }
                    if (notice.getType() == 100 || notice.getType() == 110 || notice.getType() == 115 || notice.getType() == 120) {
                        //apns 推送
                        ClientConfig clientConfig = SKBeanUtils.getImCoreService().getClientConfig();
                        if (1 == clientConfig.getDisplayRedPacket()) {
                            log.info("apns voip push VoipToken is null userId => {}", notice.getTo());
                            ApnsHttp2PushService.pushMsgToUser(deviceInfo, notice, isBeta ? ApnsHttp2PushService.PushEnvironment.BETA : ApnsHttp2PushService.PushEnvironment.Pro, true,initFlag);
                        }
                    } else {
                        ApnsHttp2PushService.pushMsgToUser(deviceInfo, notice, isBeta ? ApnsHttp2PushService.PushEnvironment.BETA : ApnsHttp2PushService.PushEnvironment.Pro, false,initFlag);
                    }
                    break;
                //百度推送
                case KConstants.PUSHSERVER.BAIDU:
                    BaiduPushService.PushMessage msg = new BaiduPushService.PushMessage(notice);
//					String appId=pushConfig.getIosPush().getBdAppStoreAppId();
//					String result = BaiduPushService.pushSingle(2, deviceInfo, msg);
//					System.out.println(notice.getTo()+"=====>"+result);
                    break;

                default:
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        if (initFlag) {
            SKBeanUtils.getImCoreService().updatePushFlag(0);
        }
        return true;
    }

}

