package com.basic.im.push.service;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.basic.im.entity.PushConfig;
import com.basic.im.push.vo.MsgNotice;
import com.basic.im.user.entity.User;
import com.basic.utils.StringUtil;
import com.xiaomi.xmpush.server.*;
import com.xiaomi.xmpush.server.Message.Builder;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.parser.ParseException;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

//小米通知栏推送集成
@Slf4j
public class XMPushService extends PushServiceUtils {

    private static Map<String, Sender> expandSender;// app推送sender

    private static Sender initSender(String xmAppSecret) {
        return new Sender(xmAppSecret);// 申请到的AppSecret
    }

    private static Sender reSetSender(String packName) {
        Sender sender = initSender(getPushConfig(packName).getXmAppSecret());
        expandSender.put(packName, sender);
        return sender;
    }

    /**
     * 根据包名获取推送Sender
     * @param packName 包名
     * @return Sender
     */
    private static Sender getSender(String packName) {
        Sender sender;
        if (null != expandSender) {
            sender = expandSender.get(packName);
            if (null != sender) {
                return sender;
            } else {
                return reSetSender(packName);
            }
        }else{
            expandSender = Maps.newConcurrentMap();
            return reSetSender(packName);
        }
    }

    /**
     * 获取当前有效渠道
     *
     * @param
     * @return
     */
    private static Result getChannleList(String packName) {
        try {
            PushConfig.AndroidPush pushConfig = getPushConfig(packName);
            Result result=null;
            if(null!=pushConfig) {
                ChannelHelper channelHelper = new ChannelHelper(pushConfig.getXmAppSecret());

                result = channelHelper.getChannelList(1);
            }
            return result;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * 通知栏推送
     * pushToRegId 多包名下token更改为deviceInfo
     */
    public static void pushToRegId(MsgNotice notice, String callNum, User.DeviceInfo deviceInfo, boolean flag, boolean initFlag) {
        if (null == deviceInfo || StringUtil.isEmpty(deviceInfo.getPushToken())) {
            return;
        }
        if (initFlag) {
            expandSender = Maps.newConcurrentMap();
        }
        if (StringUtils.isEmpty(notice.getText())) {
            notice.setText("收到一条消息...");
        }
        String messagePayload = notice.getText();
        String title = notice.getTitle();
        String description = notice.getText();
        if (notice.getIsGroup()) {
            description = notice.getText().replace(notice.getGroupName(), "");
            title = notice.getTitle();
        }
        String packName = deviceInfo.getPackName();
        Message message = null;
        Builder builder = new Message.Builder()
                .title(title)
                .description(description).payload(messagePayload)
                .restrictedPackageName(packName)
                .passThrough(0)//消息使用通知栏方式
                .extra(Constants.EXTRA_PARAM_NOTIFY_EFFECT, Constants.NOTIFY_LAUNCHER_ACTIVITY);
//        .extra(Constants.EXTRA_PARAM_INTENT_URI, "intent:#Intent;component=com.xiaomi.mipushdemo/.NewsActivity;end")

        //自定义参数
        builder.extra("from", notice.getFrom() + "");
        builder.extra("fromUserName", notice.getName() + "");
        builder.extra("messageType", notice.getType() + "");
        builder.extra("to", notice.getTo() + "");
        if (100 == notice.getType() || 110 == notice.getType() ||
                115 == notice.getType() || 120 == notice.getType()) {
            builder.extra("callNum", callNum + "");
        }

        String url = null;
        if (notice.getIsGroup()) {
            url = "intent://"+packName+"/notification#Intent;scheme=chat;launchFlags=0x10000000;S.userId="+notice.getRoomJid()+";end";
        } else {
            url = "intent://"+packName+"/notification#Intent;scheme=chat;launchFlags=0x10000000;S.userId="+notice.getFrom()+";end";
        }
        String channleId = flag ? "superVideoMessage" : "普通消息通知";
        // 自定义渠道（兼容旧版没有自定义渠道推送）
        Result channleResult = getChannleList(packName);
        if(null!=channleResult) {
            org.json.simple.JSONObject channleJSON = channleResult.getData();
            List<Object> channleList = (List<Object>) channleJSON.get("list");
            if (!channleList.isEmpty()) {
                message = builder.notifyType(1).extra(Constants.EXTRA_PARAM_SOUND_URI, "android.resource://" + packName + "/raw/dial").extra(Constants.EXTRA_PARAM_INTENT_URI, url)
                        .extra("channel_id", channleId).extra("channel_id", "high_system").build();
            }else{
                message = builder.notifyType(1).extra(Constants.EXTRA_PARAM_SOUND_URI, "android.resource://" + packName + "/raw/dial").extra(Constants.EXTRA_PARAM_INTENT_URI, url).build();
            }
        }else {
            message = builder.notifyType(1).extra(Constants.EXTRA_PARAM_SOUND_URI, "android.resource://" + packName + "/raw/dial").extra(Constants.EXTRA_PARAM_INTENT_URI, url).build();
        }
        /*
            message = builder.notifyType(1).extra(Constants.EXTRA_PARAM_NOTIFY_EFFECT, Constants.NOTIFY_ACTIVITY).extra(Constants.EXTRA_PARAM_INTENT_URI, url)     // 使用默认提示音提示
            .extra("channel_id",channleId).build();
        */
        try {
            Result result = getSender(packName).send(message, deviceInfo.getPushToken(), 3);
            log.info(result.toString());
        } catch (Exception e) {
           log.error(e.getMessage(),e);
        }
    }

    /**
     * @throws Exception
     * @Description:小米的全量推送
     **/
    protected static void sendBroadcast(MsgNotice notice) {
        AtomicReference<String> url = new AtomicReference<String>();
        List<PushConfig> parentPushConfig = getParentPushConfig();
        if(null == parentPushConfig || parentPushConfig.isEmpty()){
            return;
        }else{
            parentPushConfig.stream().forEach(pushConfig -> {
                if (null == notice.getObjectId()) {
                    url.set("intent://" + pushConfig.getPackageName() + "/notification#Intent;scheme=chat;launchFlags=0x10000000;end");
                } else {
                    try {
                        url.set("intent://" + pushConfig.getPackageName() + "/notification#Intent;scheme=chat;launchFlags=0x10000000;S.url=" + URLEncoder.encode(notice.getObjectId(), "UTF-8") + ";end");
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                }
                Constants.useOfficial();
                String messagePayload = "This is a message";
                String title = notice.getTitle();
                String description = notice.getText();
                Message message = new Message.Builder()
                        .title(title)
                        .description(description).payload(messagePayload)
                        .restrictedPackageName(pushConfig.getPackageName())
                        .notifyType(1)     // 使用默认提示音提示
                        .extra(Constants.EXTRA_PARAM_NOTIFY_EFFECT, Constants.NOTIFY_ACTIVITY).extra(Constants.EXTRA_PARAM_INTENT_URI, url.get())
                        .build();
                Result broadcastAll = null;
                try {
                    broadcastAll = getSender(pushConfig.getPackageName()).broadcastAll(message, 3);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                log.info("XIAO MI PUSH RESULT : {}", JSONObject.toJSONString(broadcastAll));
            });
        }

    }
}

