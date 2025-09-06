package com.basic.im.push.service;

import com.google.common.collect.Maps;
import com.notnoop.apns.APNS;
import com.notnoop.apns.PayloadBuilder;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.entity.PushConfig;
import com.basic.im.push.vo.MsgNotice;
import com.basic.im.user.entity.User;
import com.basic.im.utils.SKBeanUtils;
import com.basic.utils.StringUtil;
import com.turo.pushy.apns.ApnsClient;
import com.turo.pushy.apns.ApnsClientBuilder;
import com.turo.pushy.apns.DeliveryPriority;
import com.turo.pushy.apns.PushNotificationResponse;
import com.turo.pushy.apns.util.ApnsPayloadBuilder;
import com.turo.pushy.apns.util.SimpleApnsPushNotification;
import com.turo.pushy.apns.util.concurrent.PushNotificationFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;

import javax.net.ssl.SSLException;
import java.io.File;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Semaphore;

@Slf4j
public class ApnsHttp2PushService extends com.basic.im.push.service.PushServiceUtils {

    private static final String sandboxServer = "gateway.sandbox.push.apple.com";
    private static ApnsClient apnsService;

    private static ApnsClient betaApnsService;

    private static ApnsClient viopService;

    private static Map<String,ApnsClient> iosPushClient;// 根据包名来获取对应配置

    private static EventLoopGroup eventLoopGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2);
    private static final Semaphore semaphore = new Semaphore(10000);

    public static enum PushEnvironment {
        BETA,//企业版 环境
        Pro,//发布环境  Apple Store 版
        VOIP//Viop  Apple Store 版
    }


    private static final String CERTIFICATE_EXPIRED = "Received fatal alert: certificate_expired";

    private static ApnsClientBuilder getApnsClientBuilder(PushConfig.IOSPush pushConfig, PushEnvironment environment) {
        try {
            String pkpath = null;
            switch (environment) {
                case BETA:
                    pkpath = pushConfig.getBetaApnsPk();
                    break;
                case Pro:
                    pkpath = pushConfig.getAppStoreApnsPk();
                    break;
                case VOIP:
//                    pkpath = pushConfig.getVoipPk();
                    break;
                default:
                    break;
            }
            if (pkpath.startsWith("classpath:")) {
                ClassPathResource resource = new ClassPathResource(pkpath);
                String path = resource.getClassLoader().getResource(pkpath.replace("classpath:", "")).getPath();
                pkpath = path;
            }
            ApnsClientBuilder builder = new ApnsClientBuilder();

            if (1 == pushConfig.getIsApnsSandbox()) {
                builder.setApnsServer(ApnsClientBuilder.DEVELOPMENT_APNS_HOST);
            } else {
                builder.setApnsServer(ApnsClientBuilder.PRODUCTION_APNS_HOST);
            }
            builder.setClientCredentials(new File(pkpath), pushConfig.getPkPassword());
            builder.setEventLoopGroup(eventLoopGroup).setConcurrentConnections(Runtime.getRuntime().availableProcessors());
            return builder;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }

    }

    /**
     * 初始化ANPS推送
     * @param
     * @return
     */
    private static ApnsClient initApnsClient(String packageName, boolean isBeta) throws SSLException {
        PushConfig.IOSPush iosPushConfig = getIOSPushConfig(packageName);
        ApnsClientBuilder builder = getApnsClientBuilder(iosPushConfig, isBeta ? PushEnvironment.BETA : PushEnvironment.Pro);
        ApnsClient apnsClient = builder.build();
        iosPushClient.put(packageName, apnsClient);
        return apnsClient;
    }

    /**
     * 根据具体证书类型获取APNS推送Client
     * @param
     * @return
     */
    private static ApnsClient getApnsService(String packageName,boolean isBeta) throws SSLException {
        log.info(isBeta ? "====getBetaService======》" : "====getApnsService======》");
        ApnsClient apnsClient;
        if(null != iosPushClient){
            apnsClient = iosPushClient.get(packageName);
            if(null != apnsClient){
                return apnsClient;
            }else{
                return initApnsClient(packageName,isBeta);
            }
        }else{
            iosPushClient = Maps.newConcurrentMap();
            return initApnsClient(packageName,isBeta);
        }
    }

    private static ApnsClient getVoipService(String packName) {
        try {
            if (null == viopService) {
//                PushConfig pushConfig = PushServiceUtils.getPushConfig();
                PushConfig.IOSPush pushConfig = getIOSPushConfig(packName);
                synchronized (pushConfig) {
                    if (null != viopService) {
                        return viopService;
                    }
                    ApnsClientBuilder builder = getApnsClientBuilder(pushConfig, PushEnvironment.VOIP);

                    viopService = builder.build();
                    log.info("====getVoipService======》");
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return viopService;
    }

    public static String builderPayloadOld(MsgNotice notice, boolean flag) {
        PayloadBuilder builder = APNS.newPayload();
        builder.customField("content-available", 1);
        builder.alertBody(notice.getText() + "");
        builder.badge(notice.getMsgNum());
        if (flag) {
            builder.sound("whynotyou.caf");
        } else {
            builder.sound("default");
        }
        builder.customField("from", notice.getFrom() + "");
        builder.customField("fromUserName", notice.getName());
        builder.customField("messageType", notice.getType() + "");
        builder.customField("to", notice.getTo() + "");
        if (!StringUtil.isEmpty(notice.getRoomJid())) {
            builder.customField("roomJid", notice.getRoomJid());
        }
        if (!StringUtil.isEmpty(notice.getRoomId())) {
            builder.customField("roomId", notice.getRoomId());
        }
        if (!StringUtil.isEmpty(notice.getObjectId())) {
            builder.customField("url", notice.getObjectId());
            builder.customField("fromUserName", "");
        }
        log.info("builder {}", "builder");
        return builder.build();
    }

    public static String builderPayload(MsgNotice notice) {
        ApnsPayloadBuilder builder = new ApnsPayloadBuilder();

        builder.setContentAvailable(true);
        //builder.addCustomProperty("content-available", 1);

        builder.setAlertTitle(notice.getText() + "");
        //builder.setAlertBody();
        builder.setBadgeNumber(notice.getMsgNum());

        builder.setSound("default");
        builder.addCustomProperty("from", notice.getFrom() + "")
                .addCustomProperty("fromUserName", notice.getName())
                .addCustomProperty("messageType", notice.getType() + "")
                .addCustomProperty("to", notice.getTo() + "");
        return builder.buildWithDefaultMaximumLength();
    }

    public static void pushMsgToUser(User.DeviceInfo deviceInfo, MsgNotice notice, PushEnvironment env, boolean flag, boolean initFlag) {
        if(initFlag){
            iosPushClient = Maps.newConcurrentMap();
        }
        if(!notice.isNumFlag()){
            /**
             * 没有设置推送数量 数量+1
             */
            notice.setMsgNum(userCoreService.incrementAddMsgNum(notice.getTo(),1));
        }else {
            /**
             * 设置了推送数量 直接群缓存的
             */
            notice.setMsgNum(userCoreService.getMsgNum(notice.getTo()));
        }
        if(KConstants.isDebug) {
            log.info("ios push {}  num {} ",notice.getTo(),notice.getMsgNum());
        }
        String packetName = deviceInfo.getPackName();
        final ChatApnsPushNotification notification;

        final String payload = builderPayloadOld(notice, flag);
        if (PushEnvironment.VOIP != env) {
            if (PushEnvironment.Pro == env) {
//			   String appStoreAppId = getPushConfig().getAppStoreAppId();
                // 个人版
                notification = new ChatApnsPushNotification(deviceInfo.getPushToken(), packetName, payload, env, notice.getMessageId());
            } else {
                // 企业版
                notification = new ChatApnsPushNotification(deviceInfo.getPushToken(), packetName, payload, env, notice.getMessageId());
            }

        } else {
            notification = new ChatApnsPushNotification(deviceInfo.getPushToken(), packetName + ".voip", payload, env, notice.getMessageId());
        }

        notification.setTo(notice.getTo());

        sendPushToApns(notification, packetName);

    }

    private static void listenerNotificationResponse(final PushNotificationFuture<ChatApnsPushNotification, PushNotificationResponse<ChatApnsPushNotification>> notificationFuture, String packageName) {
        try {
            notificationFuture.addListener(new GenericFutureListener<Future<PushNotificationResponse>>() {
                @Override
                public void operationComplete(Future<PushNotificationResponse> pushNotificationResponseFuture) throws Exception {

                    if (notificationFuture.isSuccess()) {
                        PushNotificationResponse<ChatApnsPushNotification> response = notificationFuture.getNow();
                        if (response.isAccepted()) {
                            log.info("send apns Success collapseId:{}  to:{} ", notificationFuture.getPushNotification().getCollapseId(), notificationFuture.getPushNotification().getTo());
                        } else {
                            Date invalidTime = response.getTokenInvalidationTimestamp();
                            log.error("Notification rejected by the APNs gateway: " + response.getRejectionReason());
                            if (invalidTime != null) {
                                log.error("\t…and the token is invalid as of " + response.getTokenInvalidationTimestamp());
                            }
                        }
                    } else {
                        ChatApnsPushNotification notification = notificationFuture.getPushNotification();
                        log.error("send apns failed notification collapseId= {} to ={} device token={}   {} ", notification.getCollapseId(), notification.getTo(), notification.getToken(),notificationFuture.cause().getMessage());
                        if (null != notificationFuture.cause() &&
                                CERTIFICATE_EXPIRED.equals(notificationFuture.cause().getLocalizedMessage())) {
                            return;
                        }
                        if(3>notification.getReSend()) {
                            notification.addReSend();
                            sendPushToApns(notification, packageName);
                        }
                    }
                    semaphore.release();
                }
            });
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private static void sendPushToApns(ChatApnsPushNotification notification, String packageName) {
        final PushNotificationFuture<ChatApnsPushNotification, PushNotificationResponse<ChatApnsPushNotification>> notificationFuture;
        if (KConstants.isDebug) {
            log.info("{} apns push to > {}  {} ", notification.getCollapseId(), notification.getTo(), notification.getToken());
        }
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            log.error("ios push InterruptedException get semaphore failed, collapseId: {} deviceToken:{} ", notification.getCollapseId(), notification.getToken());
        }
        try {
            if (PushEnvironment.VOIP != notification.getEnvironment()) {
                notificationFuture = getApnsService(packageName, (PushEnvironment.BETA == notification.getEnvironment())).sendNotification(notification);
            } else {
                notificationFuture = getVoipService(packageName).sendNotification(notification);
            }
            listenerNotificationResponse(notificationFuture, packageName);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }


    }

    public static class ChatApnsPushNotification extends SimpleApnsPushNotification {

        private PushEnvironment environment;
        private int to;



        private int reSend=0;

        public int getReSend() {
            return reSend;
        }

        public void setReSend(int reSend) {
            this.reSend = reSend;
        }
        public void addReSend(){
            this.reSend++;
        }

        public ChatApnsPushNotification(String token, String topic, String payload, PushEnvironment environment, String collapseId) {
            this(token, topic, payload, collapseId);
            this.setEnvironment(environment);
        }

        public ChatApnsPushNotification(String token, String topic, String payload, String collapseId) {
            super(token, topic, payload, new Date(System.currentTimeMillis() + DEFAULT_EXPIRATION_PERIOD_MILLIS), DeliveryPriority.IMMEDIATE, collapseId, null);
        }

        public PushEnvironment getEnvironment() {
            return environment;
        }

        public void setEnvironment(PushEnvironment environment) {
            this.environment = environment;
        }

        public int getTo() {
            return to;
        }

        public void setTo(int to) {
            this.to = to;
        }

    }

}
