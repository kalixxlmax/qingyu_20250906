package com.basic.im.push.plugin;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.constants.MsgType;
import com.basic.im.constant.RedisKeyConstant;
import com.basic.im.constant.TopicConstant;
import com.basic.im.friends.entity.Friends;
import com.basic.im.live.entity.LiveRoom;
import com.basic.im.message.MessageType;
import com.basic.im.push.constants.PushLocaleConstants;
import com.basic.im.push.i18n.LocaleMessageUtils;
import com.basic.im.push.rocketmq.PushMessageInternationListenerConcurrently;
import com.basic.im.push.service.JPushServices;
import com.basic.im.push.service.PushServiceUtils;
import com.basic.im.push.vo.HwMsgNotice;
import com.basic.im.push.vo.MsgNotice;
import com.basic.im.push.vo.PushMessageDTO;
import com.basic.im.user.entity.User;
import com.basic.im.user.model.KSession;
import com.basic.im.user.service.UserCoreRedisRepository;
import com.basic.im.user.service.UserCoreService;
import com.basic.im.utils.MqMessageSendUtil;
import com.basic.im.utils.SKBeanUtils;
import com.basic.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.redisson.api.RMap;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.*;

/**
 * @description: 客服消息第三方推送处理 <br>
 * @date: 2020/7/27 0027  <br>
 * @author: lidaye <br>
 * @version: 1.0 <br>
 */
@Component
public class CustomerMessagePushPlugin extends AbstractMessagePushPlugin {


    private static final Logger log = LoggerFactory.getLogger(CustomerMessagePushPlugin.class);


    @Autowired
    private DefaultMessagePushPlugin defaultMessagePushPlugin;


    @Autowired
    private RedissonClient redissonClient;

    @Override
    public boolean hanlderPushMessage(JSONObject messageBean) {
        try {
            PushMessageDTO pushMessageDTO = JSON.parseObject(messageBean.toJSONString(), PushMessageDTO.class);
            log.info("customerMessage  pushMessage  :{}", messageBean.toJSONString());
            MsgNotice msgNotice = parseMsgNotice(pushMessageDTO);

            // 访客给所有离线客服发送消息
            if (MessageType.VISITOR_STATUS_NOTICE == msgNotice.getType()) {
                // 推送指定客服  toUserId != getSrvId  getSrvId
                if(msgNotice.getTo() != pushMessageDTO.getSrvId()){
                    log.info("推送给指定客服 ：toUserId ：{}，srvId :{}",msgNotice.getTo(),pushMessageDTO.getSrvId());
                    msgNotice.setTo(Math.toIntExact(pushMessageDTO.getSrvId()));
                    pushOne(Math.toIntExact(pushMessageDTO.getSrvId()), msgNotice, pushMessageDTO);
                }else{
                    // 推送离线客服
                    List<Long> customers = queryOffLineServiceList(msgNotice.getTo());
                    if (null == customers || customers.isEmpty()) {
                        log.info("offLine service is null");
                        return false;
                    } else {
                        for (Long serviceId : customers) {
                            int toUserId = Math.toIntExact(serviceId);
                            msgNotice.setTo(toUserId);
                            pushMessageDTO.setSrvId(serviceId);
                            pushOne(toUserId, msgNotice, pushMessageDTO);
                        };
                    }
                }
            } else {
                pushOne(msgNotice.getTo(), msgNotice, pushMessageDTO);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }

        return true;

    }


    @Override
    protected MsgNotice parseMsgNotice(PushMessageDTO message) throws Exception {
        MsgNotice notice = new MsgNotice();
        int messageType;
        String text;

        try {
            String msgId = message.getMessageId();
            if (!StringUtil.isEmpty(msgId)) {
                notice.setMessageId(msgId);
            }

            messageType = message.getType();
            notice.setFrom((int) message.getFromUserId());

            if (StringUtil.isEmpty(notice.getName())) {
                notice.setName(message.getFromUserName());
            }
            notice.setToName(message.getToUserName());
            notice.setFileName(message.getFileName());

            if (!notice.getIsGroup()) {
                notice.setTo(Integer.parseInt(message.getToUserId()));
            }
            notice.setTitle(notice.getName());

            if (!StringUtil.isEmpty(message.getObjectId())) {
                notice.setObjectId(message.getObjectId());
            }

            if (0 < message.getIsEncrypt() || 0 < message.getEncryptType()) {
                text = PushLocaleConstants.MESSAGE;
            } else if (1 == message.getIsReadDel()) {
                // 阅后即焚消息
                text = PushLocaleConstants.READ_DEL_MESSAGE;
            } else {
                text = message.getContent();
            }
            notice.setType(messageType);
        } catch (Exception e) {
            throw e;
        }

        notice.setText(text);
        if (!notice.getIsGroup()) {

            if (StringUtil.isEmpty(notice.getToName())) {
                notice.setToName(userCoreService.getNickName(notice.getTo()));
            }
        } else {
            if (StringUtil.isEmpty(notice.getName())) {
                notice.setName(userCoreService.getNickName(notice.getFrom()));
            }
        }
        return notice;
    }


    //推送给一个用户
    protected void pushOne(final int to, MsgNotice notice, PushMessageDTO messageDTO) {
        try {
            if (to == notice.getFrom()) {
                return;
            }

            /**
             * 推送到 接受者 的ios 设备和 android 设备上
             */
            final User.DeviceInfo androidDevice = userCoreRedisRepository.getAndroidPushToken(to);
            final User.DeviceInfo iosDevice = userCoreRedisRepository.getIosPushToken(to);
            if (null == androidDevice && null == iosDevice) {
                System.out.print("deviceMap is Null, to : " + to);
                return;
            } else if (null != androidDevice && null != iosDevice) {
                if (StringUtil.isEmpty(androidDevice.getPushToken()) &&
                        StringUtil.isEmpty(iosDevice.getPushToken())) {
                    log.error("PushToken is Null > {}", to);
                    return;
                }
            }
            try {
                if (null != androidDevice) {
                    // 推送内容国际化处理
                    String text = parseText(notice, messageDTO, "android");
                    if (null == text) {
                        log.error("text 为null 不需要推送 type :{}", notice.getType());
                        return;
                    } else {
                        notice.setText(text);
                    }
                    log.info("userId {} deviceId {} content {} ", to, "android", text);
                    /*String adress = androidDevice.getAdress();
                    if (!StringUtil.isEmpty(adress)) {
//                        if (androidDevice.getPushServer().equals(KConstants.PUSHSERVER.HUAWEI) && !adress.equals(SKBeanUtils.getImCoreService().getPushConfig().getServerAdress())) {
                        if (androidDevice.getPushServer().equals(KConstants.PUSHSERVER.HUAWEI)) {
                            //log.info("放入华为推送队列");
                            HwMsgNotice hwMsgNotice = new HwMsgNotice(androidDevice.getPushToken(), notice);
                            MqMessageSendUtil.convertAndSend(TopicConstant.HW_PUSH_MESSAGE_TOPIC, hwMsgNotice.toString());
                            return;
                        }
                    }*/

                    PushServiceUtils.pushToAndroid(to, notice, androidDevice);
                }
                if (null != iosDevice) {
                    String text = parseText(notice, messageDTO, "ios");
                    if (null == text) {
                        log.error("text 为null 不需要推送 type :{}", notice.getType());
                        return;
                    } else {
                        notice.setText(text);
                    }
                    notice.setText(text);
                    log.info("userId {} deviceId {} content {} ", to, "ios", text);
                    byte flag = SKBeanUtils.getImCoreService().getClientConfig().getIsOpenAPNSorJPUSH();
                    if (0 == flag) {
                        PushServiceUtils.pushToIos(to, notice, iosDevice);
                    } else {
                        Map<String, String> content = Maps.newConcurrentMap();
                        content.put("msg", notice.getText());
                        content.put("regId", iosDevice.getPushToken());
                        content.put("title", notice.getTitle());
                        JPushServices.buildPushObject_ios_tagAnd_alertWithExtrasAndMessage(content, iosDevice);
                    }


                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    protected String parseText(MsgNotice notice, PushMessageDTO messageDTO, String deviceId) {

        // 不是发给客服的消息
       /* if (null != messageDTO.getSrvId() && messageDTO.getSrvId() != notice.getTo()) {
            return defaultMessagePushPlugin.parseText(messageDTO.getType(), notice, messageDTO.getContent(), deviceId);
        }*/

        Map<String, String> serviceMap = queryServiceMap(notice.getTo());

        if (messageDTO.getType() / 100 != 7) {
            // 新访客消息推送
            String msgPush = serviceMap.get(RedisKeyConstant.CustomerServiceKey.NEW_VISITOR_MSGPUSH);
            if ("1".equals(msgPush) && -1 != messageDTO.getSrvId()) {
                return defaultMessagePushPlugin.parseText(messageDTO.getType(), notice, messageDTO.getContent(), deviceId);
            }
            // 同事消息
            String innerTalk = serviceMap.get(RedisKeyConstant.CustomerServiceKey.NEW_INNERTALK_PUSH);
            if ("1".equals(innerTalk) && -1 == messageDTO.getSrvId()) {
                return defaultMessagePushPlugin.parseText(messageDTO.getType(), notice, messageDTO.getContent(), deviceId);
            }
            return null;
        }


       /* String language = "zh";
        String userToken = userCoreRedisRepository.getTokenByUserIdAndDeviceId(notice.getTo(), deviceId);
        KSession kSession = userCoreRedisRepository.queryUserSesson(userToken);
        if (null != kSession) {
            language = kSession.getLanguage();
        }
        Locale requestLocale = LocaleMessageUtils.getRequestLocale(language);
        */

        String text = null;
        try {
            switch (messageDTO.getType()) {
                case MessageType.VISITOR_STATUS_NOTICE:
                    if ("1".equals(notice.getText())) {
                        String msgPush = serviceMap.get(RedisKeyConstant.CustomerServiceKey.NEW_VISITOR_PUSH);
                        if ("1".equals(msgPush)) {
                            text = "新到一个访客:"+notice.getName();
                        } else {
                            log.info("type :{}, msgPush is :{}, serviceId :{}", notice.getType(), msgPush, notice.getTo());
                            return null;
                        }
                    } else if ("2".equals(notice.getText())) {
                        String msgPush = serviceMap.get(RedisKeyConstant.CustomerServiceKey.NEW_TALK_PUSH);
                        if ("1".equals(msgPush)) {
                            text = "新到一个访客咨询:"+notice.getName();
                        } else {
                            log.info("type :{}, msgPush is :{}, serviceId :{}", notice.getType(), msgPush, notice.getTo());
                            return null;
                        }
                    }
                    break;
                case MessageType.SERVICE_TRANSFER:
                    if (messageDTO.getSrvId() != Long.parseLong(messageDTO.getToUserId())) {
                        return null;
                    }
                    if (0 == messageDTO.getFileSize()) {
                        String msgPush = serviceMap.get(RedisKeyConstant.CustomerServiceKey.NEW_TALK_PUSH);
                        if ("1".equals(msgPush)) {
                            text = "新的分配【" + messageDTO.getFromUserName() + "】";
                        } else {
                            return null;
                        }
                    } else {
                        // 转接
                        String fowardHint = serviceMap.get(RedisKeyConstant.CustomerServiceKey.NEW_FOWARD_PUSH);
                        if ("1".equals(fowardHint)&&!StringUtil.isEmpty(messageDTO.getContent())) {
                            text = "来自【" + messageDTO.getContent() + "】的转接访客 【" + messageDTO.getFromUserName() + "】";
                            //text = "来自【" + (StringUtil.isEmpty(messageDTO.getContent()) ? "系统分配" : messageDTO.getContent()) + "】的转接访客 【" + messageDTO.getFromUserName() + "】";
                        } else {
                            return null;
                        }
                    }
                    break;
                case MessageType.CUSTOMER_MENU:
                    text = messageDTO.getFromUserName() + ":" + messageDTO.getContent();
                    break;
                default:
                    return null;
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        if (StringUtils.hasText(text)) {
            if (text.length() > 50) {
                text = text.substring(0, 50) + "...";
            }
        }
        return text;
    }


    public String buildRedisKey(String keyFormat, Object... params) {
        return String.format(keyFormat, params);
    }

    public void updateServiceMap(long serviceId, String key, String value) {
        String redisKey = buildRedisKey(RedisKeyConstant.CustomerServiceKey.CUSTOMER_SERVICE_KEY, serviceId);
        RMap<String, String> map = redissonClient.getMap(redisKey);

        map.put(key, value);


    }

    public String queryServiceMapValue(long serviceId, String key) {

        String redisKey = buildRedisKey(RedisKeyConstant.CustomerServiceKey.CUSTOMER_SERVICE_KEY, serviceId);
        RMap<String, String> rMap = redissonClient.getMap(redisKey);
        return rMap.get(key);

    }

    public void updateServiceMap(long serviceId, Map<String, String> map) {
        if (map.isEmpty()) {
            return;
        }
        String redisKey = buildRedisKey(RedisKeyConstant.CustomerServiceKey.CUSTOMER_SERVICE_KEY, serviceId);
        RMap<String, String> rMap = redissonClient.getMap(redisKey);
        rMap.putAll(map);

    }

    public Map<String, String> queryServiceMap(long serviceId) {

        String redisKey = buildRedisKey(RedisKeyConstant.CustomerServiceKey.CUSTOMER_SERVICE_KEY, serviceId);
        RMap<String, String> rMap = redissonClient.getMap(redisKey);
        return rMap.readAllMap();

    }

    public Set<Long> getServiceList(long companyMpId) {
        String redisKey = buildRedisKey(RedisKeyConstant.CustomerServiceKey.CUSTOMER_SERVICE_LIST_KEY, companyMpId);
        RSet<Long> redisList = redissonClient.getSet(redisKey);

        return redisList.readAll();
    }

    /**
     * 根据公众号ID 查询不在线的客服列表
     *
     * @param companyMpId
     * @return
     */
    public List<Long> queryOffLineServiceList(long companyMpId) {
        Set<Long> redisList = getServiceList(companyMpId);

        if (redisList.isEmpty()) {
            return null;
        }
        List<Long> serviceIdList = new ArrayList<>();
        for (Long serId : redisList) {
            if (!userCoreRedisRepository.queryUserOnline(serId)) {
                serviceIdList.add(serId);
            }
        }
        return serviceIdList;
    }

}
