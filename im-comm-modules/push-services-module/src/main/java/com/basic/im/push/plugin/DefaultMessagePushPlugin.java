package com.basic.im.push.plugin;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.constants.MsgType;
import com.basic.im.constant.TopicConstant;
import com.basic.im.entity.PushConfig;
import com.basic.im.friends.entity.Friends;
import com.basic.im.friends.service.FriendsManager;
import com.basic.im.live.entity.LiveRoom;
import com.basic.im.live.service.impl.LiveRoomManagerImpl;
import com.basic.im.message.MessageType;
import com.basic.im.push.i18n.LocaleMessageUtils;
import com.basic.im.push.service.JPushServices;
import com.basic.im.push.service.PushServiceUtils;
import com.basic.im.push.vo.MsgNotice;
import com.basic.im.push.vo.PushMessageDTO;
import com.basic.im.room.service.RoomCoreRedisRepository;
import com.basic.im.room.service.RoomCoreService;
import com.basic.im.user.entity.User;
import com.basic.im.user.model.KSession;
import com.basic.im.utils.MqMessageSendUtil;
import com.basic.im.utils.SKBeanUtils;
import com.basic.utils.StringUtil;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @description: DefaultMessagePushPlugin <br>
 * @date: 2020/7/28 0028  <br>
 * @author: lidaye <br>
 * @version: 1.0 <br>
 */
@Component
public class DefaultMessagePushPlugin extends AbstractMessagePushPlugin implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(DefaultMessagePushPlugin.class);

    private ExecutorService threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);;

    @Autowired
    private FriendsManager friendsManager;

    @Autowired
    private LiveRoomManagerImpl liveRoomManager;

    @Autowired
    private RoomCoreService roomCoreService;

    @Autowired
    private RoomCoreRedisRepository roomCoreRedisRepository;

    public DefaultMessagePushPlugin(){
        PushThread pushThread = new PushThread();
        threadPool.execute(pushThread);

        //SKBeanUtils.getImCoreService().initPushConfig();
        log.info("pushThread  start end ===>");
    }

    /**
     * ArrayListBlockingQueue
     * <p>
     * LinkedBlockingQueue
     */
    private Queue<MsgNotice> queue = new LinkedBlockingQueue<>();

    @Override
    public boolean hanlderPushMessage(JSONObject messageBean) {

        try {
            offLinePush(messageBean);

            return true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
			/*try {
				if((DateUtil.currentTimeSeconds()-KConstants.Expire.HOUR)>jsonMsg.getLong("timeSend")) {
					return;
				}
			} catch (Exception e2) {
				return;
			}*/
//			reSendPushToMq(body);
        }
    }

    /**
     * 重新发 推送消息 发送到队列中
     *
     * @param message
     */
    public void reSendPushToMq(String message) {
        MqMessageSendUtil.sendMessage(TopicConstant.PUSH_MESSAGE_TOPIC, message, true);
    }

    /**
     * 离线推送
     *
     * @param jsonMsg
     */
    public void offLinePush(JSONObject jsonMsg) throws Exception {
        MsgNotice notice;
        try {
            //String c = new String(body.getBytes("iso8859-1"),"utf-8");

            PushMessageDTO pushMessageDTO= JSON.parseObject(jsonMsg.toJSONString(),PushMessageDTO.class);
            log.info("defaultMessage push :{}",jsonMsg.toJSONString());
            notice = parseMsgNotice(pushMessageDTO);

            if (null == notice) {
                return;
            }
            /*
             * if(KConstants.isDebug)
             * log.info("MsgNotice ==> {}  > {} ",notice.getText(),notice.toString());
             */

        } catch (NumberFormatException e) {
            log.error(e.getMessage(), e);
            return;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
        try {
            push(notice);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void push(final MsgNotice notice) {
        try {
            if (!notice.getIsGroup()) {
                /**
                 * 自己发送给自己的消息
                 */
                if (notice.getFrom() == notice.getTo()) {
                    return;
                }
                //pushOne(notice.getTo(), notice);
                queue.offer(notice);
            } else {
                pushGroup(notice.getTo(), notice);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public String initParseText(MsgNotice notice, String text, String deviceId) {
        if (StringUtil.isEmpty(text)) {
            return text;
        }
        String userToken = userCoreRedisRepository.getTokenByUserIdAndDeviceId(notice.getTo(), deviceId);
        KSession kSession = userCoreRedisRepository.queryUserSesson(userToken);
        String language = "zh";
        if (null != kSession) {
            language = kSession.getLanguage();
        }
        Locale requestLocale = LocaleMessageUtils.getRequestLocale(language);
        if (text.equals("localMessage")) {
            text = notice.getName() + notice.getGroupName() + ":" + LocaleMessageUtils.getMessage("localMessage", requestLocale);
        } else if (text.equals("chatLocalMessage")) {
            text = notice.getName() + ":" + LocaleMessageUtils.getMessage("localMessage", requestLocale);
        } else if (text.equals("message")) {
            text = LocaleMessageUtils.getMessage("message", requestLocale);
        } else if (text.equals("burnAfterReadingMessage")) {
            text = LocaleMessageUtils.getMessage("burnAfterReadingMessage", requestLocale);
        } else if (text.equals("virtualBillClearMessage")) {
            text = LocaleMessageUtils.getMessage("virtualBillClearMessage", requestLocale);
        }
        return text;
    }

    //推送给一个用户
    @Override
    public void pushOne(final int to, MsgNotice notice) {
        try {


            if (to == notice.getFrom()) {
                return;
            }
            //判断用户是否开启消息免打扰
            if (!notice.getIsGroup()) {
                //判断用户是否对好友设置了消息免打扰
                if (friendsManager.getFriendIsNoPushMsg(to, notice.getFrom())) {
                    log.info("免打扰======>  userId {} form {} ",to,notice.getFrom());
                    return;
                }
            }

			/*Map<String, DeviceInfo> loginDeviceMap = getUserManager().getLoginDeviceMap(to);
			if(null==loginDeviceMap){
				 log.error("deviceMap is Null > "+to);
				 return;
			}*/

            /**
             * 推送到 接受者 的ios 设备和 android 设备上
             */
			/*androidDevice=loginDeviceMap.get(KConstants.DeviceKey.Android);
			iosDevice=loginDeviceMap.get(KConstants.DeviceKey.IOS);*/
            final User.DeviceInfo androidDevice = userCoreRedisRepository.getAndroidPushToken(to);
            final User.DeviceInfo iosDevice = userCoreRedisRepository.getIosPushToken(to);
            if (null == androidDevice && null == iosDevice) {
                System.out.println("deviceMap is Null, to : " + to);
                return;
            } else if (null != androidDevice && null != iosDevice) {
                if (StringUtil.isEmpty(androidDevice.getPushToken()) &&
                        StringUtil.isEmpty(iosDevice.getPushToken())) {
                    log.error("PushToken is Null > {}", to);
                    return;
                }
            }
            try {
                String finalText = notice.getText();
                if (null != androidDevice) {


                    if(userCoreRedisRepository.queryUserOnline(to, KConstants.DeviceKey.Android)){

                    }else {
                        // 推送内容国际化处理
                        notice.setText(initParseText(notice, notice.getText(), KConstants.DeviceKey.Android));
                        String text = parseText(notice.getType(), notice, notice.getText(), KConstants.DeviceKey.Android);
                        if (null == text) {
                            log.error("text 为null 不需要推送 type :{}", notice.getType());
                            return;
                        }
                        notice.setText(text);
                        PushServiceUtils.pushToAndroid(to, notice, androidDevice);
                    }

                    //log.info("推送设备 "+androidDevice.getPushServer()+"  推送地区"+androidDevice.getAdress());
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




					/*if(0==androidDevice.getOnline()){

					}else {
							log.error("androidDevice  is Online => {}",to);
					}*/
                }
                if (null != iosDevice) {
                    if(userCoreRedisRepository.queryUserOnline(to, KConstants.DeviceKey.IOS)){

                    }else {
                        notice.setText(finalText);
                        notice.setText(initParseText(notice, notice.getText(), KConstants.DeviceKey.IOS));
                        String text = parseText(notice.getType(), notice, notice.getText(), KConstants.DeviceKey.IOS);
                        if (null == text) {
                            log.error("text 为null 不需要推送 type :{}", notice.getType());
                            return;
                        }
                        notice.setText(text);
                        byte flag = SKBeanUtils.getImCoreService().getClientConfig().getIsOpenAPNSorJPUSH();
                        if (0 == flag) {
                            PushServiceUtils.pushToIos(to, notice, iosDevice);
                        } else {
                            Map<String, String> content = Maps.newConcurrentMap();
                            content.put("msg", notice.getText());
                            content.put("regId", iosDevice.getPushToken());
                            content.put("title", notice.getTitle());
                            JPushServices.buildPushObject_ios_tagAnd_alertWithExtrasAndMessage(content,iosDevice);
                        }
                    }


					/*if(0==iosDevice.getOnline()){
						PushServiceUtils.pushToIos(to, notice, iosDevice);
					}else {
							log.error("iosDevice  is Online => {}",to);
					}*/
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
            queue.offer(notice);
        }
    }

    //推送给群组
    private void pushGroup(int to, MsgNotice notice) {
        String finalRoomText = notice.getText();
        if (notice.getType() == 1 && !StringUtil.isEmpty(notice.getObjectId())
                && !notice.getObjectId().equals(notice.getRoomJid())) {
            //@ 群成员
            String[] objectIdlist = notice.getObjectId().split(" ");
            for (int i = 0; i < objectIdlist.length; i++) {
                try {
                    ObjectId roomId = roomCoreService.getRoomId(notice.getRoomJid());
                    log.info("免打扰 roomId :{}  userId : {}     boolean :{}", roomId, Integer.valueOf(objectIdlist[i]), roomCoreService.getMemberIsNoPushMsg(roomId, Integer.valueOf(objectIdlist[i])));
                    if (roomCoreService.getMemberIsNoPushMsg(roomId, Integer.valueOf(objectIdlist[i]))) {
                        continue;
                    }
                    notice.setTo(Integer.parseInt(objectIdlist[i]));
                    //notice.setToName(getUserManager().getNickName(Integer.parseInt(objectIdlist[i])));
                    notice.setStatus(1);
                    notice.setText(finalRoomText);
                    pushOne(Integer.parseInt(objectIdlist[i]), notice);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

            }
        } else {
            List<Integer> groupUserList = null;
            try {
                groupUserList = roomCoreRedisRepository.queryRoomPushMemberUserIds(notice.getRoomJid());
                groupUserList.remove((Integer) notice.getFrom());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                queue.offer(notice);
                return;
            }
            for (Integer userId : groupUserList) {
                //notice.setToName(getUserManager().getNickName(userId));
                notice.setTo(userId);
                notice.setStatus(1);
                notice.setText(finalRoomText);
                pushOne(userId, notice);
            }
        }


    }

    @Override
    protected MsgNotice parseMsgNotice(PushMessageDTO messageDTO) throws Exception {
        MsgNotice notice = new MsgNotice();
        int messageType;
        String text;
        String jid;

        try {
            String msgId = messageDTO.getMessageId();
            if (!StringUtil.isEmpty(msgId)) {
                notice.setMessageId(msgId);
            }

            messageType = messageDTO.getType();
            if (MsgType.TYPE_RED_BACK != messageType) {
                notice.setFrom((int) messageDTO.getFromUserId());
            }

            if (StringUtil.isEmpty(notice.getName())) {
                notice.setName(messageDTO.getFromUserName());
            }
            notice.setToName(messageDTO.getToUserName());
            notice.setFileName(messageDTO.getFileName());
            jid = messageDTO.getRoomJid();
            notice.setIsGroup(messageDTO.isGroup());
            if (!notice.getIsGroup()) {
                notice.setTo(Integer.parseInt(messageDTO.getToUserId()));
            }
            notice.setTitle(notice.getName());
            if (!StringUtil.isEmpty(messageDTO.getContent()) &&
                    messageDTO.getContent().contains("http://api.map.baidu.com/staticimage")) {
                messageType = MsgType.TYPE_LOCATION;
                if (notice.getIsGroup()) {
                    notice.setTitle(roomCoreService.getRoomName(jid));
                    notice.setRoomJid(jid);
                    notice.setGroupName("(" + notice.getTitle() + ")");
//						text=notice.getName()+notice.getGroupName()+":[位置]";
                    text = "localMessage";
                } else {
                    notice.setTitle(notice.getName());
//						text=notice.getName()+":[位置]";
                    text = "chatLocalMessage";
                }
            }
            if (!StringUtil.isEmpty(messageDTO.getObjectId())) {
                notice.setObjectId(messageDTO.getObjectId());
            }
            LiveRoom liveRoom = null;
            if (!StringUtil.isEmpty(jid)) {
                notice.setRoomJid(jid);
                if (!MessageType.liveRoomType.contains(messageType)) {
                    ObjectId roomId = roomCoreService.getRoomId(jid);
                    if (null != roomId) {
                        notice.setRoomId(roomId.toString());
                    }

                    notice.setTitle(roomCoreService.getRoomName(jid));
                } else {
                    liveRoom = liveRoomManager.getLiveRoomByJid(jid);
                    if (null != liveRoom) {
                        notice.setRoomId(liveRoom.getRoomId().toString());
                        notice.setTitle(liveRoom.getName());
                    }

                }
                if(StringUtil.isEmpty(notice.getTitle())){
                    //群组不存在
                    return null;
                }
                notice.setGroupName("(" + notice.getTitle() + ")");
            } else if (!StringUtil.isEmpty(messageDTO.getObjectId())) {
                if (!MessageType.liveRoomType.contains(messageType)) {
                    String roomName = roomCoreService.getRoomName(messageDTO.getObjectId());
                    if (null != roomName) {
                        notice.setTitle(roomName);
                        notice.setGroupName("(" + notice.getTitle() + ")");
                    }

                } else if (MessageType.liveRoomType.contains(messageType)) {
                    liveRoomManager.getLiveRoomByJid(messageDTO.getObjectId());
                    if (null != liveRoom) {
                        notice.setTitle(liveRoom.getName());
                        notice.setGroupName("(" + notice.getTitle() + ")");
                    }
                }
                if(StringUtil.isEmpty(notice.getTitle())){
                    //群组不存在
                    return null;
                }
            }
            if (0 < messageDTO.getIsEncrypt() || 0 < messageDTO.getEncryptType()) {
//				text="[消息]";
                text = "message";
            } else if (1 == messageDTO.getIsReadDel()) {
//				text = "[点击查看T]";// 阅后即焚消息
                text = "burnAfterReadingMessage";// 阅后即焚消息
            } else if (MsgType.TYPE_RICH_TEXT == messageType){
                //富文本群发 用大标题显示通知
                JSONObject jsonObject = JSONObject.parseObject(messageDTO.getContent());
                text = jsonObject.getString("title");
            } else if (MsgType.TYPE_SHARE_CONTENT == messageType){
                JSONObject jsonObject = JSONObject.parseObject(messageDTO.getContent());
                text = jsonObject.getString("appName");
            } else {
                text = messageDTO.getContent();
            }
            notice.setType(messageType);
//			text = parseText(messageType, notice, text);
			/*if (null == text) {
				//log.error("{} text 为null 不需要推送。。。。",msgId);
				return null;
			}*/
        } catch (Exception e) {
            throw e;
        }

        notice.setText(text);
        if (!notice.getIsGroup()) {
            Friends friends = friendsManager.getFriends(notice.getTo(), notice.getFrom());
            if (null != friends && !StringUtil.isEmpty(friends.getRemarkName())) {
                notice.setName(friends.getRemarkName());
            }
            if (StringUtil.isEmpty(notice.getToName())) {
                notice.setToName(userCoreService.getNickName(notice.getTo()));
            }
        } else {
            notice.setName(userCoreService.getNickName(notice.getFrom()));
        }
        return notice;
    }

    protected String parseText(int messageType, MsgNotice notice, String content, String deviceId) {
        String userToken = userCoreRedisRepository.getTokenByUserIdAndDeviceId(notice.getTo(), deviceId);
        KSession kSession = userCoreRedisRepository.queryUserSesson(userToken);
        String language = "zh";
        if (null != kSession) {
            language = kSession.getLanguage();
        }
        Locale requestLocale = LocaleMessageUtils.getRequestLocale(language);
        String text = null;
        try {
            switch (messageType) {
                case MsgType.TYPE_TEXT:
                case MsgType.TYPE_EDIT:
                case MsgType.TYPE_FEEDBACK:
                    log.info("content: {}", content);
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + content;
                    log.info("text : {}", text);
                    log.info("title : {}", notice.getTitle());
                    log.info("finall text :{}", notice.getText());
					/*if(deviceId.equals("android")){
//						notice.setTitle((notice.getIsGroup() ? notice.getName() + notice.getGroupName() : notice.getName()));
						text = notice.getName()+": "+ content;
						log.info("title :{} ",notice.getTitle());
						log.info("text {}",text);
					}else{
						text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ": " + content;
					}*/
                    if (notice.getIsGroup()) {
                        if (!StringUtil.isEmpty(notice.getObjectId()) && !StringUtil.isEmpty(notice.getRoomJid())) {
//							text="[有人@我]"+notice.getName()+notice.getGroupName()+":"+content;
                            text = LocaleMessageUtils.getMessage("byAlt", requestLocale) + notice.getName() + notice.getGroupName() + ":" + content;
                            if (notice.getObjectId().equals(notice.getRoomJid())) {
//								text="[有全体消息]"+notice.getName()+notice.getGroupName()+":"+content;
                                text = LocaleMessageUtils.getMessage("allNews", requestLocale) + notice.getName() + notice.getGroupName() + ":" + content;
                            }
                        }
                    }
                    break;
                case MsgType.TYPE_IMAGE:
                case MsgType.TYPE_GIF:
//					text=notice.getName()+(notice.getIsGroup()?notice.getGroupName():"")+":[图片]";
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + LocaleMessageUtils.getMessage("picture", requestLocale);
                    break;
                case MsgType.TYPE_VOICE:
//					text=notice.getName()+(notice.getIsGroup()?notice.getGroupName():"")+":[语音]";
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + LocaleMessageUtils.getMessage("voice", requestLocale);
                    break;
                case MsgType.TYPE_LOCATION:
//					text=notice.getName()+(notice.getIsGroup()?notice.getGroupName():"")+":[位置]";
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + LocaleMessageUtils.getMessage("position", requestLocale);
                    break;
                case MsgType.TYPE_VIDEO:
//					text=notice.getName()+(notice.getIsGroup()?notice.getGroupName():"")+":[视频]";
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + LocaleMessageUtils.getMessage("video", requestLocale);
                    break;
                case MsgType.TYPE_CARD:
//					text=notice.getName()+(notice.getIsGroup()?notice.getGroupName():"")+":[名片]";
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + LocaleMessageUtils.getMessage("businessCard", requestLocale);
                    break;
                case MsgType.TYPE_FILE:
//					text=notice.getName()+(notice.getIsGroup()?notice.getGroupName():"")+":[文件]";
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + LocaleMessageUtils.getMessage("file", requestLocale);
                    break;
                case MsgType.TYPE_RED:
//					text=notice.getName()+(notice.getIsGroup()?notice.getGroupName():"")+":[红包]";
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + LocaleMessageUtils.getMessage("redPacket", requestLocale);
                    break;
                case MsgType.TYPE_SHARE_VLOG:
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + LocaleMessageUtils.getMessage("redPacket", requestLocale);
                    break;
                case MsgType.TYPE_LINK:
                case MsgType.TYPE_SHARE_LINK:
//					text=notice.getName()+(notice.getIsGroup()?notice.getGroupName():"")+":[链接]";
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + LocaleMessageUtils.getMessage("link", requestLocale);
                    break;
                case MsgType.TYPE_83:
//                  text=notice.getName()+(notice.getIsGroup()?notice.getGroupName():"")+":领取了红包";
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + LocaleMessageUtils.getMessage("receiveRedPacket", requestLocale);
                    break;
                case MsgType.TYPE_SHAKE:
//                    text=notice.getName()+":[戳一戳]";
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("toPoke", requestLocale);
                    break;
                case MsgType.TYPE_CHAT_HISTORY:
//                    text=notice.getName()+":[聊天记录]";
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("chatRrecord", requestLocale);
                    break;
                case MsgType.TYPE_RED_BACK:
//                    text="系统消息:[红包退款]";
                    text = LocaleMessageUtils.getMessage("refundRedPacket", requestLocale);
                    break;

                case MsgType.TYPE_REPLY:
                  text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + content;
                    break;
                case MsgType.TYPE_TRANSFER:
//                    text=notice.getName()+(notice.getIsGroup()?notice.getGroupName():"")+":[转账]";
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + LocaleMessageUtils.getMessage("transferAccounts", requestLocale);
                    break;
                case MsgType.TYPE_DICE:
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + LocaleMessageUtils.getMessage("dice", requestLocale);
                    break;
                case MsgType.TYPE_RPS:
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + LocaleMessageUtils.getMessage("rockPaperScissors", requestLocale);
                    break;
                case MsgType.TYPE_RECEIVETRANSFER:
//                    text=notice.getName()+(notice.getIsGroup()?notice.getGroupName():"")+":领取了转账";
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + LocaleMessageUtils.getMessage("receiveTransferAccounts", requestLocale);
                    break;
                case MsgType.TYPE_REFUNDTRANSFER:
//                    text="系统消息:[转账退款]";
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + LocaleMessageUtils.getMessage("transferAccountsRefund", requestLocale);
                    break;
                case MsgType.TYPE_IMAGE_TEXT:
                case MsgType.TYPE_IMAGE_TEXT_MANY:
//                    text=notice.getName()+(notice.getIsGroup()?notice.getGroupName():"")+":[图文]";
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + LocaleMessageUtils.getMessage("imageText", requestLocale);
                    break;
                case MsgType.TYPE_BACK:
//                    text=notice.getName()+(notice.getIsGroup()?notice.getGroupName():"")+":撤回了一条消息";
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + LocaleMessageUtils.getMessage("messageRecall", requestLocale);
                    break;
                case MsgType.DIANZAN:
//                    text=notice.getName()+":点赞了与我相关的 [生活圈]";
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("lifeCircle", requestLocale);
                    break;
                case MsgType.PINGLUN:
//                    text=notice.getName()+":评论了与我相关的 [生活圈]";
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("comment", requestLocale);
                    break;
                case MsgType.ATMESEE:
//                    text=notice.getName()+":提到了你 [生活圈]";
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("mention", requestLocale);
                    break;
                case MsgType.TYPE_MUCFILE_ADD:
//                    text=notice.getName()+(notice.getIsGroup()?notice.getGroupName():"")+":上传了群文件";
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + LocaleMessageUtils.getMessage("uploadGroupFile", requestLocale);
                    break;
                case MsgType.TYPE_MUCFILE_DEL:

//                    text=notice.getName()+(notice.getIsGroup()?notice.getGroupName():"")+":删除了文件";
                    text = notice.getName() + (notice.getIsGroup() ? notice.getGroupName() : "") + ":" + LocaleMessageUtils.getMessage("deleteFile", requestLocale);
                    break;
                case MsgType.TYPE_SAYHELLO:
//                    text=notice.getName()+":请求加为好友";
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("requesToBeFriends", requestLocale);
                    break;
                case MsgType.TYPE_FRIEND:
//                    text=notice.getName()+":加了你为好友";
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("toBeFriends", requestLocale);
                    break;
                case MsgType.TYPE_DELALL:
//                    text=notice.getName()+":解除了好友关系";
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("deleteFriends", requestLocale);
                    break;
                case MsgType.TYPE_PASS:
//                    text=notice.getName()+":同意加为好友";
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("agreeToBeFriends", requestLocale);
                    break;
				/*case MsgType.TYPE_CHANGE_NICK_NAME:
					notice.setIsGroup(1);
					text=notice.getName()+(notice.getGroupName())+":修改了群昵称为"+content;
					break;*/
                case MsgType.TYPE_CHANGE_ROOM_NAME:
//                    text='"'+notice.getName()+'"'+(notice.getGroupName())+":修改了群名为"+content;
                    text = '"' + notice.getName() + '"' + (notice.getGroupName()) + ":" + LocaleMessageUtils.getMessage("updateRoomName", requestLocale);
                    break;
                case MsgType.TYPE_DELETE_ROOM:
//                    text="("+content+"):群组已被解散";
                    text = content + ":" + LocaleMessageUtils.getMessage("disbandRoom", requestLocale);
                    break;
                case MsgType.TYPE_DELETE_MEMBER:

                    if (notice.getFrom() == notice.getTo()) {
                        //notice.setName(SKBeanUtils.getUserManager().getNickName(notice.getTo()));
//                        text='"'+notice.getName()+'"'+" 退出了群组:"+(notice.getGroupName());
                        text = notice.getName() + ":" + LocaleMessageUtils.getMessage("dropOutRoom", requestLocale) + " " + (notice.getGroupName());
                    } else {
                        //notice.setName(SKBeanUtils.getUserManager().getNickName(notice.getTo()));
//                        text='"'+notice.getName()+'"'+" 把你移出了群组:"+(notice.getGroupName());
                        text = '"' + notice.getName() + '"' + LocaleMessageUtils.getMessage("removeRoomFromYou", requestLocale) + (notice.getGroupName());
                    }
                    break;
                case MsgType.TYPE_NEW_NOTICE:

//                    text='"'+notice.getName()+'"'+(notice.getGroupName())+" 发布了群公告:"+content;
                    text = '"' + notice.getName() + '"' + (notice.getGroupName()) + " " + LocaleMessageUtils.getMessage("releaseRoomNotice", requestLocale) + content;
                    break;
                case MsgType.TYPE_GAG:
                    long ts = Long.parseLong(content);
                    //long time=DateUtil.currentTimeSeconds();
                    if (0 < ts) {
//                        text='"'+notice.getName()+'"'+(notice.getGroupName())+" 你被禁言了";
                        text = '"' + notice.getName() + '"' + (notice.getGroupName()) + " " + LocaleMessageUtils.getMessage("youAreBanned", requestLocale);
                    } else {
//                        text='"'+notice.getName()+'"'+(notice.getGroupName())+" 取消了禁言";
                        text = '"' + notice.getName() + '"' + (notice.getGroupName()) + " " + LocaleMessageUtils.getMessage("cancelBanned", requestLocale);
                    }
                    break;
                case MsgType.NEW_MEMBER:
                    if (!notice.getIsGroup()) {
                        if (notice.getFrom() != notice.getTo()) {
//                            text='"'+notice.getName()+'"'+" 邀请你加入了群组:"+(notice.getGroupName());
                            text = '"' + notice.getName() + " " + '"' + LocaleMessageUtils.getMessage("invitedYuToJoinGroup", requestLocale) + ":" + notice.getGroupName();
                        } else {
//                            text='"'+notice.getName()+'"'+" 加入了群组:"+(notice.getGroupName());
                            text = '"' + notice.getName() + " " + '"' + LocaleMessageUtils.getMessage("joinGroup", requestLocale) + ":" + notice.getGroupName();
                        }
                    } else {
                        if (notice.getFrom() != notice.getTo()) {
//								text='"'+notice.getName()+'"'+" 邀请 '"+notice.getToName()+"' 加入了群组:"+(notice.getGroupName());
                            text = '"' + notice.getName() + " " + '"' + LocaleMessageUtils.getMessage("invited", requestLocale) + notice.getToName() + LocaleMessageUtils.getMessage("joinGroup", requestLocale) + ":" + (notice.getGroupName());
                        } else {
//								text='"'+notice.getName()+'"'+" 加入了群组:"+(notice.getGroupName());
                            text = '"' + notice.getName() + " " + '"' + LocaleMessageUtils.getMessage("joinGroup", requestLocale) + ":" + notice.getGroupName();
                        }
                    }


                    break;
                case MsgType.TYPE_SEND_MANAGER:
                    if (1 == Integer.parseInt(content)) {
//                        text=notice.getName()+(notice.getGroupName())+" 你被设置了管理员";
                        text = notice.getName() + (notice.getGroupName()) + LocaleMessageUtils.getMessage("administrator", requestLocale);
                    } else {
//                        text=notice.getToName()+(notice.getGroupName())+" 你被取消了管理员";
                        text = notice.getName() + (notice.getGroupName()) + LocaleMessageUtils.getMessage("cancelAdministrator", requestLocale);
                    }
                    break;
                case MsgType.TYPE_GROUP_TRANSFER:
//					text=notice.getName()+" 把群组 "+(notice.getGroupName())+" 群组转让给你了";
                    text = notice.getName() + LocaleMessageUtils.getMessage("putGroup", requestLocale) + (notice.getGroupName()) + LocaleMessageUtils.getMessage("groupTransferredToYou", requestLocale);
                    break;
                case MsgType.TYPE_INPUT:
                case MsgType.TYPE_CHANGE_SHOW_READ:
                    return null;
                case MsgType.TYPE_IS_MU_CONNECT_TALK:
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("initiateConnectTalk", requestLocale);
                    break;
                case MsgType.TYPE_IS_SCREEN_SHARING:
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("screenSharing", requestLocale);
                    break;
                case MsgType.TYPE_IS_CONNECT_VOICE:
//					text=notice.getName()+":邀请您语音通话";
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("invitesVoiceCall", requestLocale);
                    break;
                case MsgType.TYPE_IS_CONNECT_VIDEO:
//					text=notice.getName()+":邀请您视频通话";
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("invitesVideoCall", requestLocale);
                    break;
                case MsgType.TYPE_IS_MU_CONNECT_Video:
//					text=notice.getName()+":邀请您加入视频会议";
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("invitesJoinVideoConference", requestLocale);
                    break;
                case MsgType.TYPE_IS_MU_CONNECT_VOICE:
//					text=notice.getName()+":邀请您加入语音会议";
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("invitesJoinVoiceConference", requestLocale);
                    break;
                case MsgType.TYPE_NO_CONNECT_VOICE:
//					text=notice.getName()+":取消了语音通话";
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("cancelVoiceCall", requestLocale);
                    break;
                case MsgType.TYPE_NO_CONNECT_VIDEO:
//					text=notice.getName()+":取消了视频通话";
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("cancelVideoCall", requestLocale);
                    break;
                case MsgType.TYPE_IS_MU_END_CONNECT_VIDEO:
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("endVideoCall", requestLocale);
                    break;
                case MsgType.TYPE_IS_MU_END_CONNECT_VOICE:
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("endVoiceCall", requestLocale);
                    break;
                case MsgType.CODEPAYMENT:
//					text = notice.getName()+":付款成功";
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("successfulPayment", requestLocale);
                    break;

                case MsgType.CODEARRIVAL:
//					text = notice.getName()+":收款成功";
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("successfulCollectPayment", requestLocale);
                    break;
                case MsgType.CODERECEIPT:
//					text = notice.getName()+":二维码付款成功";
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("qrCodeSuccessfulPayment", requestLocale);
                    break;
                case MsgType.CODEERECEIPTARRIVAL:
//					text = notice.getName()+":二维码收款成功";
                    text = notice.getName() + ":" + LocaleMessageUtils.getMessage("qrCodeSuccessfulPayment", requestLocale);
                    break;
                case MsgType.MANUAL_RECHARGE:
//					text = "充值审核通知";
                    text = LocaleMessageUtils.getMessage("rechargeAuditNotice", requestLocale);
                    break;
                case MsgType.MANUAL_WITHDRAW:
//					text = "提现审核通知";
                    text = LocaleMessageUtils.getMessage("withdrawalAuditNotice", requestLocale);
                    break;
                default:

                    if (100 < messageType) {
                        return null;
                    } else if (StringUtil.isEmpty(content)) {
                        return null;
                    } else {
                        text = content;
                    }
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (StringUtils.hasText(text)) {
            if (text.length() > 50) {
                text = text.substring(0, 50) + "...";
            }
        }
        return text;
    }



    @Autowired
    private Environment environment;

    @Override
    public void run(String... args) throws Exception {
       if(!PushServiceUtils.initPushConfig()) {

           initPushConfig();
       }
    }
    /**
     * 初始化推送配置
     **/
    private void initPushConfig() {
        List<PushConfig> pushConfigs = SKBeanUtils.getImCoreService().getParentPushConfig();
        if (pushConfigs.size() > 0){
            return;
        }
        if(StringUtil.isEmpty(environment.getProperty("im.pushConfig.packageName"))){
            return;
        }
        PushConfig pushConfig = new PushConfig();
        pushConfig.setId(10000);
        pushConfig.setPackageName(environment.getProperty("im.pushConfig.packageName"));
        pushConfig.setAppStoreAppId(environment.getProperty("im.pushConfig.appStoreAppId"));
        pushConfig.setBetaAppId(environment.getProperty("im.pushConfig.betaAppId"));
        //安卓配置
        PushConfig.AndroidPush androidPush = new PushConfig.AndroidPush();
        androidPush.setXmAppSecret(environment.getProperty("im.pushConfig.xm_appSecret"));
        androidPush.setXmChannle(0);
        androidPush.setHwAppSecret(environment.getProperty("im.pushConfig.hw_appSecret"));
        androidPush.setHwAppId(environment.getProperty("im.pushConfig.hw_appId"));
        androidPush.setHwTokenUrl(environment.getProperty("im.pushConfig.hw_tokenUrl"));
        androidPush.setHwApiUrl(environment.getProperty("im.pushConfig.hw_apiUrl"));
        androidPush.setHwIconUrl(environment.getProperty("im.pushConfig.hw_iconUrl"));
        androidPush.setIsOpen((byte) 0);
        androidPush.setServerAdress("CN");
        androidPush.setJPushAppKey(environment.getProperty("im.pushConfig.jPush_appkey"));
        androidPush.setJPushMasterSecret(environment.getProperty("im.pushConfig.jPush_masterSecret"));
        androidPush.setFcmDataBaseUrl(environment.getProperty("im.pushConfig.FCM_dataBaseUrl"));
        androidPush.setFcmKeyJson(environment.getProperty("im.pushConfig.FCM_keyJson"));
        androidPush.setMzAppSecret(environment.getProperty("im.pushConfig.mz_appSecret"));
        if (!StringUtil.isEmpty(environment.getProperty("im.pushConfig.mz_appId"))){androidPush.setMzAppId(Integer.valueOf(environment.getProperty("im.pushConfig.mz_appId")));}
        if (!StringUtil.isEmpty(environment.getProperty("im.pushConfig.vivo_appId"))){androidPush.setVivoAppId(Integer.valueOf(environment.getProperty("im.pushConfig.vivo_appId")));}
        androidPush.setVivoAppKey(environment.getProperty("im.pushConfig.vivo_appKey"));
        androidPush.setVivoAppSecret(environment.getProperty("im.pushConfig.vivo_appSecret"));
        androidPush.setOppoAppKey(environment.getProperty("im.pushConfig.oppo_appKey"));
        androidPush.setOppoMasterSecret(environment.getProperty("im.pushConfig.oppo_masterSecret"));
        //IOSPush ios推送配置
        PushConfig.IOSPush iosPush = new PushConfig.IOSPush();
        iosPush.setAppStoreApnsPk(environment.getProperty("im.pushConfig.appStoreApnsPk"));
        iosPush.setBetaApnsPk(environment.getProperty("im.pushConfig.betaApnsPk"));
        iosPush.setPkPassword(environment.getProperty("im.pushConfig.pkPassword"));
        if(!StringUtil.isEmpty(environment.getProperty("im.pushConfig.isApnsSandbox"))){iosPush.setIsApnsSandbox(Byte.valueOf(environment.getProperty("im.pushConfig.isApnsSandbox").trim()));}
        if(!StringUtil.isEmpty(environment.getProperty("im.pushConfig.isDebug"))){iosPush.setIsDebug(Byte.valueOf(environment.getProperty("im.pushConfig.isDebug").trim()));}
        iosPush.setBdAppStoreAppId(environment.getProperty("im.pushConfig.bd_appStore_appId"));
        iosPush.setBdAppStoreAppKey(environment.getProperty("im.pushConfig.bd_appStore_appKey"));
        iosPush.setBdAppStoreSecretKey(environment.getProperty("im.pushConfig.bd_appStore_secret_key"));
        iosPush.setBdAppKey(environment.getProperty("im.pushConfig.bd_appKey").split(","));
        iosPush.setBdRestUrl(environment.getProperty("im.pushConfig.bd_rest_url"));
        iosPush.setBdSecretKey(environment.getProperty("im.pushConfig.bd_secret_key").split(","));

        pushConfig.setAndroidPush(androidPush);
        pushConfig.setIosPush(iosPush);
        SKBeanUtils.getImCoreService().savePushConfig(pushConfig);

        log.info("\n" + ">>>>>>>>>>>>>>> 推送配置数据初始化完成  <<<<<<<<<<<<<");
    }

    public class PushThread extends Thread {

        @Override
        public void run() {
            while (true) {
                if (queue.isEmpty()) {
                    try {
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    try {
                        MsgNotice notice = queue.poll();
                        if (null == notice) {
                            return;
                        }
                        if (!notice.getIsGroup()) {
                            pushOne(notice.getTo(), notice);
                        } else if (1 == notice.getStatus()) {
                            pushOne(notice.getTo(), notice);
                        } else if (notice.getIsGroup()) {
                            pushGroup(notice.getTo(), notice);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.error(e.getMessage(), e);
                    }

                }
            }
        }
    }
}
