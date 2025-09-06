package com.basic.im.service;

import com.alibaba.fastjson.JSONObject;
import com.basic.commons.thread.ThreadUtils;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.ex.VerifyUtil;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.message.IMessageRepository;
import com.basic.im.msg.dao.MsgDao;
import com.basic.im.msg.entity.Collect;
import com.basic.im.user.dao.CollectionDao;
import com.basic.im.user.dao.CourseDao;
import com.basic.im.user.dao.CourseMessageDao;
import com.basic.im.user.entity.Course;
import com.basic.im.user.entity.CourseMessage;
import com.basic.im.user.entity.Emoji;
import com.basic.im.user.service.CollectService;
import com.basic.im.user.service.UserCoreService;
import com.basic.im.user.service.UserRedisService;
import com.basic.im.user.service.impl.AbstractCollectService;
import com.basic.im.utils.ConstantUtil;
import com.basic.im.utils.SKBeanUtils;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Slf4j
@Service
public class CollectServiceImpl extends AbstractCollectService {

    /**
     * 添加收藏
     */
    public synchronized Emoji newAddCollection(Integer userId, Emoji emoji) {
        if (emoji.getType() != Emoji.Type.TEXT) {
            Emoji dbEmoji = collectionDao.queryEmojiByUrlAndType(emoji.getMsg(), emoji.getType(), userId);
            if (null != dbEmoji) {
                throw new ServiceException(KConstants.ResultCode.NotRepeatOperation);
            }
            try {
                String copyFile = ConstantUtil.copyFile(-1, emoji.getMsg());
                emoji.setUrl(copyFile);
            } catch (ServiceException e) {
                throw new ServiceException(e.getMessage());
            }
        } else if (emoji.getType() == Emoji.Type.TEXT) {
            emoji.setCollectContent(emoji.getMsg());
        }
        Document emojiMsg = emojiMsg(emoji);
        if (null != emojiMsg) {
            // 格式化body 转译&quot;
            JSONObject test = JSONObject.parseObject(emojiMsg.getString("message"));
            if (3 == test.getIntValue("encryptType")) {
                throw new ServiceException(KConstants.ResultCode.NOTSUPPORT_COLLECT);
            }
            if (emoji.getType() != Emoji.Type.TEXT) {
                if (null != test.get("fileName"))
                    emoji.setFileName(test.get("fileName").toString());
                if (null != test.get("fileSize")) {
                    emoji.setFileSize(Double.parseDouble(test.get("fileSize").toString()));
                }
            }
            if (emoji.getType() == Emoji.Type.VOICE) {
                emoji.setFileLength(Integer.parseInt(test.get("fileTime").toString()));
            }
        }
        emoji.setUserId(userId);
        emoji.setCreateTime(DateUtil.currentTimeSeconds());
        collectionDao.addEmoji(emoji);
        userRedisService.deleteUserCollectCommon(userId); // 维护用户收藏的缓存
        return emoji;
    }

    // 旧版收藏 兼容版本
    @Override
    public List<Object> addCollection(int userId, String roomJid, String msgId, String type) {
        int isSaveMsg = SKBeanUtils.getSystemConfig().getIsSaveMsg();
        if (0 == isSaveMsg)
            throw new ServiceException(KConstants.ResultCode.NOSAVEMSGAND);
        Emoji getEmoji;
        Document data = null;
        List<Object> listEmoji = new ArrayList<>();
        List<String> listMsgId;
        List<String> listType;
        if (!StringUtil.isEmpty(msgId)) {
            listMsgId = Arrays.asList(msgId.split(","));
            listType = Arrays.asList(type.split(","));
            for (int i = 0; i < listMsgId.size(); i++) {
                getEmoji = collectionDao.getEmoji(listMsgId.get(i), userId);
                if (getEmoji == null) {
                    Emoji emoji = new Emoji();
                    emoji.setUserId(userId);
                    emoji.setType(Integer.parseInt(listType.get(i)));
                    if (!StringUtil.isEmpty(roomJid)) {
                        emoji.setRoomJid(roomJid);
                    }

                    if (!StringUtil.isEmpty(listMsgId.get(i))) {
                        emoji.setMsgId(listMsgId.get(i));
                        data = messageRepository.emojiMsg(userId, roomJid, listMsgId.get(i));
                        if (data == null) {
                            continue;
                        }
                        JSONObject jsonObject = JSONObject.parseObject(data.getString("message"));
                        if (3 == jsonObject.getIntValue("isEncrypt")) {
                            throw new ServiceException(KConstants.ResultCode.NOTSUPPORT_COLLECT);
                        }

                    }
                    if (Integer.parseInt(listType.get(i)) != 5) {
                        JSONObject obj = JSONObject.parseObject(data.toJson());
                        try {
                            String copyFile = ConstantUtil.copyFile(-1, obj.get("content").toString());
                            data.replace("content", copyFile);
                            emoji.setUrl(copyFile);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    emoji.setMsg(data.toJson());
                    emoji.setCreateTime(DateUtil.currentTimeSeconds());
                    collectionDao.addEmoji(emoji);
                    listEmoji.add(emoji);
                    // 维护用户收藏的缓存
                    userRedisService.deleteUserCollectCommon(userId);
                    userRedisService.deleteUserCollectEmoticon(userId);
                } else {
                    return null;
                }
            }
        }
        return listEmoji;
    }

    //添加课程
    @Override
    public void addMessageCourse(int userId, List<String> messageIds, long createTime, String courseName, String roomJid) {
        Course course = new Course();
        course.setUserId(userId);
        course.setMessageIds(messageIds);
        course.setCreateTime(createTime);
        course.setCourseName(courseName);
        course.setRoomJid(roomJid);
        courseDao.addCourse(course);
        ThreadUtils.executeInThread(obj -> {
            List<Document> documents = messageRepository.queryMsgDocument(userId, roomJid, messageIds);
            for (Document dbObj : documents) {
                JSONObject jsonObject = JSONObject.parseObject(dbObj.getString("message"));
                if (jsonObject.getLong("deleteTime") > 0) {
                    jsonObject.put("deleteTime", -1);
                    dbObj.put("body", jsonObject.toString());
                    dbObj.put("deleteTime", -1);
                }
                if (Emoji.Type.FILE == jsonObject.getIntValue("encryptType")) {
                    throw new ServiceException(KConstants.ResultCode.NOTSUPPORT_COLLECT);
                }
                courseMessageDao.addCourseMessage(new ObjectId(), course.getUserId(), course.getCourseId().toString(), jsonObject.toJSONString(), String.valueOf(dbObj.get("messageId")), String.valueOf(dbObj.get("timeSend")));
            }
        });
    }

}
