package com.basic.im.user.service.impl;

import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
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
import com.basic.im.room.service.RoomManager;
import com.basic.im.user.dao.CollectionDao;
import com.basic.im.user.dao.CourseDao;
import com.basic.im.user.dao.CourseMessageDao;
import com.basic.im.user.entity.Course;
import com.basic.im.user.entity.CourseMessage;
import com.basic.im.user.entity.Emoji;
import com.basic.im.user.service.CollectService;
import com.basic.im.user.service.UserCoreService;
import com.basic.im.user.service.UserRedisService;
import com.basic.im.utils.ConstantUtil;
import com.basic.im.utils.SKBeanUtils;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

@Slf4j
public abstract class AbstractCollectService implements CollectService {


    @Autowired
    protected UserCoreService userCoreService;

    @Autowired
    protected UserRedisService userRedisService;

    @Autowired
    @Lazy
    protected IMessageRepository messageRepository;

    @Autowired
    protected CollectionDao collectionDao;

    @Autowired
    protected CourseMessageDao courseMessageDao;

    @Autowired
    protected CourseDao courseDao;

    @Lazy
    @Autowired(required=false)
    protected MsgDao msgDao;

    @Autowired
    private RoomManager roomManager;



    /**
     * 添加新的收藏
     */
    public Emoji addNewEmoji(String emoji) {
        Emoji newEmoji = null;
        if (StringUtil.isEmpty(emoji)) {
            throw new ServiceException("addNewEmoji emoji is null");
        }
        List<Emoji> emojiList = JSONObject.parseArray(emoji, Emoji.class);
        Integer userId = VerifyUtil.verifyUserId(ReqUtil.getUserId());
        for (Emoji emojis : emojiList) {
            emojis.setUserId(userId);
            Emoji emoji1;

            // 朋友圈单张图片与朋友圈单个视频，不保存朋友圈动态Id，作为独立的收藏
            // 单张图片与单张短视频可以重复收藏
            if (emojis.getType() == Emoji.Type.ONE_PHOTO || emojis.getType() == Emoji.Type.ONE_VIDEO){
                emojis.setCollectMsgId(StrUtil.EMPTY);
                emojis.setCreateTime(DateUtil.currentTimeSeconds());
                collectionDao.addEmoji(emojis);
                ThreadUtils.executeInThread(obj -> {
                    ConstantUtil.copyFile(emojis.getUrl());              // 复制文件
                    userRedisService.deleteUserCollectCommon(userId);    // 维护用户收藏的缓存
                });
                continue;
            }
            // 执行其他收藏类型收藏逻辑
            if (!StringUtil.isEmpty(emojis.getMsgId())) {
                emoji1 = collectionDao.getEmoji(emojis.getMsg(), emojis.getType(), emojis.getUserId(), emojis.getMsgId());
            } else {
                emoji1 = collectionDao.getEmoji(emojis.getMsg(), emojis.getType(), emojis.getUserId(), null);
            }
            // 检测是否重复，此处重复会抛出隐藏
            checkRepeat(emoji1,emojis);
            // 根据不同的收藏类型进行收藏
            if (!StringUtil.isEmpty(emojis.getMsgId()) && 0 == emojis.getCollectType()) {
                newEmoji = newAddCollection(userId, emojis);    // 添加收藏
            } else if (StringUtil.isEmpty(emojis.getMsgId()) && 0 == emojis.getCollectType()) {
                newEmoji = newAddEmoji(userId, emojis);          // 添加表情
            } else if (StringUtil.isEmpty(emojis.getMsgId()) && -1 == emojis.getCollectType()) {
                newEmoji = newAddCollection(userId, emojis);    // 无关消息的相关收藏
            }
            // 朋友圈收藏
            if (StringUtil.isEmpty(emojis.getMsgId())
                    && 1 == emojis.getCollectType()
                    && StringUtil.isEmpty(emojis.getTitle())
                    && StringUtil.isEmpty(emojis.getShareURL())) {
                newEmoji = msgCollect(userId, emojis, 0);
                saveCollect(VerifyUtil.verifyObjectId(newEmoji.getCollectMsgId()), userCoreService.getNickName(emojis.getUserId()), emojis.getUserId());
            } else if (StringUtil.isEmpty(emojis.getMsgId())
                    && 1 == emojis.getCollectType()
                    && !StringUtil.isEmpty(emojis.getTitle())
                    && !StringUtil.isEmpty(emojis.getShareURL())) {
                newEmoji = msgCollect(userId, emojis, 1); // SDK分享链接
                saveCollect(VerifyUtil.verifyObjectId(newEmoji.getCollectMsgId()), userCoreService.getNickName(emojis.getUserId()), emojis.getUserId());
            }
        }
        return newEmoji;
    }



    private Emoji msgCollect(Integer userId, Emoji msgEmoji, int isShare) {
        StringBuilder buffer = new StringBuilder();
        if (msgEmoji.getType() != Emoji.Type.TEXT) {
            String[] msgs = msgEmoji.getMsg().split(",");
            String copyFile;
            String newCopyFile;
            for (String msg : msgs) {
                copyFile = ConstantUtil.copyFile(msg);
                buffer.append(copyFile).append(",");
            }
            newCopyFile = buffer.deleteCharAt(buffer.length() - 1).toString();
            msgEmoji.setUrl(newCopyFile);
        }
        Emoji emoji = null;
        if (0 == isShare) {
            emoji = new Emoji(msgEmoji.getUserId(), msgEmoji.getType(), (Emoji.Type.TEXT == msgEmoji.getType() ? null : msgEmoji.getUrl()), msgEmoji.getMsg(),
                    msgEmoji.getFileName(), msgEmoji.getFileSize(), msgEmoji.getFileLength(), msgEmoji.getCollectType(), msgEmoji.getCollectContent(), msgEmoji.getCollectMsgId());
        } else if (1 == isShare) {
            emoji = new Emoji(msgEmoji.getUserId(), msgEmoji.getType(), (Emoji.Type.TEXT == msgEmoji.getType() ? null : msgEmoji.getUrl()), msgEmoji.getMsg(),
                    msgEmoji.getFileName(), msgEmoji.getFileSize(), msgEmoji.getFileLength(), msgEmoji.getCollectType(), msgEmoji.getCollectContent(), msgEmoji.getCollectMsgId(), msgEmoji.getTitle(), msgEmoji.getShareURL());
        }

        addParam(emoji,msgEmoji);
        // Msg msg = msgDao.get(msgEmoji.getUserId(), new ObjectId(msgEmoji.getCollectMsgId()));
        collectionDao.addEmoji(emoji);
        // 维护朋友圈收藏
        userRedisService.deleteUserCollectCommon(userId);
        return emoji;
    }

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
                String copyFile = ConstantUtil.copyFile( emoji.getMsg());
                emoji.setUrl(copyFile);
            } catch (ServiceException e) {
                throw new ServiceException(e.getMessage());
            }
        } else if (emoji.getType() == Emoji.Type.TEXT) {
            emoji.setCollectContent(emoji.getMsg());
        }
        if (-1 != emoji.getCollectType()) {
            Document emojiMsg = emojiMsg(emoji);
            if (null != emojiMsg) {
                // 格式化 body 转译 &quot;
                JSONObject test = JSONObject.parseObject(emojiMsg.getString("body"));
                if (Emoji.Type.FILE == test.getIntValue("isEncrypt")) {
                    throw new ServiceException(KConstants.ResultCode.NOTSUPPORT_COLLECT);
                }
                if (emoji.getType() != Emoji.Type.TEXT) {
                    if (null != test.get("fileName")) {
                        emoji.setFileName(test.get("fileName").toString());
                    }
                    if (null != test.get("fileSize")) {
                        emoji.setFileSize(Double.parseDouble(test.get("fileSize").toString()));
                    }
                }
                if (emoji.getType() == Emoji.Type.VOICE) {
                    emoji.setFileLength(Integer.parseInt(test.get("timeLen").toString()));
                }
            }
        }
        emoji.setUserId(userId);
        emoji.setCreateTime(DateUtil.currentTimeSeconds());
        collectionDao.addEmoji(emoji);
        userRedisService.deleteUserCollectCommon(userId);    // 维护用户收藏的缓存
        return emoji;
    }

    /**
     * 语音、文件特殊处理
     */
    protected Document emojiMsg(Emoji emoji) {
        VerifyUtil.isRollback(0==SKBeanUtils.getSystemConfig().getIsSaveMsg(),KConstants.ResultCode.NOSAVEMSGAND);
        return messageRepository.emojiMsg(emoji.getUserId(), emoji.getRoomJid(), emoji.getMsgId());
    }

    /**
     * 添加收藏表情
     */
    public Emoji newAddEmoji(Integer userId, Emoji emoji) {
        Emoji dbEmoji = collectionDao.queryEmojiByUrlAndType(emoji.getMsg(), emoji.getType(), userId);
        if (null != dbEmoji) {
            throw new ServiceException(KConstants.ResultCode.NotRepeatOperation);
        }
        String copyFile = ConstantUtil.copyFile(emoji.getUrl());
        emoji.setUserId(userId);
        emoji.setType(emoji.getType());
        emoji.setUrl(!StringUtil.isEmpty(copyFile) ? copyFile : emoji.getUrl());
        emoji.setCreateTime(DateUtil.currentTimeSeconds());
        collectionDao.addEmoji(emoji);
        // 维护用户自定义表情缓存
        userRedisService.deleteUserCollectEmoticon(userId);
        return emoji;
    }

    // 收藏详情记录
    public void saveCollect(ObjectId msgId, String nickname, int userId) {
        Collect collect = new Collect(msgId, nickname, userId);
        msgDao.addCollect(collect);
    }


    /**
     * 旧版添加收藏 兼容版本
     */
    @Override
    public List<Object> addCollection(int userId, String roomJid, String msgId, String type) {
        int isSaveMsg = SKBeanUtils.getSystemConfig().getIsSaveMsg();
        if (0 == isSaveMsg) {
            throw new ServiceException(KConstants.ResultCode.NOSAVEMSGAND);
        }
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
                        JSONObject jsonObject = JSONObject.parseObject(data.getString("body"));
                        if (3 == jsonObject.getIntValue("isEncrypt")) {
                            throw new ServiceException(KConstants.ResultCode.NOTSUPPORT_COLLECT);
                        }

                    }
                    if (Integer.parseInt(listType.get(i)) != Emoji.Type.TEXT) {
                        JSONObject obj = JSONObject.parseObject(data.toJson());
                        try {
                            String copyFile = ConstantUtil.copyFile(obj.get("content").toString());
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

    /**
     * 添加收藏表情
     */
    @Override
    public Object addEmoji(int userId, String url, String type) {
        Emoji getEmoji = null;
        if (!StringUtil.isEmpty(url)) {
            getEmoji = collectionDao.getEmoji(userId, url);
        }
        String copyFile = ConstantUtil.copyFile( url);
        if (getEmoji == null) {
            Emoji emoji = new Emoji();
            emoji.setUserId(userId);
            emoji.setType(Integer.parseInt(type));
            if (!StringUtil.isEmpty(copyFile)) {
                emoji.setUrl(copyFile);
            } else {
                emoji.setUrl(url);
            }
            emoji.setCreateTime(DateUtil.currentTimeSeconds());
            collectionDao.addEmoji(emoji);
            userRedisService.deleteUserCollectEmoticon(userId);  // 维护用户表情缓存
            return emoji;
        } else {
            return null;
        }
    }


    /**
     * 取消收藏
     */
    @Override
    public void deleteEmoji(Integer userId, String emojiId) {
        VerifyUtil.isRollback(StrUtil.isBlank(emojiId),KConstants.ResultCode.PARAMS_ERROR);
        List<String> list = Arrays.asList(emojiId.split(","));
        list.forEach(emjId -> {
            if (ObjectId.isValid(emjId)) {
                ObjectId emJidObj = new ObjectId(emjId);
                Emoji getEmoji = collectionDao.getEmoji(emJidObj, userId);
                if (null != getEmoji) {
                    if (StrUtil.isNotBlank(getEmoji.getCollectMsgId())){
                        if (getEmoji.getCollectType() == 1) {
                            msgDao.deleteCollect(new ObjectId(getEmoji.getCollectMsgId()));
                        }
                    }
                    if (getEmoji.getType() != Emoji.Type.FACE) {
                        userRedisService.deleteUserCollectCommon(userId);
                    } else {
                        userRedisService.deleteUserCollectEmoticon(userId);
                    }
                    collectionDao.deleteEmoji(emJidObj, userId);
                }
            }else {
                userRedisService.deleteUserCollectEmoticon(userId);
            }
        });
    }

    //收藏列表
    @Override
    public List<Emoji> emojiList(int userId, int type, int pageSize, int pageIndex) {
        // 用户收藏
        List<Emoji> emojiLists;
        if (type != Emoji.Type.OTHER) {
            emojiLists = collectionDao.queryEmojiList(userId, type,pageIndex,pageSize);
            // 兼容旧版文本
            emojiLists = unescapeHtml3(emojiLists);
        } else {
            List<Emoji> userCollectCommon = userRedisService.getUserCollectCommon(userId,pageIndex,pageSize);
            if (null != userCollectCommon && userCollectCommon.size() > 0) {
                emojiLists = new ArrayList<>(userCollectCommon);
            } else {
                emojiLists = collectionDao.queryEmojiListOrType(userId,pageIndex,pageSize);
                // 兼容旧版文本
                emojiLists = unescapeHtml3(emojiLists);
                userRedisService.saveUserCollectCommon(userId, emojiLists,pageIndex,pageSize);
            }
        }
        // 根据收藏类型设置来源名称
        return buildResult(emojiLists,buildTargetName(),buildUserName());
    }

    @Override
    public List<Emoji> searchCollection(Integer userId, String keyword, int pageSize, int pageIndex) {
        return buildResult(collectionDao.queryListByKey(userId, keyword, pageSize, pageIndex),buildUserName(),buildTargetName());
    }

    /**
     * 旧版收藏的文本数据格式化
     */
    public List<Emoji> unescapeHtml3(List<Emoji> emojiLists){
        if(null == emojiLists) {
            return null;
        }
        for (Emoji emojis : emojiLists) {
            if(5 == emojis.getType() && null == emojis.getCollectContent()){
                Document emojiMsg = emojiMsg(emojis);
                if (null != emojiMsg) {
                    // 格式化body 转译&quot;
                    JSONObject test = JSONObject.parseObject(emojiMsg.getString("body"));
                    if(3==test.getIntValue("isEncrypt")){
                        throw new ServiceException(KConstants.ResultCode.NOTSUPPORT_COLLECT);
                    }
                    if(null != test.get("content")){
                        emojis.setMsg(test.get("content" ).toString());
                        log.info("旧版转译后的 content:"+test.get("content" ).toString());
                        emojis.setCollectContent(test.get("content").toString());
                    }
                }
            }
        }

        return emojiLists;
    }


    //收藏表情列表
    @Override
    public List<Emoji> emojiList(int userId) {
        List<Emoji> emojis;
        List<Emoji> userCollectEmoticon = userRedisService.getUserCollectEmoticon(userId);
        if(null != userCollectEmoticon && userCollectEmoticon.size() >0) {
            emojis = userCollectEmoticon;
        } else{
            emojis = collectionDao.queryEmojiList(userId,Emoji.Type.FACE);
            userRedisService.saveUserCollectEmoticon(userId, emojis);
        }
        return emojis;
    }

    /**
     * 添加课程
     */
    @Override
    public void addMessageCourse(int userId, List<String> messageIds, long createTime, String courseName,String roomJid) {
        Course course = new Course();
        course.setUserId(userId);
        course.setMessageIds(messageIds);
        course.setCreateTime(createTime);
        course.setCourseName(courseName);
        course.setRoomJid(roomJid);
        courseDao.addCourse(course);
        List<Document> documents = messageRepository.queryMsgDocument(userId, roomJid, messageIds);
        for (Document dbObj : documents) {
            JSONObject jsonObject = JSONObject.parseObject(dbObj.getString("body"));
            // 处理录制设置过消息过期时间的消息
            Long deleteTime = jsonObject.getLong("deleteTime");
            if (deleteTime !=null && deleteTime > 0) {
                jsonObject.put("deleteTime", -1);
                dbObj.put("body", jsonObject.toString());
                dbObj.put("deleteTime", -1);
            }
            if (Emoji.Type.FILE == jsonObject.getIntValue("isEncrypt")) {
                throw new ServiceException(KConstants.ResultCode.NOTSUPPORT_COLLECT);
            }
            courseMessageDao.addCourseMessage(new ObjectId(), course.getUserId(), course.getCourseId().toString(), JSON.toJSONString(dbObj), String.valueOf(dbObj.get("messageId")), String.valueOf(dbObj.get("timeSend")));
        }
    }

    /**
     * 获取课程列表
     */
    @Override
    public List<Course> getCourseList(int userId) {
        return courseDao.getCourseListByUserId(userId);
    }

    /**
     * 修改课程
     */
    @Override
    public void updateCourse(Course course, String courseMessageId) {
        Course getCourse = courseDao.getCourseById(course.getCourseId());
        if(!StringUtil.isEmpty(courseMessageId)){
            CourseMessage courseMessage = courseMessageDao.queryCourseMessageById(courseMessageId);
            // 兼容IOS旧版本
            if(null == courseMessage){
                courseMessage = courseMessageDao.queryCourseMessageByIdOld(new ObjectId(courseMessageId));
            }
            courseMessageDao.deleteCourseMessage(courseMessage);
            // 维护讲课messageIds
            List<String> messageIds = getCourse.getMessageIds();
            messageIds.removeIf(next -> next.equals(courseMessageId));
            if(0 == messageIds.size()){
                courseDao.deleteCourse(course.getCourseId(),course.getUserId());
                return;
            }
            course.setMessageIds(messageIds);
        }
        courseDao.updateCourse(course.getCourseId(),course.getMessageIds(),course.getUpdateTime(),course.getCourseName());
    }

    /**
     * 删除课程
     */
    @Override
    public boolean deleteCourse(Integer userId,ObjectId courseId) {
        if(null == courseDao.getCourse(courseId,userId)) {
            return false;
        }
        List<CourseMessage> asList = courseMessageDao.getCourseMessageList(String.valueOf(courseId),userId);
        for (CourseMessage courseMessage : asList) {
            courseMessageDao.deleteCourseMessage(courseMessage);
        }
        courseDao.deleteCourse(courseId,userId);
        return true;
    }

    /**
     * 获取详情
     */
    @Override
    public List<CourseMessage> getCourse(String courseId) {
        return courseMessageDao.getCourseMessageList(courseId,null);
    }

    /**
     * 检查收藏是否重复
     */
    private void checkRepeat(Emoji emoji1,Emoji emojis){
        if (null != emoji1) {
            VerifyUtil.isRollback(null != emoji1.getMsgId() && emoji1.getMsgId().equals(emojis.getMsgId()),KConstants.ResultCode.NotRepeatOperation);
            if (emoji1.getType() != Emoji.Type.ONE_PHOTO && emoji1.getType()!=Emoji.Type.ONE_VIDEO){
                if (StrUtil.isNotBlank(emoji1.getMsg()) && StrUtil.isBlank(emoji1.getCollectMsgId())) {
                    VerifyUtil.isRollback(emoji1.getMsg().equals(emojis.getMsg()),KConstants.ResultCode.NotRepeatOperation);
                }
                // 判断动态是否重复收藏
                if (StrUtil.isNotBlank(emoji1.getMsg()) && StrUtil.isNotBlank(emoji1.getCollectMsgId())){
                    VerifyUtil.isRollback(emoji1.getMsg().equals(emojis.getMsg()) && emoji1.getCollectMsgId().equals(emojis.getCollectMsgId()),KConstants.ResultCode.NotRepeatOperation);
                }
            }
            VerifyUtil.isRollback(null != emoji1.getCollectMsgId() && emoji1.getCollectMsgId().equals(emojis.getCollectMsgId()),KConstants.ResultCode.NotRepeatOperation);
        }
    }

    /**
     * 构建收藏信息返回结果
     */
    @SafeVarargs
    public final List<Emoji> buildResult(List<Emoji> emojis, Consumer<? super Emoji>... actions){
        if (ObjectUtil.hasEmpty(emojis,actions)){
            return emojis;
        }
        emojis.forEach(emo->{
            for (Consumer<? super Emoji> action : actions) {
                action.accept(emo);
            }
        });
        return emojis;
    }

    /**
     * 设置收藏来源名称
     */
    public Consumer<? super Emoji> buildTargetName(){
        return (Consumer<Emoji>) emo -> {
            if (emo.getTargetType() != null && emo.getTargetType() == Emoji.TargetType.SINGLE && NumberUtil.isNumber(emo.getTargetId())) {
                emo.setTargetName(userCoreService.getNickName(Integer.parseInt(emo.getTargetId())));
            } else if (emo.getTargetType() != null && emo.getTargetType() == Emoji.TargetType.ROOM && StrUtil.isNotBlank(emo.getTargetId()) && ObjectId.isValid(emo.getTargetId())) {
                emo.setTargetName(roomManager.getRoomName(new ObjectId(emo.getTargetId())));
            }
        };
    }

    /**
     * 设置收藏发布者名称
     */
    public Consumer<? super Emoji> buildUserName(){
        return (Consumer<Emoji>) emo -> {
            if (!ObjectUtil.isEmpty(emo.getToUserId())){
                emo.setToUserName(userCoreService.getNickName(emo.getToUserId()));
            }
        };
    }


    private void addParam(Emoji emoji,Emoji msgEmoji){
        if (emoji!=null){
            emoji.setTargetId(msgEmoji.getTargetId());
            emoji.setTargetType(msgEmoji.getTargetType());
            emoji.setTargetName(msgEmoji.getTargetName());
            emoji.setToUserId(msgEmoji.getToUserId());
            emoji.setToUserName(msgEmoji.getToUserName());
        }
    }
}
