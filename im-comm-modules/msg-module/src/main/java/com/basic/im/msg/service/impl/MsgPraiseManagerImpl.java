package com.basic.im.msg.service.impl;

import com.basic.common.model.PageResult;
import com.basic.commons.thread.ThreadUtils;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.friends.service.FriendsManager;
import com.basic.im.message.MessageService;
import com.basic.im.message.MessageType;
import com.basic.im.msg.dao.MsgCommentDao;
import com.basic.im.msg.dao.MsgDao;
import com.basic.im.msg.dao.MsgPraiseDao;
import com.basic.im.msg.entity.Msg;
import com.basic.im.msg.entity.Praise;
import com.basic.im.msg.service.MsgPraiseManager;
import com.basic.im.msg.service.MsgRedisRepository;
import com.basic.im.support.Callback;
import com.basic.im.user.entity.User;
import com.basic.im.user.service.UserCoreService;
import com.basic.utils.DateUtil;
import com.basic.utils.StringUtil;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;

/**
 * @author zhm
 * @version V1.0
 * @Description: 朋友圈点赞相关业务
 * @date 2019/9/6 17:31
 */
@Service
public class MsgPraiseManagerImpl implements MsgPraiseManager {
    @Autowired
    private MsgPraiseDao msgPraiseDao;
    @Autowired
    private MsgDao msgDao;
    @Autowired
    private MsgCommentDao msgCommentDao;
   

    @Autowired
    private MessageService messageService;


    @Autowired
    private UserCoreService userCoreService;

    @Autowired
    private MsgRedisRepository msgRedisRepository;

    @Autowired
    private FriendsManager friendsManager;


    @Override
    public ObjectId add(int userId, ObjectId msgId) {
        User user = userCoreService.getUser(userId);

        if (!msgPraiseDao.exists(userId, msgId)) {
            Praise entity = new Praise(ObjectId.get(), msgId, user.getUserId(), user.getNickname(),
                    DateUtil.currentTimeSeconds());
           String msgIdStr = String.valueOf(msgId);
            // 更新缓存
            msgRedisRepository.deleteMsgPraise(msgIdStr);
            // 持久化赞
            msgPraiseDao.save(entity);
            // 更新消息：赞+1、活跃度+1
            msgDao.update(msgId, Msg.Op.Praise, 1);
            // 删除朋友圈缓存
            msgRedisRepository.deleteMsg(msgIdStr);
            ThreadUtils.executeInThread((Callback) obj -> push(userId, msgId, 0));

            return entity.getPraiseId();
        }

         throw new ServiceException(KConstants.ResultCode.NotRepeatOperation);
    }
    private void push(int userId,ObjectId msgId,int praiseType) {
        // xmpp推送
        User user = userCoreService.getUser(userId);
        Msg msg = msgDao.get(0, msgId);
        int type = msg.getBody().getType();
        String url = null;
        if (null != msg.getBody()) {
            if (type == 1) {
                url = msg.getBody().getText();
            } else if (type == 2) {
                url = msg.getBody().getImages().get(0).getTUrl();
            } else if (type == 3) {
                url = msg.getBody().getAudios().get(0).getOUrl();
            } else if (type == 4) {
                url = msg.getBody().getVideos().get(0).getOUrl();
            }else if (type==5){
                url = msg.getBody().getFiles().get(0).getOUrl();
            }
        }

        String t = String.valueOf(type);
        String u = String.valueOf(msgId);
        String mm = u + "," + t + "," + url;
        MessageBean messageBean = new MessageBean();
        messageBean.setType(0 == praiseType ? MessageType.PRAISE : MessageType.CANCELPRAISE);
        messageBean.setFromUserId(String.valueOf(userId));
        messageBean.setFromUserName(user.getNickname());
        messageBean.setContent("");
        messageBean.setObjectId(mm);
        messageBean.setMessageId(StringUtil.randomUUID());
        try {
            List<Integer> praiseuserIdlist;
            Query d = msgDao.createQuery("msgId", msgId);
            praiseuserIdlist = msgPraiseDao.getDatastore().findDistinct(d, "userId", "s_praise", Integer.class);
            List<Integer> userIdlist;
            userIdlist = msgCommentDao.getDatastore().findDistinct(d, "userId", "s_comment", Integer.class);

            userIdlist.addAll(praiseuserIdlist);

            userIdlist.add(msg.getUserId());

            HashSet<Integer> userIdSets = new HashSet<>(userIdlist);

            userIdSets.remove(userId);
            List<Integer> friendUserIdList = friendsManager.queryFriendUserIdList(userId);

            for (Integer toUserId : userIdSets) {
                if (friendUserIdList.contains(toUserId)) {
                    messageBean.setToUserId(toUserId.toString());
                    messageService.send(messageBean);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean delete(int userId, ObjectId msgId) {
        // 取消点赞
        Praise praise = msgPraiseDao.getPraise(userId,msgId);
        if(null!=praise){
            msgPraiseDao.deletePraise(userId,msgId);
            msgRedisRepository.deleteMsgPraise(msgId.toString());
            // 更新消息：赞-1、活跃度-1
            msgDao.update(msgId,Msg.Op.Praise,-1);
            ThreadUtils.executeInThread((Callback) obj -> push(userId, msgId, 1));
            return true;
        }else{
            throw new ServiceException(KConstants.ResultCode.DataNotExists);
        }


    }

    @Override
    public List<Praise> getPraiseList(Integer userId,ObjectId msgId, ObjectId praiseId, int pageIndex, int pageSize) {
        // 该用户的好友id列表
        List<Integer> userIdList = friendsManager.queryFriendUserIdList(userId);
        userIdList.add(userId);
        return msgPraiseDao.find(userIdList, msgId, praiseId, pageIndex, pageSize);
    }

    @Override
    public boolean exists(int userId, ObjectId msgId) {
        return msgPraiseDao.exists(userId,msgId);
    }

    @Override
    public boolean existsCollect(int userId, ObjectId msgId) {
        return msgPraiseDao.existsCollect(userId,msgId);
    }

    @Override
    public List<Praise> find(Integer userId, ObjectId msgId, ObjectId praiseId, int pageIndex, int pageSize) {
        // 该用户的好友id列表
        List<Integer> userIdList = friendsManager.queryFriendUserIdList(userId);
        userIdList.add(userId);
        return msgPraiseDao.find(userIdList, msgId, praiseId, pageIndex, pageSize);
    }



    @Override
    public PageResult<Praise> praiseListMsg(ObjectId msgId, Integer page, Integer limit) {
        return msgPraiseDao.praiseListMsg(msgId,page,limit);
    }
}
