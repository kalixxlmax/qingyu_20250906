package com.basic.im.msg.service.impl;

import com.basic.common.model.PageResult;
import com.basic.commons.thread.ThreadUtils;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.friends.service.FriendsManager;
import com.basic.im.friends.service.impl.FriendsManagerImpl;
import com.basic.im.message.MessageService;
import com.basic.im.message.MessageType;
import com.basic.im.msg.dao.MsgCommentDao;
import com.basic.im.msg.dao.MsgDao;
import com.basic.im.msg.dao.MsgPraiseDao;
import com.basic.im.msg.entity.Comment;
import com.basic.im.msg.entity.Msg;
import com.basic.im.msg.model.AddCommentParam;
import com.basic.im.msg.service.MsgCommentManager;
import com.basic.im.msg.service.MsgRedisRepository;
import com.basic.im.support.Callback;
import com.basic.im.user.entity.User;
import com.basic.im.user.service.UserCoreService;
import com.basic.utils.DateUtil;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * @author zhm
 * @version V1.0
 * @Description: TODO(朋友圈评论相关业务)
 * @date 2019/9/6 17:31
 */
@Service
public class MsgCommentManagerImpl implements MsgCommentManager {
    @Autowired
    private MsgCommentDao msgCommentDao;
    @Autowired
    private MsgDao msgDao;
    @Autowired
    private MsgPraiseDao msgPraiseDao;

    @Autowired
    private MsgRedisRepository msgRedisRepository;

    @Autowired
    private MessageService messageService;

    @Autowired
    private UserCoreService userCoreService;

    @Autowired
    private FriendsManager friendsManager;

    @Override
    public ObjectId add(int userId, AddCommentParam param) {
        User user = userCoreService.getUser(userId);
        ObjectId commentId = ObjectId.get();

        Comment entity = new Comment(commentId,
                new ObjectId(param.getMessageId()),
                user.getUserId(),
                user.getNickname(),
                param.getBody(),
                param.getToUserId(),
                param.getToNickname(),
                param.getToBody(),
                DateUtil.currentTimeSeconds());

       msgRedisRepository.deleteMsgComment(param.getMessageId());

        // 保存评论
        msgCommentDao.save(entity);
        // 更新消息：评论数+1、活跃度+1
        msgDao.update(new ObjectId(param.getMessageId()), Msg.Op.Comment, 1);
        // 删除朋友圈缓存
        msgRedisRepository.deleteMsg(param.getMessageId());
        //新线程进行xmpp推送
        ThreadUtils.executeInThread((Callback) obj -> tack(userId,param));

        return entity.getCommentId();
    }

    private void tack(int userId, AddCommentParam param){
        User user = userCoreService.getUser(userId);
        // xmpp推送
//        Query<Msg> q=getDatastore().createQuery(getEntityClass());
//        Msg msg=q.filter("msgId", new ObjectId(param.getMessageId())).get();
        Msg msg = msgDao.get(0,new ObjectId(param.getMessageId()));
        int type=msg.getBody().getType();

        String url=null;
        MessageBean messageBean=new MessageBean();
        if(type==1){
            url=msg.getBody().getText();
        }else if(type==2){
            url=msg.getBody().getImages().get(0).getTUrl();
        }else if(type==3){
            url=msg.getBody().getAudios().get(0).getOUrl();
        }else if(type==4){
            url=msg.getBody().getVideos().get(0).getOUrl();
        }else if (type==5){
            url=msg.getBody().getFiles().get(0).getOUrl();
        }
        String u=String.valueOf(type);
        String us=param.getMessageId()+","+u+","+url;
        messageBean.setType(MessageType.COMMENT);//类型为42
        messageBean.setFromUserId(String.valueOf(userId));//评论者的Id
        messageBean.setFromUserName(user.getNickname());//评论者的昵称

        messageBean.setObjectId(us);//id,type,url
        messageBean.setContent(param.getBody());//评论内容
        messageBean.setMessageId(StringUtil.randomUUID());

        try {
            List<Integer> praiseuserIdlist;
            Query d= msgDao.createQuery("msgId",new ObjectId(param.getMessageId()));
            praiseuserIdlist = msgPraiseDao.getDatastore().findDistinct(d,"userId","s_praise", Integer.class);
            List<Integer> userIdlist;
            userIdlist =  msgCommentDao.getDatastore().findDistinct(d,"userId","s_comment", Integer.class);

            userIdlist.addAll(praiseuserIdlist);

            userIdlist.add(msg.getUserId());
            HashSet<Integer> userIdSets=new HashSet<>(userIdlist);

            userIdSets.remove(userId);

            List<Integer> friendUserIdList = friendsManager.queryFriendUserIdList(userId);

            for(Integer toUserId : userIdSets) {
                if(friendUserIdList.contains(toUserId)) {
                    //被回复者的ID
                    messageBean.setToUserId(toUserId.toString());
                    if (!StringUtil.isEmpty(param.getToNickname())) {
                        messageBean.setToUserName(param.getToNickname());//被回复者的昵称
                    }
                    messageService.send(messageBean);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean delete(ObjectId msgId, String commentId) {
        String[] commentIds = StringUtil.getStringList(commentId);
        for (String commId : commentIds) {
            if(!ObjectId.isValid(commId)) {
                continue;
            }
            // 删除评论
//            Query<Comment> query = getDatastore().createQuery(Comment.class).field(MongoOperator.ID).equal(new ObjectId(commId));
            Comment comment = msgCommentDao.getComment(new ObjectId(commId));
            if(null != comment){
//                getDatastore().findAndDelete(query);
                msgCommentDao.deleteComment(new ObjectId(commId));
                // 更新消息：评论数-1、活跃度-1
               msgDao.update(msgId, Msg.Op.Comment, -1);
            }else{
                throw new ServiceException(KConstants.ResultCode.DataNotExists);
            }
        }
        // 清除缓存
       msgRedisRepository.deleteMsgComment(msgId.toString());
        return true;
    }

    @Override
    public List<Comment> find(Integer userId, ObjectId msgId, ObjectId commentId, int pageIndex, int pageSize) {
        // 该用户的好友id列表
        List<Integer> userIdList = friendsManager.queryFriendUserIdList(userId);
        userIdList.add(userId);// 添加自己的ID
        return msgCommentDao.find(userIdList,msgId,commentId,pageIndex,pageSize);
    }

    @Override
    public PageResult<Comment> commonListMsg(ObjectId msgId, Integer page, Integer limit) {
        return msgCommentDao.commonListMsg(msgId,page,limit);
    }
}
