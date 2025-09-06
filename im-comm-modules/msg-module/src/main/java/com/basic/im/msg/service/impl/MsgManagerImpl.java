package com.basic.im.msg.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.basic.common.model.PageResult;
import com.basic.commons.thread.ThreadUtils;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.VerifyUtil;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.entity.Config;
import com.basic.im.friends.service.impl.FriendsManagerImpl;
import com.basic.im.message.MessageService;
import com.basic.im.message.MessageType;
import com.basic.im.msg.dao.MsgCommentDao;
import com.basic.im.msg.dao.MsgDao;
import com.basic.im.msg.dao.MsgPraiseDao;
import com.basic.im.msg.entity.Comment;
import com.basic.im.msg.entity.Msg;
import com.basic.im.msg.entity.Praise;
import com.basic.im.msg.model.AddMsgParam;
import com.basic.im.msg.model.MessageExample;
import com.basic.im.msg.model.PublicMsgQueryModel;
import com.basic.im.msg.service.MsgManager;
import com.basic.im.msg.service.MsgRedisRepository;
import com.basic.im.support.Callback;
import com.basic.im.user.entity.User;
import com.basic.im.user.service.UserCoreService;
import com.basic.im.utils.SKBeanUtils;
import com.basic.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * @author zhm
 * @version V1.0
 * @Description: 朋友圈相关业务
 * @date 2019/9/6 17:30
 */
@Slf4j
@Service
public class MsgManagerImpl implements MsgManager {
    @Autowired
    @Lazy
    private MsgDao msgDao;
    public MsgDao getMsgDao(){
        return msgDao;
    }
    @Autowired
    @Lazy
    private MsgCommentDao msgCommentDao;
    @Autowired
    @Lazy
    private MsgPraiseDao msgPraiseDao;

   /* @Autowired
    private GiveGiftDao giveGiftDao;*/



    @Autowired
    private MusicManagerImpl musicManager;

    @Autowired
    private MessageService messageService;

    @Autowired
    private MsgRedisRepository msgRedisRepository;



    @Autowired
    private UserCoreService userCoreService;


    @Autowired
    private FriendsManagerImpl friendsManager;

    public Msg add(int userId, AddMsgParam param) {
        //去redis根据userId是否有数据
        User user =userCoreService.getUser(userId);
        //设置一些列参数
        Msg entity = Msg.build(user, param);
        //设置状态
        entity.setState(getInitMsgStatus(param));
        // 保存生活圈消息
        msgDao.add(entity);
        // 如果musicId不为空维护音乐使用次数
        if(!StringUtil.isEmpty(param.getMusicId())){
            musicManager.updateUseCount(new ObjectId(param.getMusicId()));
        }
        List<Integer> friendUserIdList = friendsManager.queryFriendUserIdList(userId);
        if(null != param.getUserRemindLook()){
            if(null != param.getUserNotLook()){
                List<Integer> collect = param.getUserRemindLook().stream().filter(item -> param.getUserNotLook().contains(item)).collect(toList());
                //log.info("朋友圈提醒朋友列表:{}, 不给看列表:{}, 交集列表：{}",param.getUserRemindLook(),param.getUserNotLook(), JSONObject.toJSONString(collect));
                param.getUserRemindLook().removeAll(collect);
                if(null == param.getUserRemindLook()){
                    return entity;
                }
            }
            ThreadUtils.executeInThread((Callback) obj -> {
                for(int i=0;i<param.getUserRemindLook().size();i++){
                    push(userId,param.getUserRemindLook().get(i),entity.getMsgId());
                }
            });
        }
        // 新消息 通知给自己的好友
        if (2 != param.getVisible() && null != friendUserIdList &&!friendUserIdList.isEmpty()) {
            // 移除不给看的好友，朋友圈属性设置
            if(null!=entity.getUserNotLook()&&!entity.getUserNotLook().isEmpty()){
                friendUserIdList.removeAll(entity.getUserNotLook());
            }
            List<Integer> filterCircleUserIds = userCoreService.queryFilterCircleUserIds(userId);
            friendUserIdList.removeAll(filterCircleUserIds);

            // 移除不给看的好友，用户全局设置
            if (null != user.getSettings() && null != user.getSettings().getNotSeeFilterCircleUserIds()){
                friendUserIdList.removeAll(user.getSettings().getNotSeeFilterCircleUserIds());
            }
            if(null!=entity.getUserLook() && !entity.getUserLook().isEmpty()){
                friendUserIdList = entity.getUserLook();
            }
            newMsgPush(user, friendUserIdList, entity.getMsgId().toString());
        }
        return entity;
    }

    /**
     * 朋友圈，短视频初始状态
     * 0：正常  1：锁定  2：未审核 -1：审核失败
     */
    private int getInitMsgStatus(AddMsgParam param){
        Config systemConfig = SKBeanUtils.getSystemConfig();
        if (systemConfig == null){
            return 0;
        }
        if (StringUtil.isEmpty(param.getLable())){
            //发布朋友圈 是否需要审核
            return systemConfig.getPublishMsgWhetherCheck() == KConstants.ZERO ? KConstants.ZERO : KConstants.TWO;
        }else{
            //发布视界 是否需要审核
            return systemConfig.getPublishMsgLableWhetherCheck() == KConstants.ZERO ? KConstants.ZERO : KConstants.TWO;
        }
    }

    private void newMsgPush(User user,List<Integer> userIdList,String msgId){

        MessageBean messageBean=new MessageBean();
        messageBean.setType(MessageType.MSG_NEW);
        messageBean.setFromUserId(String.valueOf(user.getUserId()));
        messageBean.setFromUserName(user.getNickname());
        messageBean.setObjectId(msgId);
        // 单聊消息
        messageBean.setMsgType(0);
        messageBean.setMessageId(StringUtil.randomUUID());

        messageService.send(messageBean,userIdList);
    }
    private void push(int userId,int toUserId,ObjectId msgId){
        // xmpp推送
        User user =userCoreService.getUser(userId);
        Msg msg = msgDao.get(msgId);
        int type=msg.getBody().getType();
        String url=null;
        if(type==1){
            url=msg.getBody().getText();
        }else if(type==2){
            url=msg.getBody().getImages().get(0).getTUrl();
        }else if(type==3){
            url=msg.getBody().getAudios().get(0).getOUrl();
        }else if(type==4){
            url=msg.getBody().getImages().get(0).getOUrl();
        }
        String t=String.valueOf(type);
        String u=String.valueOf(msgId);
        String mm=u+","+t+","+url;
        MessageBean messageBean=new MessageBean();
        messageBean.setType(MessageType.REMIND);
        messageBean.setFromUserId(String.valueOf(userId));
        messageBean.setFromUserName(user.getNickname());
        messageBean.setContent("");
        messageBean.setObjectId(mm);
        messageBean.setToUserId(String.valueOf(toUserId));
        messageBean.setMsgType(0);// 单聊消息
        messageBean.setMessageId(StringUtil.randomUUID());
        messageService.send(messageBean);
    }

    private List<Msg> fetchAndAttach(int userId, List<Msg> msgList) {
        // 该用户的好友id列表
        List<Integer> userIdList = friendsManager.queryFriendUserIdList(userId);
        userIdList.add(userId);
        if (null != msgList && 0 < msgList.size()) {
            msgList.forEach(msg -> {
                if (null != msg.getBody()) {
                    if (msg.getBody().getType() == 5) {
                        if (null != msg.getBody().getFiles() && null != msg.getBody().getFiles().get(0)) {
                            msg.setFileName(msg.getBody().getFiles().get(0).getoFileName());
                        }
                    }
                }
                msg.setComments(getComments(userIdList, msg.getMsgId().toString()));
                msg.setPraises(getPraises(userIdList, msg.getMsgId().toString()));
                // msg.setGifts(giveGiftDao.find(msg.getMsgId(), null, 0, 10));
                msg.setIsPraise(msgPraiseDao.exists(ReqUtil.getUserId(), msg.getMsgId()) ? 1 : 0);
                msg.setIsCollect(msgDao.existsCollect(ReqUtil.getUserId(), msg.getMsgId()) ? 1 : 0);
                msg.setHiding(userCoreService.getSettings(msg.getUserId()).getHiding());
            });
        }

        return msgList;
    }

    /**
     * 获取最新二十条评论
     */
    private List<Comment> getComments(List<Integer> userIds, String msgId) {
        /*List<Comment> msgComment = msgRedisRepository.getMsgComment(msgId);
        if (null != msgComment && msgComment.size() > 0) {
            return msgComment;
        } else {
            List<Comment> commonListMsg = msgCommentDao.find(new ObjectId(msgId), null, 0, 20);
            if (null != commonListMsg && commonListMsg.size() > 0) {
                msgRedisRepository.saveMsgComment(msgId, commonListMsg);
            }
            return commonListMsg;
        }*/
        return msgCommentDao.find(userIds,new ObjectId(msgId), null, 0, 20);
    }

    private List<Praise> getPraises(List<Integer> userIds, String msgId){
        /*List<Praise> msgPraise = msgRedisRepository.getMsgPraise(msgId);
        if(null != msgPraise && msgPraise.size() > 0){
            return msgPraise;
        }else {
            List<Praise> praiseListMsg = msgPraiseDao.find(new ObjectId(msgId), null, 0, 20);
            if(null != praiseListMsg && praiseListMsg.size() > 0) {
                msgRedisRepository.saveMsgPraise(msgId, praiseListMsg);
            }
            return praiseListMsg;
        }*/
        return msgPraiseDao.find(userIds, new ObjectId(msgId), null, 0, 20);
    }

    @Override
    public Msg get(int userId, ObjectId msgId) {
        // 该用户的好友id列表
        List<Integer> userIdList = friendsManager.queryFriendUserIdList(userId);
        userIdList.add(userId);
        Msg msg = msgRedisRepository.getMsg(String.valueOf(msgId));
        if(null == msg){
            msg = msgDao.get(msgId);
            if (null == msg) {
                return msg;
            } else if (0 == userId) {
                return msg;
            }
            msgRedisRepository.saveMsg(msg);
        }
        msg.setComments(msgCommentDao.find(userIdList, msg.getMsgId(), null, 0, 20));
        msg.setPraises(msgPraiseDao.find(userIdList, msg.getMsgId(), null, 0, 20));
        // msg.setGifts(giveGiftDao.find(msg.getMsgId(), null, 0, 20));
        msg.setIsPraise(msgPraiseDao.exists(userId, msg.getMsgId()) ? 1 : 0);
        msg.setIsCollect(msgDao.existsCollect(ReqUtil.getUserId(), msg.getMsgId()) ? 1 : 0);
        msg.setHiding(userCoreService.getSettings(msg.getUserId()).getHiding());
        return msg;
    }

    public Msg getMsgInfo(ObjectId msgId) {
        Msg msg = msgRedisRepository.getMsg(String.valueOf(msgId));
        if(null == msg) {
            msg = msgDao.get(msgId);
        }
        return msg;
    }

    public PageResult<Msg> getMsgList(Integer page, Integer limit, String nickname, Integer userId,String type) {
        PageResult<Msg> result=new PageResult<>();
        try {
            result = msgDao.getMsgListResult(userId,nickname,page,limit,type);
            for(Msg msg : result.getData()){
                User user = userCoreService.getUser(msg.getUserId());
                if(null == user){
                    // 过滤废弃的测试账号朋友圈数据
                    ThreadUtils.executeInThread((Callback) obj -> msgDao.deleteMsg(msg.getUserId()));

                }else{
                    msg.setUserStatus(user.getStatus());
                }
            }
//            result.setCount(query.count());
//            result.setData(msgList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public void deleteMsg(String userId) {
        msgDao.delete(userId);
    }

    @Override
    public List<Msg> findByExample(int userId, MessageExample example) {
        return fetchAndAttach(userId,msgDao.findByExample(userId,example));
    }

    @Override
    public List<Msg> getUserMsgList(Integer userId, Integer toUserId, ObjectId msgId,int pageIndex, Integer pageSize) {
        return fetchAndAttach(userId,msgDao.getUserMsgList(userId,toUserId,msgId,pageIndex,pageSize));
    }

    @Override
    public List<Msg> getUserMsgIdList(int userId, int toUserId, ObjectId msgId, int pageSize) {
        return msgDao.getUserMsgIdList(userId,toUserId,msgId,pageSize);
    }

    @Override
    public List<Msg> getSquareMsgList(int userId, ObjectId msgId, Integer pageSize) {
        return msgDao.getSquareMsgList(userId,msgId,pageSize);
    }

    @Override
    public List<Msg> getMsgListByIds(int userId, String ids) {
        return fetchAndAttach(userId,msgDao.gets(userId,ids));
    }

    @Override
    public List<Msg> getMsgIdList(int userId, int toUserId, ObjectId msgId, int pageSize) {
        return msgDao.getMsgIdList(userId,toUserId,msgId,pageSize);
    }

    @Override
    public List<Msg> getMsgList(Integer userId, ObjectId msgId, Integer pageSize, Integer pageIndex) {
        return fetchAndAttach(userId, msgDao.getMsgList(userId,msgId,pageIndex,pageSize));
    }


    @Override
    public List<Msg> getPureVideo(int userId,Integer pageIndex, Integer pageSize, String lable) {
        List<Msg> pureVideo = msgDao.getPureVideo(userId, pageIndex, pageSize, lable);
        VerifyUtil.execute(pureVideo!=null,()-> {
            assert pureVideo != null;
            pureVideo.forEach(obj-> obj.setHiding(userCoreService.getSettings(obj.getUserId()).getHiding()));
        });
        return pureVideo;
    }

    @Override
    public List<Msg> queryPublicMsg(PublicMsgQueryModel queryModel){
        queryModel.setDistance(SKBeanUtils.getSystemConfig().getDistance());


        List<Msg> msgList= msgDao.queryPublicMsg(queryModel);
        return msgList;
    }

    @Override
    public boolean forwarding(Integer userId, AddMsgParam param) {
        return msgDao.forwarding(userId,param);
    }

    @Override
    public void lockingMsg(ObjectId msgId, int state) {
        msgDao.lockingMsg(msgId,state);
    }

    @Override
    public void delete(String[] msgIds) {
        msgDao.delete(msgIds);
        msgRedisRepository.deleteMsg(msgIds); // 删除缓存
    }

    @Override
    public boolean delete(ObjectId messageId) {
        this.delete(new String[]{messageId.toString()});
        return true;
    }
}
