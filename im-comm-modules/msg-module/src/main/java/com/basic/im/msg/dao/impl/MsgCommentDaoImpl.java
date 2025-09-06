package com.basic.im.msg.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.msg.dao.MsgCommentDao;
import com.basic.im.msg.model.AddCommentParam;
import com.basic.im.msg.service.MsgRedisRepository;
import com.basic.im.repository.MongoRepository;
import com.basic.utils.DateUtil;
import com.basic.im.msg.entity.Comment;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/**
 * @author zhm
 * @version V1.0
 * @date 2019/9/6 16:29
 */
@Repository
public class MsgCommentDaoImpl extends MongoRepository<Comment, ObjectId> implements MsgCommentDao {

    @Autowired
    private MsgRedisRepository msgRedisRepository;
    @Override
    public Class<Comment> getEntityClass() {
        return Comment.class;
    }
    private final String s_comment="s_comment";// 评论表名称

    @Override
    public ObjectId add(int userId,String nickName, AddCommentParam param) {
        ObjectId commentId = ObjectId.get();
        Comment entity = new Comment(commentId, new ObjectId(
                param.getMessageId()), userId, nickName,
                param.getBody(), param.getToUserId(), param.getToNickname(),
                param.getToBody(), DateUtil.currentTimeSeconds());
         saveEntity(entity);
         return commentId;
    }

    @Override
    public boolean delete(ObjectId msgId, String commentId) {
        String[] commentIds = StringUtil.getStringList(commentId);
        for (String commId : commentIds) {
            if(!ObjectId.isValid(commId)) {
                continue;
            }
            deleteById(new ObjectId(commId));
        }
        // 清除缓存
        msgRedisRepository.deleteMsgComment(msgId.toString());
        return true;
    }

    /**
     * 获取评论列表，不能看见非朋友的评论
     * @param
     * @return
     */
    public List<Comment> find(List<Integer> userIds, ObjectId msgId, ObjectId commentId, int pageIndex, int pageSize) {
        Query query = null;
        if (null != commentId) {
            query = createQuery("commentId", commentId);
        } else {
            query = createQuery("msgId", msgId);
        }
        if(null!=userIds&&!userIds.isEmpty()) {
            Criteria userIdCriteria = Criteria.where("userId").in(userIds);
            query.addCriteria(userIdCriteria);
        }
        descByquery(query, "time");
        return queryListsByQuery(query, pageIndex, pageSize);
    }

    @Override
    public List<ObjectId> getCommentIds(Integer userId){
        Query query=createQuery("userId",userId);

        return  getDatastore().findDistinct(query,"msgId",getEntityClass(),ObjectId.class);
    }

    @Override
    public void update(int userId, Map<String, Object> map) {
        Query query=createQuery("userId",userId);
        Update ops = createUpdate();
        map.forEach((key,value)->{
            ops.set(key,value);
        });
        update(query,ops);
    }

    @Override
    public Comment getComment(ObjectId id) {
        return get(id);
    }

    @Override
    public void deleteComment(ObjectId id) {
        deleteById(id);
    }

    @Override
    public PageResult<Comment> commonListMsg(ObjectId msgId, Integer page, Integer limit) {
        PageResult<Comment> result = new PageResult<Comment>();
        Query query=createQuery("msgId", msgId);
        ascByquery(query,"time");
        result.setCount(count(query));
        result.setData(queryListsByQuery(query,page, limit, 1));
        return result;
    }
}
