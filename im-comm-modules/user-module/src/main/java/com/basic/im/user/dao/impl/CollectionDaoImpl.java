package com.basic.im.user.dao.impl;

import com.basic.commons.thread.ThreadUtils;
import com.basic.im.comm.utils.HttpUtil;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.msg.entity.Collect;
import com.basic.im.repository.MongoRepository;
import com.basic.im.support.Callback;
import com.basic.im.user.dao.CollectionDao;
import com.basic.im.user.entity.Emoji;
import com.basic.im.user.service.UserRedisService;
import com.basic.im.utils.ConstantUtil;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class CollectionDaoImpl extends MongoRepository<Emoji,Integer> implements CollectionDao {

    @Override
    public Class<Emoji> getEntityClass() {
        return Emoji.class;
    }


    @Autowired
    private UserRedisService userRedisService;


    @Override
    public void addEmoji(Emoji emoji) {
        if (emoji.getType() == Emoji.Type.PHOTO && !StringUtil.isEmpty(emoji.getCollectMsgId())){
            Query query =createQuery("userId", ReqUtil.getUserId());
            addToQuery(query,"collectMsgId",emoji.getCollectMsgId());
            Emoji emoji1 = findOne(query);
            if (emoji1 != null){
                return;
            }
        }
        getDatastore().save(emoji);
    }

    @Override
    public void deleteEmoji(ObjectId emojiId, Integer userId) {
        Query query =createQuery("_id",emojiId);
        addToQuery(query,"userId",userId);
        deleteByQuery(query);
    }

    @Override
    public void deleteEmoji(String collectMsgId, int userId) {
        Query query = createQuery("collectMsgId",collectMsgId);
        addToQuery(query,"userId",userId);
        deleteByQuery(query);
    }

    @Override
    public Emoji getEmoji(ObjectId emojiId, Integer userId) {
        Query query = createQuery("_id",emojiId);
        addToQuery(query,"userId",userId);
        return findOne(query);
    }

    @Override
    public Emoji getEmoji(String msgId, Integer userId) {
        Query query = createQuery("msgId",msgId);
        addToQuery(query,"userId",userId);
        return findOne(query);
    }

    @Override
    public Emoji getEmoji(Integer userId, String url) {
        Query query = createQuery("userId",userId);
        addToQuery(query,"url",url);
        return findOne(query);
    }

    @Override
    public Emoji getEmoji(String msg, int type, Integer userId, String msgId) {
        Query query =createQuery("userId",userId);
        addTypeQuery(type,query);
        addToQuery(query,"msg",msg);
        if(!StringUtil.isEmpty(msgId)){
            addToQuery(query,"msgId",msgId);
        }
        return findOne(query);
    }

    @Override
    public Emoji getEmoji(String collectMsgId, int userId) {
        Query query =createQuery("userId",userId);
        addToQuery(query,"collectMsgId",collectMsgId);
        return findOne(query);

    }

    @Override
    public List<Emoji> queryEmojiList(Integer userId, int type,Integer pageIndex,Integer pageSize) {
        Query query =createQuery("userId",userId);
        if(Emoji.Type.OTHER != type){
            addTypeQuery(type,query);
        }else{
            query.addCriteria(Criteria.where("type").nin(Emoji.Type.FACE));
        }
        query.with(createPageRequest(pageIndex,pageSize, Sort.by(Sort.Order.desc("createTime"))));
        return queryListsByQuery(query);
    }

    @Override
    public List<Emoji> queryEmojiList(Integer userId, int type) {
        Query query =createQuery("userId",userId);
        if(Emoji.Type.OTHER != type){
            addTypeQuery(type,query);
        }else{
            query.addCriteria(Criteria.where("type").nin(Emoji.Type.FACE));
        }
        query.with(Sort.by(Sort.Order.desc("createTime")));
        return queryListsByQuery(query);
    }

    @Override
    public List<Emoji> queryListByKey(Integer userId, String keyword, int pageSize, int pageIndex) {
        Query query = createQuery("userId",userId);
        query.addCriteria(Criteria.where("msg").regex(keyword));
        // 只模糊查询文本内容
        query.addCriteria(Criteria.where("type").is(Emoji.Type.TEXT));
        query.with(PageRequest.of(pageIndex,pageSize));
        query.with(Sort.by(Sort.Order.desc("createTime")));
        return queryListsByQuery(query);
    }

    @Override
    public List<Emoji> queryEmojiListOrType(Integer userId,Integer pageIndex,Integer pageSize) {
        Query query =createQuery("userId",userId);
        Criteria criteria = createCriteria().orOperator(Criteria.where("type").lt(Emoji.Type.MAX_SEQ), Criteria.where("type").is(Emoji.Type.MAX_SEQ));
        query.addCriteria(criteria);
        query.addCriteria(Criteria.where("type").ne(Emoji.Type.FACE));
        query.with(createPageRequest(pageIndex,pageSize, Sort.by(Sort.Order.desc("createTime"))));
        return queryListsByQuery(query);
    }

    @Override
    public Emoji queryEmojiByUrlAndType(String url,int type,Integer userId) {
        Query query =createQuery("userId",userId);
        addTypeQuery(type,query);
        addToQuery(query,"url",url);
       return findOne(query);
    }

    public void deleteCollectInfo(int userId, String msgId) {
        Query query =createQuery("msgId",new ObjectId(msgId));
        addToQuery(query,"userId",userId);
        getDatastore().remove(query,Collect.class);
    }

    public void deleteCollect(int userId, String msgId) {
        // 删除收藏
        Emoji emoji = getEmoji(msgId,userId);
        deleteCollectInfo(userId,msgId);
        deleteEmoji(msgId,userId);
        userRedisService.deleteUserCollectCommon(userId);
        userRedisService.deleteUserCollectEmoticon(userId);
        ThreadUtils.executeInThread((Callback) obj -> {
            if(null != emoji) {
                ConstantUtil.deleteFile(emoji.getUrl());
            }
        });
    }

    /**
     * 根据 type 增加查询条件
     */
    public void addTypeQuery(Integer type,Query query){
        if (type == Emoji.Type.PHOTO){
            query.addCriteria(Criteria.where("type").in(Emoji.Type.ONE_PHOTO,Emoji.Type.PHOTO));
        }else if (type == Emoji.Type.VIDEO){
            query.addCriteria(Criteria.where("type").in(Emoji.Type.ONE_VIDEO,Emoji.Type.VIDEO));
        }else{
            addToQuery(query,"type",type);
        }
    }
}
