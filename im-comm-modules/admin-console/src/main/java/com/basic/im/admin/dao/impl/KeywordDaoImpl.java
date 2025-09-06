package com.basic.im.admin.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.admin.dao.KeywordDAO;
import com.basic.im.admin.entity.KeyWord;
import com.basic.im.admin.entity.KeywordDenyRecord;
import com.basic.im.repository.MongoRepository;
import com.basic.utils.DateUtil;
import com.basic.utils.StringUtil;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class KeywordDaoImpl extends MongoRepository<KeywordDenyRecord, ObjectId> implements KeywordDAO {

    @Override
    public Class<KeywordDenyRecord> getEntityClass() {
        return KeywordDenyRecord.class;
    }

    // 新增关键词
    @Override
    public void saveKeyword(KeyWord keyWord){
        getDatastore().save(keyWord);
    }

    // 更新关键词
    @Override
    public void updateKeyword(String word, ObjectId id){
        Query query =createQuery(id);
        Update ops = createUpdate();
        ops.set("word", word);
        ops.set("createTime", DateUtil.currentTimeSeconds());
        update(query, ops);
    }

    // 删除关键词
    @Override
    public void deleteKeyword(ObjectId id){
        Query query = createQuery(id);
        getDatastore().remove(query,KeyWord.class);
        //deleteById(id);
    }

    // 查询关键词列表
    @Override
    public List<KeyWord> queryKeywordList(String word, int pageIndex, int pageSize){
        Query query = createQuery();
        if (!StringUtil.isEmpty(word)) {
            addToQuery(query,"word", word);
        }
        descByquery(query,"createTime");

        return getDatastore().find(query,KeyWord.class);
    }

    // 查询拦截消息列表
    @Override
    public List<KeywordDenyRecord> queryMsgInerceptList(Integer userId, String toUserId, int pageIndex, int pageSize, int type, String content){
        Query query = createQuery();
        if(!StringUtil.isEmpty(content)){
            addToQuery(query,"content", content);
        }
        if(null != userId){
            addToQuery(query,"fromUserId", userId);
        }
        if(type==0){
            if(!StringUtil.isEmpty(toUserId)){
                addToQuery(query,"toUserId", Integer.valueOf(toUserId));
            }
            addToQuery(query,"roomJid",null);
        }else if(type==1){
            if(!StringUtil.isEmpty(toUserId)){
                addToQuery(query,"roomJid", toUserId);
            }
            query.addCriteria(Criteria.where("roomJid").ne(null));
        }
        return getDatastore().find(query,KeywordDenyRecord.class);
    }

    // 删除拦截消息
    @Override
    public void deleteMsgIntercept(ObjectId id){
        Query query = createQuery(id);
        getDatastore().remove(query,KeywordDenyRecord.class);
    }

    @Override
    public PageResult<KeyWord> queryKeywordPageResult(String word, int page, int limit) {
        PageResult<KeyWord> result = new PageResult<KeyWord>();
        Query query = createQuery();
        if (!StringUtil.isEmpty(word)) {
            query.addCriteria(Criteria.where("word").regex(word));
        }
        descByquery(query,"createTime");
        result.setCount(getDatastore().count(query,KeyWord.class));
        query.with(createPageRequest(page, limit, 1));
        result.setData(getDatastore().find(query,KeyWord.class));
        return result;
    }

    @Override
    public PageResult<KeywordDenyRecord> webQueryMsgInterceptList(Integer userId, String toUserId, int pageIndex, int pageSize, int type, String content) {
        PageResult<KeywordDenyRecord> data = new PageResult<>();
        Query query = createQuery();

        //时间降序排序
        query.with(Sort.by(Sort.Order.desc("createTime")));

        if(!StringUtil.isEmpty(content)){
//            addToQuery(query,"content", content);
            query.addCriteria(
                    new Criteria().orOperator(Criteria.where("msgContent").regex(content)));
        }
        if(null != userId){
            addToQuery(query,"fromUserId", userId);
        }
        if(type==0){
            if(!StringUtil.isEmpty(toUserId)){
                addToQuery(query,"toUserId", Integer.parseInt(toUserId));
            }
            //addToQuery(query,"roomJid",null);
        }else if(type==1){
            if(!StringUtil.isEmpty(toUserId)){
                addToQuery(query,"roomJid", toUserId);
            }
            query.addCriteria(Criteria.where("roomJid").ne(null));
        }
        getDatastore().find(query,KeywordDenyRecord.class);
        data.setCount(count(query));
        data.setData(queryListsByQuery(query,pageIndex,pageSize,1));
        return data;
    }

    @Override
    public long queryKeywordDenyRecordCountByType(int userId, short keywordType, short chatType) {

        Query query = createQuery("fromUserId", userId);
        addToQuery(query,"keywordType",keywordType);
        addToQuery(query,"chatType",chatType);
       return getDatastore().count(query,KeywordDenyRecord.class);
    }

    @Override
    public boolean queryByWord(String word) {
        Query query = createQuery();
        addToQuery(query,"type",1);
        addToQuery(query,"word",word);
        List<KeyWord> keyWords = getDatastore().find(query, KeyWord.class);
        if (keyWords.size() > 0){
            return true;
        }
        return false;
    }
}
