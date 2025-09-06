package com.basic.im.admin.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.admin.dao.SecretQuestionDao;
import com.basic.im.admin.entity.SecretQuestion;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.repository.MongoRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2020/5/25
 **/
@Repository
public class SecretQuestionDaoImpl extends MongoRepository<SecretQuestion, ObjectId> implements SecretQuestionDao {


    @Override
    public Class<SecretQuestion> getEntityClass() {
        return SecretQuestion.class;
    }

    @Override
    public  PageResult<SecretQuestion> getListSecretQuestion(int page, int limit, String keyWord) {
        PageResult<SecretQuestion> result=new PageResult<SecretQuestion>();
        Query query=createQuery();
        if(!StringUtil.isEmpty(keyWord)){
           query.addCriteria(new Criteria("question").regex(keyWord));
        }
        result.setCount(count(query));
        query.with(createPageRequest(page-1,limit));
        descByquery(query,"createTime");
        result.setData(queryListsByQuery(query));
        return result;
    }

    @Override
    public void deleteSecretQuestionById(ObjectId id) {
        deleteById(id);
    }

    @Override
    public SecretQuestion savaSecretQuestion(SecretQuestion secretQuestion) {
        secretQuestion.setCreateTime(DateUtil.currentTimeSeconds());
        SecretQuestion save = save(secretQuestion);
        return save;
    }

}
