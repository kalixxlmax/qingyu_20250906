package com.basic.im.admin.dao;

import com.basic.common.model.PageResult;
import com.basic.im.admin.entity.SecretQuestion;
import com.basic.mongodb.springdata.IBaseMongoRepository;
import org.bson.types.ObjectId;

/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2020/5/25
 **/
public interface SecretQuestionDao extends IBaseMongoRepository<SecretQuestion, ObjectId> {

    //分页查询密保问题
    PageResult<SecretQuestion> getListSecretQuestion(int page , int limit , String keyWord);

    //删除密保问题
    void deleteSecretQuestionById(ObjectId Id);

    //添加密保问题
    SecretQuestion savaSecretQuestion(SecretQuestion secretQuestion);
}
