package com.basic.im.admin.service;

import com.basic.common.model.PageResult;
import com.basic.im.admin.entity.SecretQuestion;
import org.bson.types.ObjectId;

/**
 * @Description: TODO (密保问题操作)
 * @Author xie yuan yang
 * @Date 2020/5/25
 **/
public interface SecretQuestionManager {

    //分页查询密保问题
    PageResult<SecretQuestion> getListSecretQuestion(int page , int limit , String keyWord);

    //删除密保问题
    void deleteSecretQuestionById(ObjectId Id);

    SecretQuestion sava(String question,byte status);
}
