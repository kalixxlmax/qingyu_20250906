package com.basic.im.admin.service.impl;

import com.basic.common.model.PageResult;
import com.basic.im.admin.dao.SecretQuestionDao;
import com.basic.im.admin.entity.SecretQuestion;
import com.basic.im.admin.service.SecretQuestionManager;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2020/5/25
 **/
@Service
public class SecretQuestionManagerImpl implements SecretQuestionManager {

    @Autowired
    private SecretQuestionDao secretQuestionDao;

    @Override
    public PageResult<SecretQuestion> getListSecretQuestion(int page, int limit, String keyWord) {
        return secretQuestionDao.getListSecretQuestion(page,limit,keyWord);
    }

    @Override
    public void deleteSecretQuestionById(ObjectId id) {
        secretQuestionDao.deleteById(id);
    }


    @Override
    public SecretQuestion sava(String question, byte status) {
        SecretQuestion secretQuestion = new SecretQuestion();
        secretQuestion.setQuestion(question);
        secretQuestion.setStatus(status);
        SecretQuestion sava = secretQuestionDao.savaSecretQuestion(secretQuestion);
        return sava;
    }
}
