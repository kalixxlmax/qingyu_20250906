package com.basic.im.manual.service.impl;

import com.basic.common.model.PageResult;
import com.basic.im.manual.dao.CollectionAccountDao;
import com.basic.im.manual.entity.CollectionAccount;
import com.basic.im.manual.service.CollectionAccountManager;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2020/3/6
 **/
@Service
public class CollectionAccountManagerImpl implements CollectionAccountManager {

    @Autowired
    private CollectionAccountDao collectionAccountDao;

    @Override
    public CollectionAccount sava(CollectionAccount securityRole) {
        return collectionAccountDao.sava(securityRole);
    }

    @Override
    public PageResult<CollectionAccount> queryCollectionAccountList(int parseInt, Integer pageSize) {
        return collectionAccountDao.queryCollectionAccountList(parseInt,pageSize);
    }

    @Override
    public void deleteCollectionAccount(ObjectId objectId) {
        collectionAccountDao.deleteCollectionAccount(objectId);
    }

    @Override
    public CollectionAccount getCollectionAccount(ObjectId objectId) {
        return collectionAccountDao.getCollectionAccount(objectId);
    }


}
