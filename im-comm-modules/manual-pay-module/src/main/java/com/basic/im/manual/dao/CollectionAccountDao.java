package com.basic.im.manual.dao;

import com.basic.common.model.PageResult;
import com.basic.im.manual.entity.CollectionAccount;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

public interface CollectionAccountDao extends IMongoDAO<CollectionAccount, ObjectId> {

    CollectionAccount sava(CollectionAccount collectionAccount);

    PageResult<CollectionAccount> queryCollectionAccountList(int parseInt, Integer pageSize);

    void deleteCollectionAccount(ObjectId objectId);

    CollectionAccount getCollectionAccount(ObjectId objectId);
}
