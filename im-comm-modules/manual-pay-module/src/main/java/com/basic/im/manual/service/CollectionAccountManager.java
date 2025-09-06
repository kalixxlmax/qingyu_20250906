package com.basic.im.manual.service;

import com.basic.common.model.PageResult;
import com.basic.im.manual.entity.CollectionAccount;
import org.bson.types.ObjectId;

public interface CollectionAccountManager {

    CollectionAccount sava(CollectionAccount lineConfig);

    PageResult<CollectionAccount> queryCollectionAccountList(int parseInt, Integer valueOf);

    void deleteCollectionAccount(ObjectId objectId);

    CollectionAccount getCollectionAccount(ObjectId objectId);
}
