package com.basic.im.manual.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.manual.dao.CollectionAccountDao;
import com.basic.im.manual.entity.CollectionAccount;
import com.basic.im.repository.MongoRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2020/3/6
 **/
@Repository
public class CollectionAccountDaoImpl extends MongoRepository<CollectionAccount, ObjectId> implements CollectionAccountDao {
    @Override
    public Class<CollectionAccount> getEntityClass() {
        return CollectionAccount.class;
    }

    @Override
    public CollectionAccount sava(CollectionAccount lineConfig) {
        return getDatastore().save(lineConfig);
    }

    @Override
    public PageResult<CollectionAccount> queryCollectionAccountList(int pageIndex, Integer pageSize) {
        PageResult<CollectionAccount> result = new PageResult<>();
        Query query = createQuery();
        query.with(createPageRequest(pageIndex-1,pageSize));
        result.setData(queryListsByQuery(query));
        result.setCount(count(query));
        return result;
    }

    @Override
    public void deleteCollectionAccount(ObjectId objectId) {
        deleteByQuery(createQuery(objectId));
    }

    @Override
    public CollectionAccount getCollectionAccount(ObjectId objectId) {
        return get(objectId);
    }


}
