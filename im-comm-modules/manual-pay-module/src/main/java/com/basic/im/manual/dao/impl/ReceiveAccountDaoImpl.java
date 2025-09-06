package com.basic.im.manual.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.manual.dao.ReceiveAccountDao;
import com.basic.im.manual.entity.ReceiveAccount;
import com.basic.im.repository.MongoRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.Map;

/**
 * @author zhm
 * @version V1.0
 * @Description: TODO(todo)
 * @date 2019/12/2 12:22
 */
@Repository
public class ReceiveAccountDaoImpl extends MongoRepository<ReceiveAccount, ObjectId> implements ReceiveAccountDao {

    @Override
    public MongoTemplate getDatastore() {
        return super.getDatastore();
    }

    @Override
    public Class<ReceiveAccount> getEntityClass() {
        return ReceiveAccount.class;
    }

    @Override
    public void addReceiveAccount(ReceiveAccount receiveAccount) {
        getDatastore().save(receiveAccount);
    }

    @Override
    public void updateReceiveAccount(ObjectId id, Map<String, Object> map) {
        Query query = createQuery("_id",id);
        Update ops = createUpdate();
        map.forEach((key,value)->{
            ops.set(key,value);
        });
        getDatastore().updateFirst(query,ops,getEntityClass());
    }

    @Override
    public void deleteReceiveAccount(ObjectId id) {
        deleteById(id);
    }

    @Override
    public PageResult<ReceiveAccount> getReceiveAccountList(int pageIndex, int pageSize, int type, String keyword) {
        Query query = createQuery();
        if(0 != pageSize){
            query.with(createPageRequest(pageIndex,pageSize));
        }
        if(0 != type){
            addToQuery(query,"type",type);
        }
        if(!StringUtil.isEmpty(keyword)){
            query.addCriteria(Criteria.where("name").is(keyword));
        }
        descByquery(query,"createTime");
        PageResult<ReceiveAccount> result = new PageResult<>();
        result.setData(getDatastore().find(query,getEntityClass()));
        result.setCount(count(query));
        return result;
    }

    @Override
    public ReceiveAccount getReceiveAccount(ObjectId id) {
        Query query = createQuery("_id",id);
        return findOne(query);
    }
}
