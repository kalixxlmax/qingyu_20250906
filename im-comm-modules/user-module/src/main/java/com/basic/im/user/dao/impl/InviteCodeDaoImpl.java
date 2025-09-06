package com.basic.im.user.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.repository.MongoRepository;
import com.basic.im.user.dao.InviteCodeDao;
import com.basic.im.user.entity.InviteCode;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

/**
 * @author zhm
 * @version V1.0
 * @Description: TODO(todo)
 * @date 2019/9/3 14:33
 */
@Repository
public class InviteCodeDaoImpl extends MongoRepository<InviteCode, ObjectId> implements InviteCodeDao {

    @Override
    public Class<InviteCode> getEntityClass() {
        return InviteCode.class;
    }

    @Override
    public void addInviteCode(InviteCode inviteCode) {
        getDatastore().save(inviteCode);
    }

    @Override
    public InviteCode findUserInviteCode(int userId) {
        Query query =createQuery("userId",userId);
        query.addCriteria(Criteria.where("totalTimes").ne(1));
        return findOne(query);
    }

    @Override
    public PageResult<InviteCode> getInviteCodeList(int userId, String keyworld, short status, int pageIndex, int pageSize) {
        PageResult<InviteCode> result = new PageResult<InviteCode>();
        Query query = createQuery("userId", userId);

        if(keyworld!="" && keyworld!=null){
            query.addCriteria(Criteria.where("inviteCode").regex(keyworld));
        }
        if(status>=0 && status<=1){
            addToQuery(query,"status", status);
        }
        ascByquery(query,"status");
        result.setCount(count(query));
        result.setData(queryListsByQuery(query,pageIndex,pageSize,1));
        return result;
    }

    @Override
    public boolean deleteInviteCode(int userId, ObjectId inviteCodeId) {
        Query query =createQuery(inviteCodeId);
        addToQuery(query,"userId",userId);

        return deleteByQuery(query).getDeletedCount()>0;
    }

    @Override
    public InviteCode findInviteCodeByCode(String inviteCode) {
        return findOne("inviteCode",inviteCode);
    }

    @Override
    public void updateUserInviteCode(int userId, String inviteCode) {
        Query query =createQuery("userId",userId);
        Update ops = createUpdate();
        ops.set("inviteCode", inviteCode);
        update(query, ops);
    }
}
