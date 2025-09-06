package com.basic.im.company.dao.impl;

import com.basic.im.company.dao.InviteJoinRecordDao;
import com.basic.im.company.entity.Employee;
import com.basic.im.company.entity.InviteJoinRecord;
import com.basic.im.i18n.LocaleMessageUtils;
import com.basic.im.user.service.UserCoreService;
import com.basic.mongodb.springdata.BaseMongoRepository;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.Iterator;
import java.util.List;
import java.util.Locale;

@Repository
public class InviteJoinRecordDaoImpl extends BaseMongoRepository<InviteJoinRecord,ObjectId> implements InviteJoinRecordDao {


    @Override
    public Class<InviteJoinRecord> getEntityClass() {
        return InviteJoinRecord.class;
    }


    @Override
    public InviteJoinRecord addInviteJoinRecord(InviteJoinRecord inviteJoinRecord) {
        inviteJoinRecord.setId(new ObjectId());
        getDatastore().save(inviteJoinRecord);
        return inviteJoinRecord;
    }

    @Override
    public InviteJoinRecord queryOne(String companyId, String departmentId, int joinUserId) {
        Query query = createQuery();
        query.addCriteria(Criteria.where("companyId").is(companyId));
        query.addCriteria(Criteria.where("departmentId").is(departmentId));
        query.addCriteria(Criteria.where("joinUserId").is(joinUserId));
        return findOne(query);
    }
}
