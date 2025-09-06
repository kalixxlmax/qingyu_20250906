package com.basic.im.invite.dao.impl;

import com.basic.im.invite.dao.InviteDao;
import com.basic.im.invite.entity.Invite;
import com.basic.im.repository.MongoRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @author zhm
 * @version V1.0
 * 
 * @date 2019/11/14 16:18
 */
@Repository
public class InviteDaoImpl extends MongoRepository<Invite, ObjectId> implements InviteDao {

    @Override
    public Class<Invite> getEntityClass() {
        return Invite.class;
    }


    @Override
    public void addInvite(Invite entity) {
        getDatastore().save(entity);
    }

    // 查询上级列表
    @Override
    public List<Invite.Grade> queryGradeList(int userId) {
        Query query = createQuery("userId",userId);
        List<Invite.Grade> list = getDatastore().find(query,Invite.Grade.class);
        return list;
    }

    // 查询所有下级
    @Override
    public List<Invite> queryInviteList(int userId) {
        Query query = createQuery("gradeList.userId",userId);
        return queryListsByQuery(query);
    }
}
