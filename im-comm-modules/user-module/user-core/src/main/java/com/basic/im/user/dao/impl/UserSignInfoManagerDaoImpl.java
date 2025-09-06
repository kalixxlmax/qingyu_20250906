package com.basic.im.user.dao.impl;

import com.basic.im.repository.MongoRepository;
import com.basic.im.user.dao.UserSignInfoManagerDao;
import com.basic.im.user.entity.UserSignInfo;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;

@Repository
public class UserSignInfoManagerDaoImpl extends MongoRepository<UserSignInfo, ObjectId> implements UserSignInfoManagerDao {

    @Override
    public Class<UserSignInfo> getEntityClass() {
        return UserSignInfo.class;
    }

    @Override
    public List<UserSignInfo> getUserSignInfoByUserId(Integer userId) {
        Query query = createQuery("userId",String.valueOf(userId));
        return queryListsByQuery(query);
    }

    @Override
    public void saveUserSignInfo(UserSignInfo userSignInfo) {
        getDatastore().save(userSignInfo);
    }

    @Override
    public void updateUserSignInfo(UserSignInfo userSignInfo) {
        Query query = createQuery("id",String.valueOf(userSignInfo.getId()));
        Update ops = createUpdate();
        ops.set("startSignDate", userSignInfo.getStartSignDate());
        ops.set("seriesSignCount", userSignInfo.getSeriesSignCount());
        ops.set("sevenCount", userSignInfo.getSevenCount());
        ops.set("signCount", userSignInfo.getSignCount());
        ops.set("dialCount", userSignInfo.getDialCount());
        ops.set("updateDate", new Date());
        update(query,ops);
    }

    @Override
    public void updateUserSignInfoCount(UserSignInfo userSignInfo) {
        Query query = createQuery("id",String.valueOf(userSignInfo.getId()));
        Update ops = createUpdate();
        ops.set("seriesSignCount", 0);
        ops.set("sevenCount", 0);
        update(query,ops);
    }

//
//    @Override
//    public List<UserSign> getUserSignByUserIdAndSignDate(Integer userId, Date yesDate) {
//        org.springframework.data.mongodb.core.query.Query query = createQuery("userId",String.valueOf(userId));
//        addToQuery(query,"signDate",yesDate);
//        return queryListsByQuery(query);
//    }

}
