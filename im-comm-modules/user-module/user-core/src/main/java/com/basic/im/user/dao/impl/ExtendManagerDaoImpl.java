package com.basic.im.user.dao.impl;

import com.basic.im.repository.MongoRepository;
import com.basic.im.user.dao.ExtendManagerDao;
import com.basic.im.user.entity.UserSign;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;

@Repository
public class ExtendManagerDaoImpl extends MongoRepository<UserSign, ObjectId> implements ExtendManagerDao {

    @Override
    public Class<UserSign> getEntityClass() {
        return UserSign.class;
    }

    @Override
    public UserSign sava(UserSign userSign) {
        return getDatastore().save(userSign);
    }

    @Override
    public List<UserSign> getUserSignByUserIdAndSignDate(Integer userId, Date yesDate) {
        org.springframework.data.mongodb.core.query.Query query = createQuery("userId",String.valueOf(userId));
        addToQuery(query,"signDate",yesDate);
        return queryListsByQuery(query);
    }

    @Override
    public List<UserSign> findUserSignByMouth(Integer userId, Date firstDay, Date lastDay) {
        org.springframework.data.mongodb.core.query.Query query = createQuery();
        query.addCriteria(Criteria.where("userId").is(userId)
                .andOperator(
                        Criteria.where("signDate").gte(firstDay),
                        Criteria.where("signDate").lte(lastDay)
                )
        );
        return queryListsByQuery(query);
    }

}
