package com.basic.im.user.dao.impl;

import com.basic.im.repository.MongoRepository;
import com.basic.im.user.entity.UserStatusCount;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

/**
 * @author zhm
 * @version V1.0
 * @Description:
 * @date 2019/9/4 9:57
 */
@Repository
public class UserStatusCountDaoImpl extends MongoRepository<UserStatusCount, ObjectId> implements UserStatusCountDao {


    @Override
    public Class<UserStatusCount> getEntityClass() {
        return UserStatusCount.class;
    }

    @Override
    public void addUserStatusCount(UserStatusCount userStatusCount) {
        getDatastore().save(userStatusCount);
    }

    @Override
    public UserStatusCount getUserStatusCount(long startTime, long endTime, int type) {
        Query query= createQuery("type",type);
        query.addCriteria(Criteria.where("time").gte(startTime).lt(endTime));
        descByquery(query,"count");
        return findOne(query);
    }

    @Override
    public long maxOnlineCount() {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("type").is(1)),
                Aggregation.group().max("count").as("maxCount")
        );
        AggregationResults<Document> userCollection = mongoTemplate.aggregate(aggregation, "UserStatusCount", Document.class);
        if(null==userCollection.getMappedResults()||userCollection.getMappedResults().isEmpty()){
            return 0;
        }
        return userCollection.getMappedResults().get(0).getLong("maxCount");
    }
}
