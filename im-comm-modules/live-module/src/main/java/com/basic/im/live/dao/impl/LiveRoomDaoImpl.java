package com.basic.im.live.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.live.dao.LiveRoomDao;
import com.basic.im.live.entity.LiveRoom;
import com.basic.im.repository.MongoRepository;
import com.basic.utils.StringUtil;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public class LiveRoomDaoImpl extends MongoRepository<LiveRoom, ObjectId> implements LiveRoomDao {


    @Override
    public Class<LiveRoom> getEntityClass() {
        return LiveRoom.class;
    }

    @Override
    public void addLiveRoom(LiveRoom entity) {
        getDatastore().save(entity);
    }

    @Override
    public Object addLiveRoomReturn(LiveRoom liveRoom) {

        return save(liveRoom).getRoomId();
    }

    @Override
    public LiveRoom getLiveRoom(ObjectId roomId) {
        if(null==roomId){
            return null;
        }
        return get(roomId);
    }

    @Override
    public LiveRoom getLiveRoomByUserId(int userId) {
        Query query = createQuery("userId",userId);
        return findOne(query);
    }

    @Override
    public LiveRoom getLiveingByUserId(int userId) {
        Query query = createQuery("userId",userId);

        addToQuery(query,"status",1);
        return findOne(query);
    }

    @Override
    public List<LiveRoom> queryLiveRoomList(int userId,Integer pageIndex,Integer pageSize) {
        Query query = createQuery("userId",userId);
        descByquery(query,"status");
        descByquery(query,"startTime");

        if(0 != pageSize) {
            query.with(createPageRequest(pageIndex,pageSize));
        }
        return queryListsByQuery(query);
    }

    @Override
    public List<LiveRoom> queryLiveRoomList(int userId) {
        return queryListsByQuery(createQuery("userId",userId));
    }


    @Override
    public LiveRoom getLiveRoomByJid(String jid) {
        Query query = createQuery("jid",jid);
        return findOne(query);
    }

    @Override
    public LiveRoom getLiveRoomByLiveRoomId(String liveRoomId) {
        Query query = createQuery("liveRoomId",liveRoomId);
        return findOne(query);
    }

    @Override
    public void updateLiveRoom(ObjectId roomId, int userId, Map<String, Object> map) {
        Query query = createQuery(roomId);
        if(0 != userId) {
            addToQuery(query,"userId",userId);
        }
        Update ops = createUpdate();
        map.forEach(ops::set);
        update(query,ops);
    }

    @Override
    public void updateLiveRoomStatus(ObjectId roomId,Integer status) {
        Update update = createUpdate().set("status", status).set("liveRoomId", null);
        super.mongoTemplate.updateFirst(Query.query(Criteria.where("roomId").is(roomId)),update,this.getEntityClass());
    }

    @Override
    public void addClickProductCount(Integer userId, ObjectId roomId) {
        Query query = createQuery(roomId);
        Update ops = createUpdate();
        ops.inc("productClickCount", 1);
        update(query,ops);
    }

    @Override
    public void updateOrderStatistics(ObjectId roomId, int orderNum, double orderMoney) {
        Query query = createQuery(roomId);
        Update ops = createUpdate();
        ops.inc("orderCount", orderNum);

        ops.inc("orderMoneyCount", orderMoney);
        update(query,ops);
    }

    @Override
    public int liveCount() {
        Query query = createQuery();
        return queryListsByQuery(query).size();
    }

    @Override
    public void updateLiveRoomNum(ObjectId roomId, int number) {
        Query query = createQuery(roomId);
        Update ops = createUpdate();
        ops.inc("numbers", number);
        if(0<number) {
            ops.inc("audienceCount", number);
        }
        update(query,ops);
    }

    @Override
    public void addRewardCount(ObjectId roomId, double number) {
        Query query = createQuery(roomId);
        Update ops = createUpdate();
        ops.inc("rewardCount", number);
        update(query,ops);
    }

    @Override
    public void addShareCount(ObjectId roomId) {
        Query query = createQuery(roomId);
        Update ops = createUpdate();
        ops.inc("shareCount", 1);
        update(query,ops);
    }

    @Override
    public void updateProductId(ObjectId roomId,String productId){
        Query query = createQuery(roomId);
        Update ops = createUpdate();
        ops.set("productId", productId);
        update(query,ops);
    }

    @Override
    public void deleteLiveRoom(ObjectId roomId) {
       deleteById(roomId);
    }

    @Override
    public PageResult<LiveRoom> findLiveRoomList(String name, String nickName, int userId, int status, int pageIndex, int pageSize, int type) {
        Query query = createQuery();
        if(!StringUtil.isEmpty(name)){
            query.addCriteria(Criteria.where("name").regex(name));
        }
        if(!StringUtil.isEmpty(nickName)){
            query.addCriteria(Criteria.where("nickName").regex(nickName));
        }
        if(0!=userId){
            addToQuery(query,"userId",userId);
        }
        if(1==status){
            addToQuery(query,"status",status);
        }
        descByquery(query,"createTime");
        PageResult<LiveRoom> pageResult = new PageResult<>();
        pageResult.setCount(count(query));
        pageResult.setData(queryListsByQuery(query,pageIndex,pageSize,type));
        return pageResult;
    }

    @Override
    public PageResult<Document> findLiveRoomListByUserId(String userId, int pageIndex, int pageSize) {
        PageResult<Document> result = new PageResult<>();
        Criteria criteria = new Criteria();
        if (!StringUtil.isEmpty(userId)) {
            criteria = Criteria.where("userId").is(Integer.valueOf(userId));
        }

        Aggregation aggregation_ =  Aggregation.newAggregation(
                Aggregation.match(criteria),
                Aggregation.group("userId").count().as("liveCount")
        );
        int count = getDatastore().aggregate(aggregation_, "LiveRoom", Document.class).getMappedResults().size();

        Aggregation aggregation =  Aggregation.newAggregation(
                Aggregation.match(criteria),
                Aggregation.group("userId").count().as("liveCount"),
                Aggregation.skip((pageIndex-1)*pageSize),
                Aggregation.limit(pageSize)
        );
        AggregationResults<Document> documents = mongoTemplate.aggregate(aggregation, "LiveRoom", Document.class);
        List<Document> mappedResults = documents.getMappedResults();
        result.setData(mappedResults);
        result.setCount(count);
        return result;
    }


    @Override
    public void clearLiveRoom() {
        Query query=createQuery();
        deleteByQuery(query);
    }

    @Override
    public PageResult<LiveRoom> getLiveRoomList(long time) {
        PageResult<LiveRoom> pageResult = new PageResult<>();
        Query query =createQuery();
        query.addCriteria(Criteria.where("createTime").lt(time));

        pageResult.setData(queryListsByQuery(query));
        pageResult.setCount(count(query));
        return pageResult;
    }

    @Override
    public void updateLiveRoom(int userId, Map<String, Object> map) {
        Query query = createQuery("userId",userId);
        Update ops = createUpdate();
        map.forEach((key,value)->{
            ops.set(key,value);
        });
        update(query,ops);
    }

    @Override
    public void updateProductIdList(int userId,ObjectId roomId, List<String> productIdList) {
        Query query = createQuery(roomId);
        Update ops = createUpdate();
        ops.set("productIdList", productIdList);
        update(query,ops);
    }
}
