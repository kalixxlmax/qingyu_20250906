package com.basic.im.dao.impl;

import com.google.common.collect.Lists;
import com.mongodb.client.MongoCollection;
import com.basic.commons.thread.ThreadUtils;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.live.dao.LiveRoomMemberDao;
import com.basic.im.live.entity.LiveRoom;
import com.basic.im.repository.MongoRepository;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class LiveRoomMemberDaoImpl extends MongoRepository<LiveRoom.LiveRoomMember, ObjectId> implements LiveRoomMemberDao,CommandLineRunner {


    @Autowired
    @Qualifier(value = "mongoLiveMember")
    private MongoTemplate mongoLiveMember;

    @Override
    public MongoTemplate getDatastore() {
        return mongoLiveMember;
    }

    @Override
    public Class<LiveRoom.LiveRoomMember> getEntityClass() {
        return LiveRoom.LiveRoomMember.class;
    }

    @Override
    public void run(String... args) throws Exception {
        ThreadUtils.executeInThread(obj->{
            mongoLiveMember.getCollectionNames().forEach(name->{
                MongoCollection<Document> collection = mongoLiveMember.getCollection(name);
                if(KConstants.DB_INDEX_COUNT< collection.countDocuments()){
                    collection.createIndex(new Document("roomId",1).append("userId",1));

                    collection.createIndex(new Document("roomId",1).append("online",1));
                }
            });


        });
    }

    @Override
    public void addLiveRoomMember(LiveRoom.LiveRoomMember entity) {
        getDatastore().save(entity,getCollectionName(entity.getRoomId()));
    }

    @Override
    public void deleteLiveRoomMember(ObjectId roomId) {
        Query query = createQuery("roomId",roomId);
        deleteByQuery(query,getCollectionName(roomId));
    }

    @Override
    public void deleteLiveRoomMember(ObjectId roomId, int userId) {
        Query query = createQuery("roomId",roomId);
        if(0 != userId) {
            addToQuery(query,"userId",userId);
        }
        // 被禁言或者设置管理员的成员维护online
        LiveRoom.LiveRoomMember member = getLiveRoomMember(roomId, userId);
        if(null!=member&&(2 == member.getType() || 0 != member.getTalkTime())){
            Map<String,Object> map = new HashMap<>(1);
            map.put("online", 0);
            updateLiveRoomMember(roomId,userId,map);
        }else{
            deleteByQuery(query,getCollectionName(roomId));
        }
    }

    @Override
    public List<LiveRoom.LiveRoomMember> queryLiveRoomMemberList(int userId) {
        List<LiveRoom.LiveRoomMember> resultList= Lists.newArrayList();
        Query query = createQuery("userId",userId);
        getDatastore().getCollectionNames().forEach(name->{
            resultList.addAll(queryListsByQuery(query,name));
        });
        return resultList;
    }

    @Override
    public LiveRoom.LiveRoomMember getLiveRoomMember(ObjectId roomId, int userId) {
        Query query = createQuery("roomId",roomId);
        if(0 != userId) {
            addToQuery(query,"userId",userId);
        }
        return findOne(query,getCollectionName(roomId));
    }

    @Override
    public void updateLiveRoomMember(ObjectId roomId, int userId, Map<String, Object> map) {
        Query query = createQuery("roomId",roomId);
        if(0 != userId) {
            addToQuery(query,"userId",userId);
        }
        Update ops = createUpdate();
        map.forEach((key,value)->{
            ops.set(key,value);
        });
        update(query,ops,getCollectionName(roomId));
    }

    @Override
    public List<LiveRoom.LiveRoomMember> getLiveRoomMemberList(ObjectId roomId,int online,Integer pageIndex, Integer pageSize) {
        Query query = createQuery("roomId",roomId);
        addToQuery(query,"online",online);
        if(0 != pageSize) {
            query.with(createPageRequest(pageIndex,pageSize));
        }
        return queryListsByQuery(query,getCollectionName(roomId));
    }

    @Override
    public List<Integer> findMembersUserIds(ObjectId roomId, int online) {
        Query query = createQuery();
        if(null!=roomId) {
            addToQuery(query,"roomId",roomId);
        }
        addToQuery(query,"online", 1);
       return getDatastore().findDistinct(query,"userId",getCollectionName(roomId),getEntityClass(),Integer.class);
    }
    @Override
    public void addMemberRewardCount(ObjectId roomId,int userId,double rewardCount){
        Query query = createQuery("roomId",roomId);
        addToQuery(query,"userId",userId);

        Update update=createUpdate();
        update.inc("rewardCount",rewardCount);

        update(query, update, getCollectionName(roomId));

    }
    @Override
    public void updateMember(int userId, Map<String, Object> map) {
        Query query =createQuery("userId",userId);
        Update ops = createUpdate();
        map.forEach((key,value)->{
            ops.set(key,value);
        });

        getDatastore().getCollectionNames().forEach(name->{
            update(query,ops,name);
        });

    }


}
