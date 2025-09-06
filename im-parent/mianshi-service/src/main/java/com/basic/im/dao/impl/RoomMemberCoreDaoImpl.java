package com.basic.im.dao.impl;

import cn.hutool.core.util.StrUtil;
import com.mongodb.client.MongoCollection;
import com.basic.commons.thread.ThreadUtils;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.repository.MongoRepository;
import com.basic.im.room.dao.RoomMemberCoreDao;
import com.basic.im.room.entity.Room;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public class RoomMemberCoreDaoImpl extends MongoRepository<Room.Member, ObjectId> implements RoomMemberCoreDao, CommandLineRunner {

    private static final String  MEMBER_DBNAME = "chat_room_member";

    public static final String  CHAT_ROOMJIDS_USERID = "chat_roomJids_userId";

    @Override
    public Class<Room.Member> getEntityClass() {
        return Room.Member.class;
    }

    @Autowired(required = false)
    @Qualifier(value = "mongoTemplateRoomMember")
    protected MongoTemplate mongoTemplateRoomMember;

    @Autowired(required = false)
    @Qualifier(value = "mongoRoomMemberJid")
    protected MongoTemplate mongoRoomMemberJid;


    private MongoTemplate getMongoRoomMemberJid(){
        return mongoRoomMemberJid;
    }

    @Override
    public MongoTemplate getDatastore() {
        return mongoTemplateRoomMember;
    }

    @Override
    public String getCollectionName(ObjectId id) {
        if (null == id) {
            logger.info(" ====  getCollectionName ObjectId is null  ====");
            throw new ServiceException("ObjectId  is  null !");
        } else {
            int remainder = 0;
            int counter = id.getCounter();
             remainder = counter / KConstants.DB_REMAINDER.MEMBER;
            return String.valueOf(remainder);
        }
    }

    @Override
    public void run(String... args) throws Exception {
        ThreadUtils.executeInThread(obj->{
            mongoTemplateRoomMember.getCollectionNames().forEach(name->{
                MongoCollection<Document> collection = mongoTemplateRoomMember.getCollection(name);
                if(KConstants.DB_INDEX_COUNT< collection.countDocuments()){

                    collection.createIndex(new Document("roomId",1).append("offlineNoPushMsg",1));

                    collection.createIndex(new Document("userId",1).append("role",1).append("createTime",1));

                    collection.createIndex(new Document("roomId",1).append("userId",1).append("offlineNoPushMsg",1));
                }
            });

            mongoRoomMemberJid.getCollectionNames().forEach(name->{
                MongoCollection<Document> collection = mongoRoomMemberJid.getCollection(name);
                if(KConstants.DB_INDEX_COUNT< collection.countDocuments()){
                    collection.createIndex(new Document("userId",1).append("jid",1));

                }
            });


        });
    }

    @Override
    public List<Integer> getRoomPushUserIdList(ObjectId roomId) {

        Query query = createQuery("roomId",roomId);
        query.addCriteria(Criteria.where("offlineNoPushMsg").ne(1));
        List<Integer> memberIdList= getDatastore().findDistinct(query,"userId",getCollectionName(roomId),Integer.class);
        return  memberIdList;
    }

    @Override
    public List<String> queryUserNoPushJidList(int userId) {

        Query query=createQuery("userId",userId);
        addToQuery(query,"offlineNoPushMsg", 1);

        return getMongoRoomMemberJid().findDistinct(query,"jid",getCollectionName(userId),String.class);
    }

    @Override
    public List<Integer> queryRoomMemberUserIdList(ObjectId roomId) {
        Query query = createQuery("roomId", roomId);
        List<Integer> roomIds= getDatastore().findDistinct(query,"userId",getCollectionName(roomId),Integer.class);
        return roomIds;
    }

    @Override
    public List<ObjectId> getRoomIdListByUserId(Integer userId) {
        Query query = createQuery("userId", userId);
        List<ObjectId> roomIds= getMongoRoomMemberJid().findDistinct(query,"roomId",getCollectionName(userId),ObjectId.class);
        return roomIds;
    }

    @Override
    public List<ObjectId> getRoomIdListByType(Integer userId, int type) {
        Query query = createQuery("userId", userId);
        if (1 == type) {// 自己的房间
            addToQuery(query,"role",1);
        } else if (2 == type) {// 加入的房间
            query.addCriteria(Criteria.where("role").ne(1));
        }
        return getMongoRoomMemberJid().findDistinct(query,"roomId",getCollectionName(userId),ObjectId.class);
    }

    @Override
    public List<ObjectId> getRoomIdListByUser(Integer userId) {
        Query query = createQuery("userId", userId);
        return getMongoRoomMemberJid().findDistinct(query,"roomId",getCollectionName(userId),ObjectId.class);
    }

    @Override
    public Room.Member getMember(ObjectId roomId, Integer userId) {
        Query query =createQuery("roomId",roomId);
        addToQuery(query,"userId",userId);
        return findOne(query,getCollectionName(roomId));
    }

    @Override
    public Object getMemberIsNoPushMsg(ObjectId roomId, Integer userId) {
        Document query = new Document("roomId",roomId);
        query.append("userId", userId);
        query.append("offlineNoPushMsg", 1);
        Object field = queryOneField(getCollectionName(roomId),"offlineNoPushMsg",query);
        return field;
    }
    @Override
    public List<String> queryUserRoomsJidListByDB(int userId) {
        Query query=createQuery("userId", userId);
        return getMongoRoomMemberJid().findDistinct(query,"jid",getCollectionName(userId),String.class);
    }

    @Override
    public void deleteRoomJidsUserId(Integer userId) {
        Query query=createQuery("userId", userId);
        getMongoRoomMemberJid().remove(query,getCollectionName(userId));

    }

    @Override
    public void updateRoomJidsUserId(Integer userId, List<String> jids) {

        /**/

       /* Query query=createQuery("userId", userId);
        Update update=createUpdate();
        update.set("jids",jids);
        getMongoRoomMemberJid().upsert(query,update,getCollectionName(userId));
        roomCoreRedisRepository.updateUserRoomJidList(userId);*/
    }


    /** @Description: 保存用户加入的群组
     * @param userId
     * @param jid
     **/
    @Override
    public void saveJidsByUserId(Integer userId, String jid, ObjectId roomId) {
        Document document = new Document();
        document.append("userId", userId).append("jid", jid).append("roomId", roomId);
        document.append("offlineNoPushMsg", 0);
        getMongoRoomMemberJid().getCollection(getCollectionName(userId)).insertOne(document);
    }



    /** 删除用户加入的群组
     * @param userId
     * @param jid
     */
    @Override
    public void delJidsByUserId(Integer userId,String jid) {
        Document document = new Document();
        document.append("userId", userId).append("jid", jid);
        getMongoRoomMemberJid().getCollection(getCollectionName(userId)).deleteOne(document);

    }

    @Override
    public List<String> getListByNotInBlack(List<String> objectIdList,ObjectId roomId, int isBlack) {
        Query query = createQuery("roomId", roomId);
        query.addCriteria(Criteria.where("isBlack").nin(isBlack).and("id").in(objectIdList));
        List<Room.Member> memberList = queryListsByQuery(query);
        List<String> resultList=new ArrayList<>();
        memberList.forEach(obj-> resultList.add(StrUtil.toString(obj.getId())));
        return resultList;
    }


    @Override
    public List<ObjectId> getRoomIdListLessType(Integer userId, int type) {
        Query query = createQuery("userId", userId);
        //query.addCriteria(Criteria.where("role").lte(type));
        return getMongoRoomMemberJid().findDistinct(query,"roomId",getCollectionName(userId),ObjectId.class);
    }



}
