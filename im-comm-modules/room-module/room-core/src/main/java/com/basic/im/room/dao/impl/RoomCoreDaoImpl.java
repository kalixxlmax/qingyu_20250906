package com.basic.im.room.dao.impl;

import com.basic.im.repository.MongoRepository;
import com.basic.im.room.dao.RoomCoreDao;
import com.basic.im.room.entity.Room;
import com.basic.im.utils.MongoUtil;
import com.basic.utils.StringUtil;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public class RoomCoreDaoImpl extends MongoRepository<Room, ObjectId> implements RoomCoreDao {

    public static final String  CHAT_ROOMJIDS_USERID = "chat_roomJids_userId";

    @Override
    public Class<Room> getEntityClass() {
        return Room.class;
    }

    @Autowired(required = false)
    @Qualifier(value = "mongoTemplateForRoom")
    protected MongoTemplate dsForRoom;
    @Override
    public MongoTemplate getDatastore() {
        return dsForRoom;
    }

    @Override
    public Room getRoomById(ObjectId roomId) {
        return queryOneById(roomId);
    }


    @Override
    public Room getRoomByDesc(String  desc) {
        Query query = createQuery("desc",desc);
        return  findOne(query);
    }


    @Override
    public Room getRoomByJid(String roomJid) {
        return findOne("jid",roomJid);
    }

    @Override
    public ObjectId getRoomId(String jid) {
        return (ObjectId) queryOneField("_id", new Document("jid", jid));
    }

    @Override
    public String queryRoomJid(ObjectId roomId) {
        return (String) queryOneFieldById("jid",roomId);
    }

    @Override
    public String getRoomNameByJid(String jid) {
        Object oneField = queryOneField("name", new Document("jid", jid));
        return  oneField !=null?(String) oneField :null;
    }

    @Override
    public String getRoomNameByRoomId(ObjectId roomId) {
        return (String) queryOneField("name", new Document("_id", roomId));
    }

    @Override
    public List<String> queryUserRoomsJidList(List<ObjectId> list) {
        //logger.info("queryUserRoomsJidList : {}", JSON.toJSONString(list));
        Query query=createQuery();
        query.addCriteria(Criteria.where("_id").in(list));
        return getDatastore().findDistinct(query,"jid",getEntityClass(),String.class);
    }

    @Override
    public List<Room> getRoomByNameInIds(String keyword, List<ObjectId> roomIdList, int pageIndex, int pageSize) {
        Query query = createQuery();
        query.addCriteria(Criteria.where("_id").in(roomIdList));
        if (!StringUtil.isEmpty(keyword)) {
            //query.addCriteria(Criteria.where("name").is(keyword));
            query.addCriteria(containsIgnoreCase("name",MongoUtil.tranKeyWord(keyword)));
        }else {
            return new ArrayList<>();
        }
        // query.addCriteria(containsIgnoreCase("name",MongoUtil.tranKeyWord(keyword)));
        query.with(createPageRequest(pageIndex,pageSize));
        return queryListsByQuery(query);
    }
}
