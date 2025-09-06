package com.basic.im.room.dao.impl;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.basic.common.model.PageResult;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.constants.MsgType;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.message.IMessageRepository;
import com.basic.im.repository.MongoRepository;
import com.basic.im.room.dao.RoomDao;
import com.basic.im.room.dao.RoomMemberDao;
import com.basic.im.room.entity.Room;
import com.basic.im.room.vo.NearByRoom;
import com.basic.im.utils.ConstantUtil;
import com.basic.im.utils.MongoUtil;
import com.basic.im.utils.SKBeanUtils;
import com.basic.mongodb.wrapper.QueryWrapper;
import com.basic.utils.StringUtil;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Repository
public class RoomDaoImpl extends MongoRepository<Room, ObjectId> implements RoomDao {


    @Autowired(required = false)
    @Qualifier(value = "mongoTemplateForRoom")
    protected MongoTemplate dsForRoom;



    @Autowired
    @Lazy
    IMessageRepository messageRepository;

    @Autowired(required=false)
    private RoomMemberDao roomMemberDao;

    @Override
    public MongoTemplate getDatastore() {
        return dsForRoom;
    }
    public final  String MUCMSG="mucmsg_";

    @Override
    public Class<Room> getEntityClass() {
        return Room.class;
    }

    @Override
    public void addRoom(Room room) {
        getDatastore().save(room);
    }



    @Override
    public Room getRoom(String roomname, ObjectId roomId) {
        Query query =createQuery("name",roomname);
        if(null!=roomId) {
            addToQuery(query,"_id",roomId);
        }
        return findOne(query);
    }



    @Override
    public long getAllRoomNums() {
        try {
            return getDatastore().getCollection(Room.getDBName()).countDocuments();
        }catch (Exception e){
            return 0;
        }

    }

    @Override
    public void updateRoomUserSize(ObjectId roomId, int userSize) {
       Query query=createQuery(roomId);

        Update update=createUpdate().inc("userSize", userSize);
        update(query,update);
    }

    @Override
    public void updateRoom(ObjectId roomId, Map<String, Object> map) {
        Query query = createQuery(roomId);
        Update ops = createUpdate();
        map.forEach(ops::set);
        update(query,ops);
    }

    @Override
    public void updateRoomByUserId(int userId, Map<String, Object> map) {
        Query query = createQuery("userId",userId);
        Update ops =createUpdate();
        map.forEach((key,vaule)->{
            ops.set(key,vaule);
        });
        update(query,ops);
    }

    @Override
    public void deleteRoom(ObjectId roomId) {
       deleteById(roomId);
    }

    @Override
    public Integer getCreateUserId(ObjectId roomId) {
        return (Integer) queryOneField("userId", new Document("_id", roomId));
    }


    @Override
    public Integer queryRoomStatus(ObjectId roomId) {
        return (Integer) queryOneFieldById("s",roomId);
    }


    @Override
    public Integer getRoomStatus(ObjectId roomId) {
        return (Integer) queryOneField("s", new Document("_id", roomId));
    }



    @Override
    public List<Room> getRoomList(List<ObjectId> list, int s, int pageIndex, int pageSize) {
        // 关闭端到端版本后不返回端到端的私密群组
        Query query = createQuery();
        byte isOpenSecureChat = SKBeanUtils.getImCoreService().getClientConfig().getIsOpenSecureChat();
        if (0 == isOpenSecureChat) {
            query.addCriteria(Criteria.where("isSecretGroup").ne(1));
        }
        query.addCriteria(Criteria.where("_id").in(list));
        if (0 != s) {
            addToQuery(query, "s", s);
        }
        if (0 != pageSize) {
            query.with(createPageRequest(pageIndex, pageSize));
        }
        descByquery(query, "_id");
        return queryListsByQuery(query);
    }

    /**
     * 获取群组列表
     * @param nearByRoom
     * @return
     */
    @Override
    public List<Room> getRoomList(NearByRoom nearByRoom) {
        // 附近的群组,禁止返回端到端的私密群组
        Query query = createQuery("isLook", 0);
       /* byte isOpenSecureChat = SKBeanUtils.getImCoreService().getClientConfig().getIsOpenSecureChat();
        if (0 == isOpenSecureChat) {
            query.addCriteria(Criteria.where("isSecretGroup").ne(1));
        }*/
        query.addCriteria(Criteria.where("isSecretGroup").ne(1));
        if (!StringUtil.isEmpty(nearByRoom.getRoomName())){
            //query.addCriteria(containsIgnoreCase("name",nearByRoom.getRoomName()));
            query.addCriteria(Criteria.where("name").is(MongoUtil.tranKeyWord(nearByRoom.getRoomName())));
        }
        if(null != nearByRoom.getRoomIds()){
            query.addCriteria(Criteria.where("_id").in(nearByRoom.getRoomIds()));
        }
        if (0 != nearByRoom.getS()) {
            addToQuery(query, "s", nearByRoom.getS());
        }
        if (0 != nearByRoom.getPageSize()) {
            query.with(createPageRequest(nearByRoom.getPageIndex(), nearByRoom.getPageSize()));
        }
        if(0 != nearByRoom.getLongitude() && 0 != nearByRoom.getLatitude()){
            if (0 == nearByRoom.getDistance()){
                nearByRoom.setDistance(ConstantUtil.getAppDefDistance());
            }
            query.addCriteria(Criteria.where("loc").withinSphere(
                    new Circle(new Point(nearByRoom.getLongitude(),nearByRoom.getLatitude()),new Distance(nearByRoom.getDistance(), Metrics.KILOMETERS)))
            );
        }

        List<Room> rooms =  queryListsByQuery(query,nearByRoom.getPageIndex(),nearByRoom.getPageSize());
        if(!rooms.isEmpty()){
            List<String> joinGroupRoomIds =  roomMemberDao.joinGroupList(nearByRoom.getUserId());
            if(null != joinGroupRoomIds &&! joinGroupRoomIds.isEmpty()){
                for (Room room : rooms) {
                    room.setInGroup(joinGroupRoomIds.contains(room.getJid()));
                }
            }
        }
        return rooms;
    }

    @Override
    public List<Room> getRoomListOrName(int pageIndex, int pageSize, String roomName,int isSearchAllJoinGroup) {
        Query query = createQuery();
        /*if (isSearchAllJoinGroup == KConstants.ZERO){
            query.addCriteria(Criteria.where("isLook").is(0));
            query.addCriteria(Criteria.where("isSecretGroup").ne(1));
            if (!StringUtil.isEmpty(roomName)){
                //query.addCriteria(containsIgnoreCase("name", MongoUtil.tranKeyWord(roomName)));
                query.addCriteria(Criteria.where("name").is(roomName));
            }
        }else {
            if (!StringUtil.isEmpty(roomName)){
                query.addCriteria(containsIgnoreCase("name", MongoUtil.tranKeyWord(roomName)));
                //query.addCriteria(Criteria.where("name").is(roomName));
            }
        }*/

        query.addCriteria(Criteria.where("isLook").is(0));
        query.addCriteria(Criteria.where("isSecretGroup").ne(1));
        if (!StringUtil.isEmpty(roomName)){
            //query.addCriteria(containsIgnoreCase("name", MongoUtil.tranKeyWord(roomName)));
            query.addCriteria(Criteria.where("name").is(MongoUtil.tranKeyWord(roomName)));
        }else {
            return new ArrayList<>();
        }
        descByquery(query,"_id");
        return queryListsByQuery(query,pageIndex,pageSize);
    }
    @Override
    public PageResult<Room> adminQueryRoomList(int page, int limit, int leastNumbers, String keyWorld, String isSecretGroup){
        PageResult<Room> result = new PageResult<Room>();

        Query query = createQuery();

        if (!com.basic.im.comm.utils.StringUtil.isEmpty(keyWorld)) {
            query.addCriteria(new Criteria().orOperator(
                    Criteria.where("name").regex(keyWorld),
                    Criteria.where("jid").is(keyWorld),
                    Criteria.where("_id").is(keyWorld)
            ));
        }
        if (!com.basic.im.comm.utils.StringUtil.isEmpty(isSecretGroup)) {
            query.addCriteria(Criteria.where("isSecretGroup").is(Integer.valueOf(isSecretGroup)));
        }
        if(leastNumbers > 0) {
            query.addCriteria(Criteria.where("userSize").gt(leastNumbers));
        }
        result.setCount(count(query));
        query.with(Sort.by(Sort.Order.desc("createTime")));
        query.with(PageRequest.of(page-1,limit));
        result.setData(queryListsByQuery(query));
        return result;
    }

    @Override
    public PageResult<Room> queryActiveRoomList(int page, int limit){
        PageResult<Room> result = new PageResult<Room>();

        PageResult<Document> activeGroupList = messageRepository.queryActiveGroupList(page - 1, limit);
        if(activeGroupList.getData().isEmpty()){
            return result;
        }

        List<String> jid = activeGroupList.getData().stream().map(document -> document.getString("jid")).collect(Collectors.toList());
        Query query = createQuery();
        query.addCriteria(Criteria.where("jid").in(jid));

        List<Room> roomList = queryListsByQuery(query);

        result.setCount(result.getCount());

        result.setData(roomList);
        return result;
    }

    @Override
    public String getRoomIdByJid(String roomJid) {
        Query query = QueryWrapper.query(Room::getJid, roomJid).build();
        query.fields().include("id");
        Room room = getDatastore().findOne(query, this.getEntityClass());
        return ObjectUtil.isNull(room)? StrUtil.EMPTY : room.getId().toString();
    }

    @Override
    public List<Object> getAddRoomsCount(long startTime, long endTime, String mapStr, String reduce) {
        List<Object> countData = new ArrayList<>();
        Document queryTime = new Document("$ne",null);

        if(startTime!=0 && endTime!=0){
            queryTime.append("$gt", startTime);
            queryTime.append("$lt", endTime);
        }
        BasicDBObject query = new BasicDBObject("createTime",queryTime);

        //获得用户集合对象
        MongoCollection<Document> collection = getDatastore().getCollection(Room.getDBName());

        MapReduceIterable<Document> mapReduceOutput = collection.mapReduce(mapStr,reduce);
        MongoCursor<Document> iterator = mapReduceOutput.filter(query).iterator();
        Map<String,Double> map = new HashMap<String,Double>();
        try {
            while (iterator.hasNext()) {
                Document obj =  iterator.next();

                map.put((String)obj.get("_id"),(Double)obj.get("value"));
                countData.add(JSON.toJSON(map));
                map.clear();
                //System.out.println(JSON.toJSON(obj));

            }
        }finally {
            if(null!=iterator){
                iterator.close();
            }
        }


        return countData;
    }

    @Override
    public List<String> queryRoomHistoryFileType(String roomJid) {
        Query query=createQuery();
        query.addCriteria(Criteria.where("contentType").in(MsgType.FileTypeArr));
        return getDatastore().findDistinct(query,"content",MUCMSG+roomJid,String.class);
    }

    // 删除群组聊天记录
    @Override
    public void dropRoomChatHistory(String roomJid) {
        messageRepository.dropRoomChatHistory(roomJid);
    }

    @Override
    public Room getRoom(ObjectId roomId) {
        Query query=createQuery(roomId);
        return findOne(query);
    }

    @Override
    public void setRoomFlag(Integer userId, ObjectId roomId, int flag) {
        Query query=createQuery(roomId);

        Update update=createUpdate().set("flag", 1==flag);
        updateFirst(query,update);
    }

    @Override
    public List<Room> queryFlagRoomList(Integer userId) {
        Query query=createQuery("flag",true);

        query.fields().include("_id")
                .include("name")
                .include("jid").include("userId");
        return queryListsByQuery(query);
    }

    @Override
    public List<ObjectId> queryFlagRoomIdList(Integer userId) {
        Query query=createQuery("flag",true);
        return distinct("_id",query,ObjectId.class);
    }

    @Override
    public List<Room> queryFlagRoomList(List<ObjectId> roomIdList) {
        Query query=createQuery();
        query.addCriteria(Criteria.where("_id").in(roomIdList));

        query.addCriteria(createCriteria().orOperator(
                Criteria.where("encryptType").gt(0),
                Criteria.where("isSecretGroup").is(1)
        ));

       /* query.fields().include("_id")
                .include("name")
                .include("jid").include("userId").include("isAttritionNotice");*/

        return queryListsByQuery(query);
    }



    @Override
    public List<Room> getRoomListByCreateTime(long endTime) {
        // 关闭端到端版本后不返回端到端的私密群组
        Query query = createQuery();
        query.addCriteria(Criteria.where("createTime").lt(endTime));
        //query.limit(100); //一次最多取100 条数据
        return queryListsByQuery(query);
    }






}
