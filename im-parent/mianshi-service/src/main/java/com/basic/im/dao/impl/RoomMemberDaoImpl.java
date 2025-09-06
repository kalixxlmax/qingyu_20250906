package com.basic.im.dao.impl;

import com.google.common.collect.Maps;
import com.basic.common.model.PageResult;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.ex.VerifyUtil;
import com.basic.im.repository.MongoRepository;
import com.basic.im.room.dao.RoomMemberCoreDao;
import com.basic.im.room.dao.RoomMemberDao;
import com.basic.im.room.entity.Room;
import com.basic.im.user.dao.UserDao;
import com.basic.im.user.entity.User;
import com.basic.im.utils.MongoUtil;
import com.basic.mongodb.wrapper.QueryWrapper;
import com.basic.mongodb.wrapper.UpdateWrapper;
import com.basic.utils.DateUtil;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.*;

@Repository
public class RoomMemberDaoImpl extends MongoRepository<Room.Member, ObjectId> implements RoomMemberDao {


    @Autowired(required = false)
    @Qualifier(value = "mongoTemplateRoomMember")
    protected MongoTemplate mongoTemplateRoomMember;
    @Autowired
    private UserDao userDao;

    @Autowired
    private RoomMemberCoreDao roomMemberCoreDao;

    @Override
    public MongoTemplate getDatastore() {
        return mongoTemplateRoomMember;
    }

    private static final String  MEMBER_DBNAME = "chat_room_member";

    @Override
    public Class<Room.Member> getEntityClass() {
        return Room.Member.class;
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
    public void addMember(Room.Member entity) {
        getDatastore().save(entity,getCollectionName(entity.getRoomId()));
    }

    @Override
    public void addMemberList(List<Room.Member> memberList) {
        if (0 == memberList.size()) {
            return;
        }

        getDatastore().insert(memberList,getCollectionName(memberList.get(0).getRoomId()));
    }

    @Override
    public Room.Member getMember(ObjectId roomId, int userId) {
        Query query =createQuery("roomId",roomId);
        addToQuery(query,"userId",userId);
        return findOne(query,getCollectionName(roomId));
    }

    @Override
    public List<Room.Member> getMemberList(ObjectId roomId,int pageIndex,int pageSize) {
        Query query = createQuery("roomId",roomId);
        descByquery(query,"createTime");
        if(0!= pageSize) {
            query.with(createPageRequest(pageIndex,pageSize,1));
        }
        return queryListsByQuery(query,getCollectionName(roomId));
    }

    @Override
    public List<Integer> getMemberUserIdList(ObjectId roomId) {
        Query query = createQuery("roomId",roomId);
        return distinct(getCollectionName(roomId),"userId",query,Integer.class);
    }

    @Override
    public PageResult<Room.Member> getMemberListResult(ObjectId roomId, int pageIndex, int pageSize) {
        Query query = createQuery("roomId",roomId);
        descByquery(query,"createTime");
        //分页
        List<Room.Member> pageData=queryListsByQuery(query,pageIndex,pageSize,1,getCollectionName(roomId));
        pageData.stream().forEach(member ->{
            User user = userDao.getUser(member.getUserId());
            member.setOnlinestate(user.getOnlinestate());
            Query logLogQuery = userDao.createQuery("userId", member.getUserId());
            User.UserLoginLog loginLog =userDao.getDatastore().findOne(logLogQuery, User.UserLoginLog.class);
            if (null != loginLog){
                member.setIpAddress(loginLog.getLoginLog().getIpAddress());
                member.setLoginTime(loginLog.getLoginLog().getLoginTime());
            }
        });
        return new PageResult<Room.Member>(pageData,count(query,getCollectionName(roomId)));
    }

    @Override
    public List<Integer> getRoomAvatarUserIdList(ObjectId roomId) {
        Query query = createQuery("roomId",roomId);
        ascByquery(query,"role");
        ascByquery(query,"createTime");
        query.with(createPageRequest(0,5));

        return distinct(getCollectionName(roomId),"userId",query,Integer.class);
    }

    @Override
    public List<Room.Member> getMemberListLessThanOrEq(ObjectId roomId, int role,int pageIndex,int pageSize) {
        Query query = createQuery("roomId",roomId);
        query.addCriteria(Criteria.where("role").lte(role));
        ascByquery(query,"role");
        ascByquery(query,"createTime");
        if(0 != pageSize)
            query.with(createPageRequest(pageIndex,pageSize));
        return queryListsByQuery(query,getCollectionName(roomId));
    }

    @Override
    public List<Room.Member> getMemberListLessThan(ObjectId roomId, int role, int pageIndex, int pageSize) {
        Query query = createQuery("roomId",roomId);
        query.addCriteria(Criteria.where("role").lt(role));
        ascByquery(query,"role");
        ascByquery(query,"createTime");
        query.with(createPageRequest(pageIndex,pageSize));
        return queryListsByQuery(query,getCollectionName(roomId));
    }

    @Override
    public List<Room.Member> getMemberListGreaterThan(ObjectId roomId, int role,int pageIndex,int pageSize) {
        Query query = createQuery("roomId",roomId);
        query.addCriteria(Criteria.where("role").gt(role));
        if(0 != pageSize){
            query.with(createPageRequest(pageIndex,pageSize));
        }

        return queryListsByQuery(query,getCollectionName(roomId));
    }

    @Override
    public List<Room.Member> getMemberListByTime(ObjectId roomId, int role, long createTime,int pageSize) {
        Query query = createQuery("roomId",roomId);
        Criteria criteria = Criteria.where("role").gt(KConstants.Room_Role.ADMIN);
        query.addCriteria(Criteria.where("createTime").gte(createTime));
        if(KConstants.Room_Role.CREATOR != role){
            criteria.ne(4);
        }
        query.addCriteria(criteria);
        ascByquery(query,"createTime");
		return queryListsByQuery(query,0,pageSize,getCollectionName(roomId));
    }


    @Override
    public List<Room.Member> getMemberListByTime(ObjectId roomId, int role, long createTime,int pageSize,String keyword) {
        Query query = createQuery("roomId",roomId);
        query.addCriteria(Criteria.where("createTime").gte(createTime));
        if(KConstants.Room_Role.CREATOR != role){
            query.addCriteria(Criteria.where("role").ne(KConstants.Room_Role.INVISIBLE));
        }

        String name = MongoUtil.tranKeyWord(keyword);
        query.addCriteria(new Criteria().orOperator(Criteria.where("nickname").regex(name), Criteria.where("remarkName").regex(name)));
        ascByquery(query,"createTime");
        return queryListsByQuery(query,0,pageSize,getCollectionName(roomId));
    }

    @Override
    public List<Room.Member> getMemberListByNickname(ObjectId roomId, String nickName) {
        return getMemberListByNickname(roomId,nickName,null);
    }
    @Override
    public void readNotice(ObjectId roomId, Integer userId) {
        Query query = createQuery("roomId",roomId);
        query.addCriteria(Criteria.where("userId").is(userId));
        Update ops = createUpdate();
        ops.set("readNotice", 1);
        update(query,ops,getCollectionName(roomId));
    }
    @Override
    public void resetReadNotice(ObjectId roomId, Integer userId) {
        Query query = createQuery("roomId",roomId);
        query.addCriteria(Criteria.where("userId").ne(userId));
        Update ops = createUpdate();
        ops.set("readNotice", 0);
        update(query,ops,getCollectionName(roomId));
    }


    @Override
    public List<Room.Member> getMemberListByNickname(ObjectId roomId, String nickName, List<Integer> userIds) {
        Query query = createQuery("roomId",roomId);
        try {
            if(null!=userIds&&!userIds.isEmpty()) {
                query.addCriteria(new Criteria().orOperator(containsIgnoreCase("nickname", nickName), Criteria.where("userId").in(userIds)));
            }else {
                query.addCriteria(containsIgnoreCase("nickname", nickName));
            }
        }catch (Exception e){
            return null;
        }
        ascByquery(query,"createTime");
        ascByquery(query,"role");
        return queryListsByQuery(query,getCollectionName(roomId));
    }



    @Override
    public List<Room.Member> getMemberListByBlack(ObjectId roomId, Integer status) {
        Query query = createQuery("roomId",roomId);
        try {
            query.addCriteria(Criteria.where("isBlack").is(status));
        }catch (Exception e){
            return null;
        }
        ascByquery(query,"createTime");
        ascByquery(query,"role");
        return queryListsByQuery(query,getCollectionName(roomId));
    }

    @Override
    public List<Room.Member> getMemberListOrder(ObjectId roomId) {
        Query query = createQuery("roomId",roomId);
        ascByquery(query,"createTime");
        ascByquery(query,"role");
        return queryListsByQuery(query,getCollectionName(roomId));
    }

    @Override
    public Map<String,Object> getMemberListOr(ObjectId roomId, int role, int userId,int pageIndex,int pageSize) {
        Query query = createQuery("roomId",roomId);
        Criteria criteria = createCriteria().orOperator(Criteria.where("role").lt(role), Criteria.where("userId").is(userId));
        query.addCriteria(criteria);
        ascByquery(query,"role");
        ascByquery(query,"createTime");
        query.with(createPageRequest(pageIndex,pageSize));
        List<Room.Member> members = queryListsByQuery(query,getCollectionName(roomId));
        Map<String,Object> membersMap = Maps.newConcurrentMap();
        membersMap.put("count",count(query,getCollectionName(roomId)));
        membersMap.put("members",members);
        return membersMap;
    }

    @Override
    public List<Room.Member> getMemberListAdminRole(ObjectId roomId, int role,int pageSize) {
        Query query = createQuery("roomId",roomId);
        Criteria criteria = Criteria.where("role").gt(KConstants.Room_Role.ADMIN);


        if(KConstants.Room_Role.CREATOR != role)
            criteria.ne(KConstants.Room_Role.INVISIBLE);
        query.addCriteria(criteria);
        ascByquery(query,"createTime");
        return queryListsByQuery(query,0,pageSize,getCollectionName(roomId));
    }







    @Override
    public List<Integer> getMemberUserIdList(ObjectId roomId,int role) {
        Query query = createQuery("roomId",roomId);
        if(role != 0)
            addToQuery(query,"role",role);
        List<Integer> memberIdList= getDatastore().findDistinct(query,"userId",getCollectionName(roomId),Integer.class);
        return  memberIdList;
    }

    @Override
    public Object getMemberOneFile(ObjectId roomId, int userId, int offlineNoPushMsg) {
        Document query = new Document("roomId",roomId);
        query.append("userId", userId);
        query.append("offlineNoPushMsg", 1);
        Object field = queryOneField(getCollectionName(roomId),"offlineNoPushMsg",query);
        return field;
    }



    @Override
    public Object findMemberRole(ObjectId roomId, int userId) {
        Object role = queryOneField(getCollectionName(roomId), "role",
                new Document("roomId", roomId).append("userId", userId));
        return role;
    }

    @Override
    public void deleteRoomMember(ObjectId roomId,Integer userId) {
        Query query = createQuery("roomId",roomId);
        if(null != userId)
            addToQuery(query,"userId",userId);
       deleteByQuery(query,getCollectionName(roomId));
    }

    @Override
    public void updateRoomMember(ObjectId roomId, long talkTime) {
        Query query = createQuery("roomId",roomId);
        Update ops = createUpdate();
        ops.set("talkTime", talkTime);
        update(query,ops,getCollectionName(roomId));
    }

    @Override
    public void updateRoomMemberDeadLine(ObjectId roomId, int userId, long deadLine) {
        Query query = createQuery("roomId",roomId);
        addToQuery(query,"userId",userId);
        Update ops = createUpdate();
        ops.set("deadLine", deadLine);
        update(query,ops,getCollectionName(roomId));
    }

    @Override
    public void updateRoomMemberRole(ObjectId roomId, int userId, int role) {
        Query query = createQuery("roomId",roomId);
        addToQuery(query,"userId",userId);
        Update ops = createUpdate();
        ops.set("role", role);
        update(query,ops,getCollectionName(roomId));
    }

    @Override
    public void updateRoomMember(ObjectId roomId, int userId, Map<String, Object> map) {
        Query query = createQuery();
        if(null != roomId){
            addToQuery(query,"roomId",roomId);
        }
        if(0 != userId){
            addToQuery(query,"userId",userId);
        }

        Update ops = createUpdate();
        map.forEach((key,value)->{
            ops.set(key,value);
        });
        update(query,ops,getCollectionName(roomId));
    }

    @Override
    public void updateRoomMemberAttribute(ObjectId roomId, int userId, String key, Object value) {
        Query query = createQuery();
        if(null != roomId){
            addToQuery(query,"roomId",roomId);
        }
        if(0 != userId){
            addToQuery(query,"userId",userId);
        }

        Update ops = createUpdate().set(key,value);
        if (null != roomId) {
            update(query,ops,getCollectionName(roomId));
        }else{
            getDatastore().getCollectionNames().forEach(name->{
                update(query,ops,name);
            });
        }

    }

    @Override
    public void updateRoomMemberNickName(ObjectId roomId, int userId, String oldNickName, Object newNickName) {
        Query query = createQuery();
        if(null != roomId){
            addToQuery(query,"roomId",roomId);
        }
        if(0 != userId){
            addToQuery(query,"userId",userId);
        }
        query.addCriteria(Criteria.where("nickname").is(oldNickName));


        Update ops = createUpdate().set("nickname",newNickName);

        getDatastore().getCollectionNames().forEach(name->{
            update(query,ops,name);
        });
    }

    @Override
    public long getMemberNumGreaterThanOrEq(ObjectId roomId, byte role,int pageIndex,int pageSize) {
        Query query = createQuery("roomId",roomId);
        if(0 != pageIndex && 0!=pageSize){
           query.with(createPageRequest(pageIndex,pageSize));
        }
        query.addCriteria(Criteria.where("role").gte(role));
        return count(query,getCollectionName(roomId));
    }

    @Override
    public long getMemberNumLessThan(ObjectId roomId, byte role,int userId) {
        Query query = createQuery("roomId",roomId);
        Criteria criteria = createCriteria().orOperator(Criteria.where("role").lt(KConstants.Room_Role.INVISIBLE), Criteria.where("userId").is(userId));
        query.addCriteria(criteria);
        return count(query,getCollectionName(roomId));
    }

    @Override
    public void setBeginMsgTime(ObjectId roomId, int userId,long clearMaxNo) {
        Query query = createQuery("roomId",roomId);
        addToQuery(query,"userId",userId);
        Update ops = createUpdate();
        ops.set("beginMsgTime", DateUtil.currentTimeSeconds());
        ops.set("clearMaxSeqNo",clearMaxNo);
        update(query,ops,getCollectionName(roomId));
    }
    @Override
    public long getAdminMemberNum(ObjectId roomId, byte role) {
        Query query = createQuery("roomId",roomId);
        query.addCriteria(Criteria.where("role").is(role));
        return count(query,getCollectionName(roomId));
    }

    @Override
    public List<String> joinGroupList(Integer userId) {
        Set<String> roomJidsSet = new HashSet<>(roomMemberCoreDao.queryUserRoomsJidListByDB(userId));
        return new ArrayList<>(roomJidsSet);
    }

    @Override
    public boolean exists(ObjectId roomId, Integer userId) {
        Query query = createQuery("roomId",roomId);
        addToQuery(query,"userId",userId);
        return 0 < count(query,getCollectionName(roomId));
    }

    @Override
    public void setHideChatSwitch(ObjectId roomId, Integer userId, byte hideChatSwitch) {
        updateFirst(QueryWrapper.query(Room.Member::getRoomId,roomId)
                    .eq(Room.Member::getUserId,userId).build(),
                UpdateWrapper.update(Room.Member::getHideChatSwitch,hideChatSwitch).build()
        ,getCollectionName(roomId));
    }


    @Override
    public List<Room.Member> findByNotInUserIds(String roomId, List<Integer> userIds,int pageIndex, int pageSize) {
        ObjectId roomObjId=VerifyUtil.verifyObjectId(roomId);
        Query query = QueryWrapper.query(Room.Member::getRoomId, roomObjId).nin(Room.Member::getUserId,userIds).build();
        query.with(createPageRequest(pageIndex,pageSize));
        return queryListsByQuery(query,getCollectionName(roomObjId));
    }

    @Override
    public List<Room.Member> findByUserIds(int userId,String roomId, List<Integer> userIds, int pageIndex, int pageSize) {
        if (userIds.contains(userId)){
            if (userIds.size()==1){
                return null;
            }
            userIds.remove(userId); // 排除当前用户
        }
        ObjectId roomObjId=VerifyUtil.verifyObjectId(roomId);
        Query query = QueryWrapper.query(Room.Member::getRoomId, roomObjId).in(Room.Member::getUserId,userIds).build();
        query.with(createPageRequest(pageIndex,pageSize));
        return queryListsByQuery(query,getCollectionName(roomObjId));
    }

    @Override
    public List<Room.Member> findMemberByRoomId(int userId,String roomId, int pageIndex, int pageSize) {
        ObjectId roomObjId=VerifyUtil.verifyObjectId(roomId);
        Query query = QueryWrapper.query(Room.Member::getRoomId, roomObjId).nin(Room.Member::getUserId,userId).build();
        query.with(createPageRequest(pageIndex,pageSize));
        return queryListsByQuery(query,getCollectionName(roomObjId));
    }
}
