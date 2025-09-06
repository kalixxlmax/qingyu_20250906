package com.basic.im.room.service;

import com.basic.im.comm.constants.KConstants;
import com.basic.im.room.dao.RoomCoreDao;
import com.basic.im.room.dao.RoomMemberCoreDao;
import com.basic.im.room.entity.Room;
import com.basic.im.room.entity.Room.Member;
import com.basic.redisson.AbstractRedisson;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.redisson.api.*;
import org.redisson.client.codec.IntegerCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.mongodb.core.mapping.Language;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Repository
public class RoomCoreRedisRepository extends AbstractRedisson {

    @Autowired(required=false)
    private RedissonClient redissonClient;

    @Override
    public RedissonClient getRedissonClient() {
        return redissonClient;
    }


    @Autowired
    @Lazy
    private RoomCoreDao roomCoreDao;

    @Autowired
    @Lazy
    private RoomMemberCoreDao roomMemberCoreDao;


    /**
     * 群组 离线推送成员列表
     */
    public static final String ROOMPUSH_MEMBERLIST = "roomPush_member:%s";

    /**
     * 用户群组 Jid 列表
     */
    public static final String ROOMJID_LIST = "roomJidList:%s";

    /**
     * 用户 免打扰的 群组列表
     */
    public static final String ROOM_NOPUSH_Jids="room_nopushJids:%s";



    /**
     * 群组对象(群组对象不包含:群成员，公告列表)
     */
    public static final String ROOMS="room:rooms:%s";



    /**
     * 群组内的群成员列表
     */
    public static final String ROOM_MEMBERLIST="room:memberList:%s";

    /**
     * 群公告列表
     */
    public static final String ROOM_NOTICELIST="room:noticeList:%s";



    final String GROUP_MESSAGE_SEQNO="msgseqNo:group:%s";


    /**
     * 指定群、指定群成员
     */
    private static final String ROOM_MEMBER="room:%s:userId:%s";


    /**
     * 用户邀请加群历史
     */
    private static final String ROOM_INVITE_HISTORY="room:invite:history:room:%s";

    public long queryGroupMessageSeqNo(String jid){

        RAtomicLong longAdder = redissonClient.getAtomicLong(String.format(GROUP_MESSAGE_SEQNO, jid));
        return longAdder.get();
    }



    /**
     * 查询群推送成员列表
     * @param jid
     * @return
     */
    public List<Integer> queryRoomPushMemberUserIds(String jid){
        String key = String.format(ROOMPUSH_MEMBERLIST,jid);
        RList<Integer> list= redissonClient.getList(key, IntegerCodec.INSTANCE);
        return list.readAll();
    }

    public void addRoomPushMember(String jid,Integer userId){
        String key = String.format(ROOMPUSH_MEMBERLIST,jid);
        RList<Integer> list = redissonClient.getList(key,IntegerCodec.INSTANCE);
        if(!list.contains(userId)) {
            list.addAsync(userId);
        }
        list.expireAsync(KConstants.Expire.DAY7, TimeUnit.SECONDS);
    }

    public void removeRoomPushMember(String jid,Integer userId){
        String key = String.format(ROOMPUSH_MEMBERLIST,jid);
        redissonClient.getList(key,IntegerCodec.INSTANCE).removeAsync(userId);

    }

    public void saveRoom(Room room){
        String key = String.format(ROOMS,room.getId().toString());
        RBucket<Room> bucket = redissonClient.getBucket(key);
        bucket.set(room, KConstants.Expire.DAY1,TimeUnit.SECONDS);
    }
    public Room queryRoom(ObjectId roomId){
        String key = String.format(ROOMS,roomId.toString());
        RBucket<Room> bucket = redissonClient.getBucket(key);
        if(bucket.isExists()) {
            return bucket.get();
        } else {
            return null;
        }
    }

    public void deleteRoom(String roomId){
        String key = String.format(ROOMS, roomId);
        redissonClient.getBucket(key).delete();
    }

    /** @Description: 群成员列表
     * @param roomId
     * @return
     **/
    public List<Member> getMemberList(String roomId){
        String key = String.format(ROOM_MEMBERLIST, roomId);
        RList<Member> rList = redissonClient.getList(key);
        return rList.readAll();
    }


    /** @Description: 删除群成员列表
     * @param roomId
     **/
    public void deleteMemberList(String roomId){
        String key = String.format(ROOM_MEMBERLIST, roomId);
        redissonClient.getBucket(key).delete();
    }

    /** @Description:保存群成员列表
     * @param roomId
     * @param members
     **/
    public void saveMemberList(String roomId,List<Member> members){
        String key = String.format(ROOM_MEMBERLIST,roomId);
        RList<Object> bucket = redissonClient.getList(key);
        bucket.clear();
        bucket.addAll(members);
        bucket.expire(KConstants.Expire.DAY7, TimeUnit.SECONDS);
    }







    public List<String> queryUserRoomJidList(Integer userId){
        String key = String.format(ROOMJID_LIST,userId);
        RList<String> list = redissonClient.getList(key);

        if(0==list.size()) {
            List<String> roomsJidList = roomMemberCoreDao.queryUserRoomsJidListByDB(userId);
            if(0<roomsJidList.size()) {
                list.addAllAsync(roomsJidList);
                list.expire(KConstants.Expire.DAY7, TimeUnit.SECONDS);
            }
            return roomsJidList;
        }else {
            return list.readAll();
        }
    }


    public void updateUserRoomJidList(Integer userId){
        String key = String.format(ROOMJID_LIST,userId);
        RBucket<Object> bucket = redissonClient.getBucket(key);
        List<String> roomsJidList = roomMemberCoreDao.queryUserRoomsJidListByDB(userId);
        bucket.set(roomsJidList, KConstants.Expire.DAY7, TimeUnit.SECONDS);

    }
    public void deleteUserRoomJidList(Integer userId){
        String key = String.format(ROOMJID_LIST,userId);
        RBucket<Object> bucket = redissonClient.getBucket(key);
        if(bucket.isExists()) {
            bucket.delete();
        }

    }



    /**
     * 查询用户开启免打扰的  群组Jid 列表
     * @param userId
     * @return
     */
    public List<String> queryNoPushJidLists(Integer userId){
        String key = String.format(ROOM_NOPUSH_Jids,userId);
        RList<String> list = redissonClient.getList(key);
        if (0 == list.size()) {
            List<String> roomsJidList = roomMemberCoreDao.queryUserNoPushJidList(userId);
            if (0 < roomsJidList.size()) {
                list.addAllAsync(roomsJidList);
                //list.expire(KConstants.Expire.DAY1, TimeUnit.SECONDS);
            }
            return roomsJidList;
        } else {
            return list.readAll();
        }
    }
    public void addToRoomNOPushJids(Integer userId,String jid){
        String key = String.format(ROOM_NOPUSH_Jids,userId);
        RList<String> list = redissonClient.getList(key);
        if(!list.contains(jid)) {
            list.addAsync(jid);
        }
        //list.expire(KConstants.Expire.DAY1, TimeUnit.SECONDS);
    }
    public void removeToRoomNOPushJids(Integer userId,String jid){
        String key = String.format(ROOM_NOPUSH_Jids,userId);
        RList<String> list = redissonClient.getList(key);
        list.removeAsync(jid);
        //list.expire(KConstants.Expire.DAY1, TimeUnit.SECONDS);
    }

    public void saveJidsByUserId(Integer userId, String jid, ObjectId roomId){
        roomMemberCoreDao.saveJidsByUserId(userId,jid,roomId);
        deleteUserRoomJidList(userId);

    }

    public void delJidsByUserId(Integer userId,String jid) {
        roomMemberCoreDao.delJidsByUserId(userId,jid);
        deleteUserRoomJidList(userId);
    }



    /**
     * 取得群成员锁
     */
    public boolean getRoomMemberLock(ObjectId roomId, Integer userId) {
        String key = String.format(ROOM_MEMBER,roomId,userId);
        RLock lock = redissonClient.getLock(key);
        boolean isLock=false;
        try{
            if (isLock=lock.tryLock(1000, TimeUnit.MILLISECONDS)){
                return true;
            }
        } catch (InterruptedException e) {
            log.error("get Member Lock is failure,error message is {} ",e.getMessage());
        } finally {
            if (isLock && lock.isLocked()){
                lock.unlock();
            }
        }
        return false;
    }


    /**
     * 添加用户邀请记录
     */
    public void addInviteCode(Integer userId, ObjectId roomId) {
        String key = String.format(ROOM_INVITE_HISTORY,roomId.toString());
        List<Integer> ids = this.getRedissonClient().getList(key);
        ids.add(userId);
    }


    /**
     * 删除用户邀请记录
     */
    public boolean removeInviteCode(Integer userId, ObjectId roomId) {
        String key = String.format(ROOM_INVITE_HISTORY,roomId.toString());
        List<Integer> ids = this.getRedissonClient().getList(key);
        if (ids.contains(userId)){
            ids.remove(userId);
            return true;
        }
        return false;
    }
}
