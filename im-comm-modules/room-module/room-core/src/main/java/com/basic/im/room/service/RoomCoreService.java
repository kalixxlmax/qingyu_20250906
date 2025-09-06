package com.basic.im.room.service;

import cn.hutool.core.collection.CollectionUtil;
import com.basic.im.room.dao.RoomCoreDao;
import com.basic.im.room.dao.RoomMemberCoreDao;
import com.basic.im.room.entity.Room;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import javax.management.Query;
import java.util.Collections;
import java.util.List;

@Service
public class RoomCoreService {


    @Autowired
    @Lazy
    private RoomCoreDao roomCoreDao;

    @Autowired
    @Lazy
    private RoomMemberCoreDao  roomMemberCoreDao;

    public ObjectId getRoomId(String roomJid) {
        return roomCoreDao.getRoomId(roomJid);
    }

    public boolean getMemberIsNoPushMsg(ObjectId roomId, Integer userId) {
        Room.Member member = roomMemberCoreDao.getMember(roomId, userId);
        if(null == member) {
            return true;
        }
        Object field = roomMemberCoreDao.getMemberIsNoPushMsg(roomId,userId);
        return null!=field;
    }

    public String getRoomName(String jid) {
        return roomCoreDao.getRoomNameByJid(jid);
    }

    public Room getRoomById(ObjectId roomId) {
       return roomCoreDao.getRoomById(roomId);
    }

    public List<String> filterBlack(List<String> objectIdList,ObjectId roomId) {
        if (CollectionUtil.isEmpty(objectIdList)){
            return objectIdList;
        }
        return roomMemberCoreDao.getListByNotInBlack(objectIdList,roomId,1);
    }
}
