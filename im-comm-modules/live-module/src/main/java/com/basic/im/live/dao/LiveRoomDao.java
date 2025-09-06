package com.basic.im.live.dao;

import com.basic.common.model.PageResult;
import com.basic.im.live.entity.LiveRoom;
import com.basic.im.repository.IMongoDAO;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;

public interface LiveRoomDao extends IMongoDAO<LiveRoom, ObjectId> {

    void addLiveRoom(LiveRoom entity);

    Object addLiveRoomReturn(LiveRoom liveRoom);

    LiveRoom getLiveRoom(ObjectId roomId);

    LiveRoom getLiveRoomByUserId(int userId);

    LiveRoom getLiveingByUserId(int userId);

    List<LiveRoom> queryLiveRoomList(int userId,Integer pageIndex,Integer pageSize);

    List<LiveRoom> queryLiveRoomList(int userId);

    LiveRoom getLiveRoomByJid(String jid);

    LiveRoom getLiveRoomByLiveRoomId(String liveRoomId);

    void updateLiveRoom(ObjectId roomId, int userId, Map<String, Object> map);

    void updateLiveRoomNum(ObjectId roomId, int number);

    void addRewardCount(ObjectId roomId, double count);

    void addShareCount(ObjectId roomId);

    void updateProductId(ObjectId roomId, String productId);

    void deleteLiveRoom(ObjectId roomId);

    PageResult<LiveRoom> findLiveRoomList(String name, String nickName, int userId, int status, int pageIndex, int pageSize, int type);

    PageResult<Document> findLiveRoomListByUserId(String userId, int pageIndex, int pageSize);

    void clearLiveRoom();

    PageResult<LiveRoom> getLiveRoomList(long time);

    void updateLiveRoom(int userId, Map<String, Object> map);

    void updateProductIdList(int userId,ObjectId roomId, List<String> productIdList);

    void updateLiveRoomStatus(ObjectId roomId,Integer status);

    void addClickProductCount(Integer userId, ObjectId roomId);

    void updateOrderStatistics(ObjectId roomId,int orderNum,double orderMoney);

    int liveCount();

}
