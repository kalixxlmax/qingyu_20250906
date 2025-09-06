package com.basic.im.msg.service;

import com.mongodb.DBObject;
import com.basic.im.entity.Gift;
import com.basic.im.entity.Givegift;
import com.basic.im.msg.model.AddGiftParam;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.List;

public interface MsgGiftManager {

    List<ObjectId> add(Integer userId, ObjectId msgId, List<AddGiftParam> paramList);

    List<Gift> getGiftList();

    List<Givegift> find(ObjectId msgId, ObjectId giftId, int pageIndex, int pageSize);

    List<Document> findByUser(ObjectId msgId);

    List<Document> findByGift(ObjectId msgId);
}
