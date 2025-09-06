package com.basic.im.dao;

import com.basic.common.model.PageResult;
import com.basic.im.entity.Gift;
import com.basic.im.repository.IMongoDAO;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;

public interface GiftDao extends IMongoDAO<Gift, ObjectId> {

    void addGift(Gift gift);

    Gift getGift(ObjectId giftId);

    void deleteGift(ObjectId giftId);

    List<Gift> getGiftList(String name, int pageIndex, int pageSize);

    Map<String,Object> getGiftListMap(String name, int pageIndex, int pageSize);

    List<Document> findByUser(ObjectId msgId);

    List<Document> findByGift(ObjectId msgId);

    PageResult<Gift> findGiftList(String name, int pageIndex, int pageSize);
}
