package com.basic.im.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.dao.GiveGiftDao;
import com.basic.im.entity.Givegift;
import com.basic.im.repository.MongoRepository;
import com.basic.utils.DateUtil;
import com.basic.utils.StringUtil;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
@Repository
public class GiveGiftDaoImpl extends MongoRepository<Givegift, ObjectId> implements GiveGiftDao {


    @Override
    public Class<Givegift> getEntityClass() {
        return Givegift.class;
    }

    @Override
    public void addGiveGift(Givegift givegift) {
        getDatastore().save(givegift);
    }

    @Override
    public void addGiveGiftList(List<Givegift> list) {
        getDatastore().save(list);
    }

    @Override
    public List<Givegift> getGiveGiftList(int userId,int toUserId ,int pageIndex, int pageSize,int type) {
        List<Givegift> list ;
        Query query = createQuery();
        if(0 != userId){
            addToQuery(query,"userId",userId);
        }
        if(0 != toUserId){
            addToQuery(query,"toUserId",userId);
        }

        if(0 != type){
            list = queryListsByQuery(query,pageIndex,pageSize,type);
        }else{

            list = queryListsByQuery(query,pageIndex,pageSize);
        }
        return list;
    }

    @Override
    public List<Givegift> getGiveGiftList(long startTime, long endTime, int pageIndex, int pageSize, int type) {
        Query query = createQuery();
        query.addCriteria(Criteria.where("time").gt(startTime).lte(endTime));

        return queryListsByQuery(query,pageIndex,pageSize,type);
    }

    @Override
    public PageResult<Givegift> getGivegift(int userId, String startDate, String endDate, Integer page, Integer limit) {
        PageResult<Givegift> result=new PageResult<Givegift>();
        Query query;
        if(StringUtil.isEmpty(startDate) && StringUtil.isEmpty(endDate)){
			query = createQuery("toUserId", userId);
        }else{
            long startTime = 0; //开始时间（秒）
            long endTime = 0; //结束时间（秒）,默认为当前时间
            startTime = StringUtil.isEmpty(startDate) ? 0 : DateUtil.toDate(startDate).getTime()/1000;
            DateUtil.getTodayNight();
            endTime = StringUtil.isEmpty(endDate) ? DateUtil.currentTimeSeconds() : DateUtil.toDate(endDate).getTime()/1000;
            long formateEndtime = com.basic.im.comm.utils.DateUtil.getOnedayNextDay(endTime,1,0);
			query =createQuery();
            query.addCriteria(Criteria.where("time").gt(startTime).lte(formateEndtime));
        }
        result.setCount(count(query));
        result.setData(queryListsByQuery(query,page,limit,1));
        return result;
    }

    @Override
    public List<Givegift> find(ObjectId msgId, ObjectId giftId, int pageIndex, int pageSize) {
        Query query=createQuery("msgId",msgId);
        descByquery(query,"_id");

        return queryListsByQuery(query,pageIndex,pageSize);
    }
}
