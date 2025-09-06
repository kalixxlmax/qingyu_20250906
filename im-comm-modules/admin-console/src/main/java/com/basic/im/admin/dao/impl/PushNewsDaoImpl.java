package com.basic.im.admin.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.admin.dao.PushNewsDao;
import com.basic.im.admin.entity.PushNews;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.repository.MongoRepository;
import com.basic.utils.DateUtil;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

/**
 * @ClassName PushConfigDaoImpl
 * @Author xie yuan yuang
 * @date 2020.08.03 12:25
 * @Description
 */
@Repository
public class PushNewsDaoImpl extends MongoRepository<PushNews, ObjectId> implements PushNewsDao {


    @Override
    public Class<PushNews> getEntityClass() {
        return PushNews.class;
    }


    @Override
    public void sava(PushNews pushNews) {
        save(pushNews);
    }

    @Override
    public PageResult<PushNews> getPushNewsList(String startDate, String endDate, int page, int limit, String type,String content) {
        PageResult<PushNews> result = new PageResult<>();
        Query query=createQuery();
        if(!StringUtil.isEmpty(type)){
            query.addCriteria(Criteria.where("type").is(Integer.valueOf(type)));
        }
        if (!StringUtil.isEmpty(content)){
            query.addCriteria(Criteria.where("content").regex(content));
        }
        descByquery(query,"createTime");
        long startTime = 0; //开始时间（秒）
        long endTime = 0; //结束时间（秒）,默认为当前时间
        if(!com.basic.utils.StringUtil.isEmpty(startDate) && !com.basic.utils.StringUtil.isEmpty(endDate)) {
            startTime = com.basic.utils.StringUtil.isEmpty(startDate) ? 0 : DateUtil.toDate(startDate).getTime()/1000;
            endTime = com.basic.utils.StringUtil.isEmpty(endDate) ? DateUtil.currentTimeSeconds() : DateUtil.toDate(endDate).getTime()/1000;
        }
        if(startTime!=0 && endTime!=0){
            query.addCriteria(Criteria.where("createTime").gt(startTime).lt(endTime));
        }
        result.setCount(count(query));
        query.with(createPageRequest(page-1,limit));
        result.setData(queryListsByQuery(query));
        return result;
    }

    @Override
    public void deletePushNews(ObjectId id) {
        deleteById(id);
    }
}
