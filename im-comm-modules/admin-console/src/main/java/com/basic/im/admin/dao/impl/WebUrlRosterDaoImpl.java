package com.basic.im.admin.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.admin.dao.WebUrlRosterDao;
import com.basic.im.admin.entity.WebUrlRoster;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.repository.MongoRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;


@Repository
public class WebUrlRosterDaoImpl extends MongoRepository<WebUrlRoster, ObjectId> implements WebUrlRosterDao {


    @Override
    public void addWebUrlRoster(WebUrlRoster webUrlRoster) {
        getDatastore().save(webUrlRoster);
    }


    @Override
    public Byte queryWebUrlType(String webUrl) {
        Query query = createQuery().addCriteria(contains("webUrl",webUrl));
        WebUrlRoster webUrlRoster = findOne(query);
        return (null == webUrlRoster) ? 0 : webUrlRoster.getUrlType();
    }


    @Override
    public PageResult<WebUrlRoster> findWebUrlRosterList(String webUrl,byte urlType, int page, int limit) {
        PageResult<WebUrlRoster> result = new PageResult<>();
        Query query = createQuery();
        if(!StringUtil.isEmpty(webUrl)){
            query.addCriteria(contains("webUrl",webUrl));
        }
        if (0!=urlType){
            query.addCriteria( Criteria.where("urlType").is(urlType));
        }
        result.setCount(count(query));
        result.setData(queryListsByQuery(query,page,limit));
        return result;
    }




    @Override
    public Class<WebUrlRoster> getEntityClass() {
        return WebUrlRoster.class;
    }
}
