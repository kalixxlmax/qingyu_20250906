package com.basic.im.admin.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.admin.config.MongoConfig;
import com.basic.im.admin.dao.UploadItemeDao;
import com.basic.im.admin.entity.UploadItem;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.utils.SKBeanUtils;
import com.basic.mongodb.springdata.BaseMongoRepository;
import com.basic.utils.DateUtil;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

/**
 * @ClassName ResourceDaoImpl
 * @Author xie yuan yang
 * @date 2020.10.20 12:22
 * @Description
 */
@Repository
public class UploadItemDaoImpl extends BaseMongoRepository<UploadItem, ObjectId> implements UploadItemeDao {

    @Autowired(required = false)
    @Qualifier(value = "mongoTemplateForResource")
    private MongoTemplate mongoTemplateForResource;


    @Override
    public MongoTemplate getDatastore() {
        String resourceDatabaseUrl = SKBeanUtils.getImCoreRepository().getConfig().getResourceDatabaseUrl();
        if (!com.basic.utils.StringUtil.isEmpty(resourceDatabaseUrl)){
            return MongoConfig.mongoTemplateForResource();
        }
        return mongoTemplateForResource;
    }

    @Override
    public Class<UploadItem> getEntityClass() {
        return UploadItem.class;
    }

    @Override
    public PageResult<UploadItem> resourcelist(int page, int limit, String keyword, String fileType, String startDate, String endDate) {
        PageResult<UploadItem> result = new PageResult<UploadItem>();
        Query query = createQuery();
        if (!StringUtil.isEmpty(keyword)){
            query.addCriteria(Criteria.where("fileName").regex(keyword));
        }
        if (!StringUtil.isEmpty(fileType)){
            query.addCriteria(Criteria.where("fileType").regex(fileType));
        }
        long startTime = 0; //开始时间（秒）
        long endTime = 0; //结束时间（秒）,默认为当前时间
        if(!com.basic.utils.StringUtil.isEmpty(startDate) && !com.basic.utils.StringUtil.isEmpty(endDate)) {
            startTime = com.basic.utils.StringUtil.isEmpty(startDate) ? 0 : DateUtil.toDate(startDate).getTime()/1000;
            endTime = com.basic.utils.StringUtil.isEmpty(endDate) ? DateUtil.currentTimeSeconds() : DateUtil.toDate(endDate).getTime()/1000;

        }
        if(startTime!=0 && endTime!=0){
            query.addCriteria(Criteria.where("createTime").gt(startTime).lt(endTime));
        }
        long count = getDatastore().count(query, getEntityClass());
        result.setCount(count);
        query.with(createPageRequest(page, limit, 1));
        result.setData(queryListsByQuery(query));
        return result;
    }

    @Override
    public void deleteResource(ObjectId id) {
        deleteById(id);
    }

    @Override
    public void deleteResource(String url) {
        Query query = createQuery("fileName",url);
        deleteByQuery(query);
    }
}
