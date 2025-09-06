package com.basic.im.admin.dao.impl;

import com.mongodb.client.result.DeleteResult;
import com.basic.im.admin.dao.PushConfigDao;
import com.basic.im.entity.PushConfig;
import com.basic.im.repository.MongoRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @ClassName PushConfigDaoImpl
 * @Author xie yuan yuang
 * @date 2020.08.03 12:25
 * @Description
 */
@Repository
public class PushConfigDaoImpl extends MongoRepository<PushConfig, Integer> implements PushConfigDao {


    @Override
    public Class<PushConfig> getEntityClass() {
        return PushConfig.class;
    }


    @Override
    public List<PushConfig> getPushConfigList() {
         Query query = createQuery();
         query.addCriteria(Criteria.where("_id").ne(10000));
         return queryListsByQuery(query);
    }

    @Override
    public PushConfig addPushConfig(PushConfig pushConfig) {
        return save(pushConfig);
    }


    @Override
    public PushConfig getPushConfigModelDetail(int id) {
        Query query = createQuery(id);
        return findOne(query);
    }

    @Override
    public boolean deletePushConfig(int id) {
        DeleteResult result = deleteById(id);
        if (result.getDeletedCount() > 0 ){
            return  true;
        }else{
            return  false;
        }
    }
}
