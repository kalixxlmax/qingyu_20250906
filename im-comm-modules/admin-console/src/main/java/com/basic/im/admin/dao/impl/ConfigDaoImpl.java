package com.basic.im.admin.dao.impl;

import com.basic.im.admin.dao.ConfigDao;
import com.basic.im.entity.Config;
import com.basic.im.entity.SystemApiConfig;
import com.basic.im.repository.MongoRepository;
import com.basic.im.utils.SKBeanUtils;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;


/**
 * @author zhm
 * @version V1.0
 * @Description: TODO(todo)
 * @date 2019/9/3 12:30
 */
@Repository
public class ConfigDaoImpl extends MongoRepository<Config, Long> implements ConfigDao {

    @Override
    public Class<Config> getEntityClass() {
        return Config.class;
    }

    @Override
    public Config getConfig() {
        Query query=createQuery();
        query.addCriteria(Criteria.where("_id").ne(null));
        return findOne(query);
    }

    @Override
    public void updateConfig(int isOpenPrivacyPosition){
        Query query=createQuery().addCriteria(Criteria.where("_id").ne(null));
        Update ops = createUpdate();
        ops.set("isOpenPrivacyPosition", isOpenPrivacyPosition);
        Config config;
        //FindAndModifyOptions find = new FindAndModifyOptions();
        config = getDatastore().findAndModify(query, ops,new FindAndModifyOptions().returnNew(true),getEntityClass());
        SKBeanUtils.getImCoreService().setConfig(config);
    }

    @Override
    public void addConfig(Config config) {
        getDatastore().save(config);
    }

    @Override
    public void setSystemApiConfig(SystemApiConfig systemApiConfig){
        Query query=createQuery();
        query.addCriteria(Criteria.where("_id").ne(null));
        Update update = createUpdate().set("systemApiConfig", systemApiConfig);
        updateFirst(query,update);
    }
    @Override
    public SystemApiConfig querySystemApiConfig(){
        Query query=createQuery();
        query.addCriteria(Criteria.where("_id").ne(null));
        Config config = findOne(query);
        if(null==config){
            return null;
        }
        return config.getSystemApiConfig();
    }
}
