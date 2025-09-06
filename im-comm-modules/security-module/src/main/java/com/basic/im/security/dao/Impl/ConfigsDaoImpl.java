package com.basic.im.security.dao.Impl;

import com.basic.im.entity.Config;
import com.basic.im.repository.MongoRepository;
import com.basic.im.security.dao.ConfigsDao;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2020/3/4
 **/
@Repository
public class ConfigsDaoImpl extends MongoRepository<Config, Long> implements ConfigsDao {
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
    public void addConfig(Config config) {
        getDatastore().save(config);
    }
}
