package com.basic.im.repository;

import com.basic.im.entity.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class IMCoreRepository extends MongoRepository<Object,Integer>{

    @Override
    public Class<Object> getEntityClass() {
        return null;
    }

    public Config getConfig() {
        Query query=createQuery();
        query.addCriteria(Criteria.where("_id").ne(null));
        return getDatastore().findOne(query,Config.class);
    }

    public void setConfig(Config config) {

         saveEntity(config);
    }

    public void setClientConfig(ClientConfig config) {

        saveEntity(config);
    }

    public ClientConfig getClientConfig() {
        Query query=createQuery();
        query.addCriteria(Criteria.where("_id").is(10000));
        return getDatastore().findOne(query,ClientConfig.class);
    }

    public void setPayConfig(PayConfig payConfig){
        saveEntity(payConfig);
    }

    public PayConfig getPayConfig(){
        Query query = createQuery();
        query.addCriteria(Criteria.where("_id").is(10000));
        return getDatastore().findOne(query,PayConfig.class);
    }

    public List<PushConfig> initPushConfig(){
        Query query = createQuery();
        return getDatastore().find(query,PushConfig.class);
    }

    public void initDBPushConfig(PushConfig pushConfig){
       getDatastore().save(pushConfig);
    }

    public void setSmsConfig(SmsConfig smsConfig){
        saveEntity(smsConfig);
    }
    public SmsConfig getSmsConfig(){
        Query query = createQuery();
        query.addCriteria(Criteria.where("_id").is(10000));
        return getDatastore().findOne(query,SmsConfig.class);
    }


}
