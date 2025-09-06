package com.basic.mongodb.strategy;

import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDbFactory;
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import java.util.HashMap;
import java.util.Map;


public class MongoStrategyContext {

   /* private static final Map<String, MongoDbFactory> MONGO_STRATEGY_FACTORY_MAP = new HashMap<>();


    public static MongoDbFactory getMongoDbFactory(String moduleId) {
        return MONGO_STRATEGY_FACTORY_MAP.get(moduleId);
    }

    public static void putMongoDbFactory(String moduleId,MongoDbFactory factory) {
        MONGO_STRATEGY_FACTORY_MAP.put(moduleId,factory);
    }

    public static void removeMongoDbFactory(String moduleId){
        MONGO_STRATEGY_FACTORY_MAP.remove(moduleId);
    }
    public static void putClient(String moduleId,SimpleMongoClientDbFactory simpleMongoClientDbFactory){
        MONGO_STRATEGY_DATASOURCE_MAP.put(moduleId,simpleMongoClientDbFactory);
    }*/

    /**
     *
      moduleId:MongoTemplate
     *
     */
    private static final Map<String,MongoTemplate> MONGO_STRATEGY_DATASOURCE_MAP = new HashMap<>();

    public static MongoTemplate getMongoDataSource(String moduleId) {
        return MONGO_STRATEGY_DATASOURCE_MAP.get(moduleId);
    }

    public static void putMongoDataSource(String moduleId,MongoTemplate mongoTemplate) {
        MONGO_STRATEGY_DATASOURCE_MAP.put(moduleId,mongoTemplate);
    }



    public static MongoTemplate initMongoTemplate(String moduleId,MongoDbFactory factory) {
        MongoTemplate mongoTemplate = new MongoTemplate(factory);
        MongoConverter converter = mongoTemplate.getConverter();
        if (converter.getTypeMapper().isTypeKey("_class")) {
            ((MappingMongoConverter) converter).setTypeMapper(new DefaultMongoTypeMapper(null));
        }
        return mongoTemplate;
    }


}