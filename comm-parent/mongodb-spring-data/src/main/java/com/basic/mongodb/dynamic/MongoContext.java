package com.basic.mongodb.dynamic;

import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.SimpleMongoClientDbFactory;
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import java.util.HashMap;
import java.util.Map;

public class MongoContext {

    private static final Map<String, MongoDbFactory> MONGO_CLIENT_DB_FACTORY_MAP = new HashMap<>();
    private static final ThreadLocal<MongoDbFactory> MONGO_DB_FACTORY_THREAD_LOCAL = new ThreadLocal<>();

    public static void setMongoDbFactory(String name) {
        MONGO_DB_FACTORY_THREAD_LOCAL.set(MONGO_CLIENT_DB_FACTORY_MAP.get(name));
    }

    public static MongoDbFactory getMongoDbFactory() {
        return MONGO_DB_FACTORY_THREAD_LOCAL.get();
    }

    public static void removeMongoDbFactory(){
        MONGO_DB_FACTORY_THREAD_LOCAL.remove();
    }


    public static MongoDbFactory getDefaultFactory(){
        return MONGO_CLIENT_DB_FACTORY_MAP.get(DataSourceNames.HOT);
    }

    public static DynamicMongoTemplate initDynamicMongoTemplate() {
        DynamicMongoTemplate dynamicMongoTemplate = new DynamicMongoTemplate(MONGO_CLIENT_DB_FACTORY_MAP.get(DataSourceNames.HOT));
        MongoConverter converter = dynamicMongoTemplate.getConverter();
        if (converter.getTypeMapper().isTypeKey("_class")) {
            ((MappingMongoConverter) converter).setTypeMapper(new DefaultMongoTypeMapper(null));
        }
        return dynamicMongoTemplate;
    }

    public static void putClient(String name,SimpleMongoClientDbFactory simpleMongoClientDbFactory){
        MONGO_CLIENT_DB_FACTORY_MAP.put(name,simpleMongoClientDbFactory);
    }
}