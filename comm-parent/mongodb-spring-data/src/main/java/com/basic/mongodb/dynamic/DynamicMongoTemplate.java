package com.basic.mongodb.dynamic;

import com.mongodb.client.MongoDatabase;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;

public class DynamicMongoTemplate extends MongoTemplate {

    public DynamicMongoTemplate(MongoDbFactory mongoDbFactory){
        super(mongoDbFactory);
    }

    @Override
    protected MongoDatabase doGetDatabase() {
        MongoDbFactory mongoDbFactory = MongoContext.getMongoDbFactory();
        return mongoDbFactory == null ? super.doGetDatabase() : mongoDbFactory.getDb();
    }
}