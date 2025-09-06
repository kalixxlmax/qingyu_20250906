package com.basic.mongodb.springdata;

import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.basic.mongodb.dynamic.DataSourceNames;
import com.basic.mongodb.dynamic.DynamicMongoTemplate;
import com.basic.mongodb.dynamic.MongoContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.SimpleMongoClientDbFactory;


@Slf4j
@Configuration
@EnableConfigurationProperties(MongoConfig.class)
public class MongoConfiguration implements InitializingBean  {

    @Autowired
    private MongoConfig mongoConfig;

    @Bean
    @ConditionalOnProperty(name="mongoConfig.transaction",havingValue = "true")
    public MongoTransactionManager transactionManager(MongoDbFactory dbFactory) {
        return new MongoTransactionManager(dbFactory);
    }

    @Primary
    @Bean(name = "mongoTemplate")
    public DynamicMongoTemplate dynamicMongoTemplate() {
        return MongoContext.initDynamicMongoTemplate();
    }

    @Bean(name = "mongoClient",destroyMethod = "close")
    public com.mongodb.MongoClient mongoClient() {
        com.mongodb.MongoClient mongoClient;
        try {
            log.info(" init mongoClient  uri {} dbname {} ", mongoConfig.getUri(),mongoConfig.getDbName());
            MongoClientURI mongoClientURI=new MongoClientURI(mongoConfig.getUri());
            // MongoCredential credential = null;
            // if(!StringUtil.isEmpty(mongoConfig.getUsername())&&!StringUtil.isEmpty(mongoConfig.getPassword())){
            //     credential = MongoCredential.createScramSha1Credential(mongoConfig.getUsername(), mongoConfig.getDbName(), mongoConfig.getPassword().toCharArray());
            // }
            mongoClient = new com.mongodb.MongoClient(mongoClientURI);
            return mongoClient;
        } catch (Exception e) {
            log.error(e.getMessage(),e);
            return null;
        }
    }

    @Bean(name = "mongoDbFactory")
    public MongoDbFactory mongoDbFactory() {
        return  MongoContext.getDefaultFactory();
    }

    @Override
    public void afterPropertiesSet() {
        MongoContext.putClient(DataSourceNames.HOT,new SimpleMongoClientDbFactory(getHotMongoClient(), mongoConfig.getDbName()));
        // 判断是否启用冷库
        if (mongoConfig.isSeparate()){
            MongoContext.putClient(DataSourceNames.COLD, new SimpleMongoClientDbFactory(mongoConfig.getColdUri()));
        }
    }

    public MongoClient getHotMongoClient() {
        MongoClient mongoClient;
        try {
            log.info(" init getHotMongoClient  uri {} dbname {} ", mongoConfig.getUri(),mongoConfig.getDbName());
            /*MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
            builder.maxWaitTime(mongoConfig.getMaxWaitTime());
            builder.connectTimeout(mongoConfig.getConnectTimeout());
            builder.socketTimeout(mongoConfig.getSocketTimeout());*/
            mongoClient = MongoClients.create(mongoConfig.getUri());
            return mongoClient;
        } catch (Exception e) {
            return null;
        }
    }
}
