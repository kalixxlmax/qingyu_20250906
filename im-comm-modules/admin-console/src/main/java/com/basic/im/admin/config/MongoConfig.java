package com.basic.im.admin.config;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.basic.im.utils.SKBeanUtils;
import com.basic.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;

/**
 * @ClassName MongoConfig
 * @Author xie yuan yang
 * @date 2020.11.02 11:33
 * @Description
 */
public class MongoConfig {

    @Autowired(required=false)
    private com.basic.mongodb.springdata.MongoConfig mongoConfig;

    private MongoClient imRoomMongoClient;

    /**
     *  获取资源数据库 MongoTemplate
     **/
    public static MongoTemplate mongoTemplateForResource() {
        String resourceDatabaseUrl = SKBeanUtils.getImCoreRepository().getConfig().getResourceDatabaseUrl();

        MongoClient imRoomMongoClient = new MongoClient(new MongoClientURI(resourceDatabaseUrl));
        MongoTemplate mongoTemplateForRoom = new MongoTemplate(imRoomMongoClient,"resources");
        return mongoTemplateForRoom;
    }
}
