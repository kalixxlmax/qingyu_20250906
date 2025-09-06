package com.basic.mongodb.morphia;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "mongoconfig")
public class MongoConfig {

    /**
     * 数据库链接  127.0.0.1:27017,127.0.0.2:28018
     */
    private String uri;

    private String dbName;
    private String roomDbName;
    //配置是否使用集群模式   读写分离    0 单机 模式     1：集群模式
    private int cluster = 0;
    private String username;
    private String password;

    private String mapPackage;


    private int connectTimeout=20000;
    private int socketTimeout=20000;
    private int maxWaitTime=20000;


}
