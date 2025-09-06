package com.basic.mongodb.springdata;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import static com.basic.mongodb.springdata.MongoConfig.PREFIX;

@Data
@Configuration
@ConfigurationProperties(PREFIX)
public class MongoConfig {

    public static final String PREFIX = "mongoconfig";

    /**
     * 数据库链接
     */
    private String uri = "mongodb://127.0.0.168:28018";

    /**
     * 数据库名称
     */
    private String dbName = "imapi";

    /**
     * 群组数据库名称
     */
    private String roomDbName = "imRoom";

    /**
     * 是否使用集群模式
     */
    private int cluster = 0;

    /**
     * 数据库用户名称
     */
    private String username = "";

    /**
     * 数据库密码
     */
    private String password = "";

    private String mapPackage;

    /**
     * 连接超时时间,单位毫秒
     */
    private int connectTimeout=20000;

    /**
     * 套接字超时，单位毫秒
     */
    private int socketTimeout=20000;

    /**
     * 最大等待时间，单位毫秒
     */
    private int maxWaitTime=20000;

    /**
     * 冷数据库连接地址
     */
    private String coldUri = "mongodb://localhost:28018/cold";


    /**
     * 是否启用事务处理
     */
    private boolean transaction = false;

    /**
     * 是否启用冷热分离
     */
    private boolean separate = false;
}
