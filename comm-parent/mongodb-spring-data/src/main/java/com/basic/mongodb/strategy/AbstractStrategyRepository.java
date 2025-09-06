package com.basic.mongodb.strategy;

import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.basic.mongodb.springdata.BaseMongoRepository;
import com.basic.utils.StringUtil;
import org.bson.Document;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import java.io.Serializable;

/**
 * AbstractStrategyRepository <br>
 *
 * @author: chat  <br>
 * @date: 2020/11/19 0019  <br>
 */
public abstract class AbstractStrategyRepository<T, ID extends Serializable> extends BaseMongoRepository<T,ID> implements IStrategyRepository, InitializingBean,ApplicationRunner {



    private MongoTemplate datastore;

    private int dbStrategy;

    private String dbname;

    private String moduleId;




    protected final String SPLIT="_";

    @Autowired
    protected Environment environment;
    /**
     * 获取分表前缀
     * @return
     */
    @Override
    public String getDBNamePrefix(){
        return getCollectionName()+SPLIT;
    }


    public MongoDatabase getMongoDatabase(){
        return getDatastore().getDb();
    }

    @Override
    public String getCollectionName(long userId){
        long remainder=0;
        if(userId>IStrategyRepository.MIN_USERID) {
            remainder=userId% getDbStrategy();
        }
        return getDBNamePrefix()+remainder;
    }

    @Override
    public String getCollectionName(int userId){
        long remainder=0;
        if(userId>IStrategyRepository.MIN_USERID) {
            remainder=userId% getDbStrategy();
        }
        return getDBNamePrefix()+remainder;
    }

    @Override
    public MongoCollection<Document> getCollection(MongoDatabase database, long userId){
        String collectionName = getCollectionName(userId);
        return database.getCollection(collectionName);
    }
    @Override
    public MongoCollection<Document> getCollection(MongoDatabase database, int userId){
        String collectionName = getCollectionName(userId);
        return database.getCollection(collectionName);
    }





    @Override
    public String getCollectionName(String userId){
        return getCollectionName(Long.parseLong(userId));
    }


    @Override
    public MongoCollection<Document> getCollection(MongoDatabase database, String userId){
        return getCollection(database,Long.parseLong(userId));
    }

    @Override
    public MongoCollection<Document> getCollection(long userId){
        String collectionName = getCollectionName(userId);
        return getDatastore().getCollection(collectionName);
    }

    @Override
    public MongoTemplate initRepository(String moduleId,String uri, String database, int dbStrategy) {
        MongoTemplate dataSource=getDatastore();
        setModuleId(moduleId);
        setDbStrategy(dbStrategy);
        setDbName(database);

        if(!StringUtil.isEmpty(uri)){
            try {
                MongoClient  mongoClient = MongoClients.create(uri);
                dataSource=new MongoTemplate(mongoClient,database);

            }catch (Exception e){
                logger.error("initRepository error {}  {}",uri,database);
                logger.error(e.getMessage(),e);
                dataSource=super.getDatastore();
            }

        }else {
            if(null!=dataSource){
                if(dataSource.getDb().getName().equals(database)){
                    return dataSource;
                }
                dataSource=MongoStrategyContext.getMongoDataSource(moduleId);
                if(dataSource.getDb().getName().equals(database)){
                    return dataSource;
                }
            }else {
                dataSource=new MongoTemplate(getMongoClient(),database);
            }

        }

        setDatastore(dataSource);
        MongoConverter converter = getDatastore().getConverter();
        if (converter.getTypeMapper().isTypeKey("_class")) {
            ((MappingMongoConverter) converter).setTypeMapper(new DefaultMongoTypeMapper(null));
        }
        MongoStrategyContext.putMongoDataSource(moduleId,dataSource);
        //MongoStrategyContext.putMongoDbFactory(moduleId);
        return dataSource;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        logger.info("afterPropertiesSet {} ",this.getClass().getName());

        Meta annotation;

        String moduleId;


        String mongoUri;
        String dbname;

        try {
            Class realClass=  this.getClass();

            if(!realClass.isAnnotationPresent(Meta.class)){
                return ;
            }
            annotation = (Meta) realClass.getAnnotation(Meta.class);
            logger.info("StrategyRepository load: " + realClass);
            int defDbStrategy = annotation.dbStrategy();
            if(0==defDbStrategy){
                defDbStrategy=IStrategyRepository.DEFULT_DB_STRATEGY;
            }
            moduleId=annotation.moduleId();

            mongoUri=getUriPropKey(moduleId);
            dbname=getDatabasePropKey(moduleId);
            if(StringUtil.isEmpty(dbname)){
                dbname=annotation.dbname();
            }
            int dbStrategy=getDbStrategyPropKey(moduleId);
            if(0==dbStrategy){
                dbStrategy=defDbStrategy;
            }
            this.initRepository(moduleId,mongoUri,dbname,dbStrategy);
            DefultMongoRepositoryStrategy
                    .getDbStrategy(getMongoClient(),dbname,defDbStrategy,dbStrategy);
        }catch (Exception e){
            logger.error(e.getMessage(),e);
        }

    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        logger.info("startup end initCreateIndex  {}",this.getClass().getName());
        initCreateIndex();
    }


    public  String getUriPropKey(String propModule){
        return getPropertyValue(propModule,PROP_DB_URI);
    }

    public  String getDatabasePropKey(String propModule){
        return getPropertyValue(propModule,PROP_DATABASE);
    }
    public  int getDbStrategyPropKey(String propModule){
        String propKey = getPropertyValue(propModule, PROP_DB_STRATEGY);
        if(StringUtil.isEmpty(propKey)){
            return 0;
        }else {
            return  Integer.parseInt(propKey);
        }
    }

    public  String getPropKey(String propModule,String suffix){
        return PROP_PREFIX+PROP_SPLIT+propModule+PROP_SPLIT+suffix;
    }

    public  String getPropertyValue(String propModule,String suffix){
        return environment.getProperty(getPropKey(propModule,suffix),"");
    }


    @Override
    public int getDbStrategy() {
        return dbStrategy;
    }

    @Override
    public void setDbStrategy(int dbStrategy) {
        this.dbStrategy=dbStrategy;
    }

    @Override
    public String getDbName() {
        return dbname;
    }

    @Override
    public void setDbName(String dbName) {
        this.dbname=dbName;
    }


    @Override
    public MongoTemplate getDatastore() {
        return  datastore;
    }

    @Override
    public void setDatastore(MongoTemplate datastore) {
        this.datastore=  datastore;
    }
    @Override
    public String getModuleId() {
        return moduleId;
    }

    @Override
    public void setModuleId(String moduleId) {
        this.moduleId = moduleId;
    }



}
