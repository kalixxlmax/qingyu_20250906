package com.basic.mongodb.strategy;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.basic.utils.StringUtil;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 数据库分表路由策略接口 <br>
 *
 * @date: 2020/11/14 0014  <br>
 * @author: chat  <br>
 */
public interface IStrategyRepository {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @interface Meta {

        String moduleId() default "";

        String dbname()  default "";
        /**
         * 默认分表策略数量
         * @return
         */
        int dbStrategy() default 100;

        /*boolean isDefault() default false;
        String[] supportedUris();*/
    }


    int MIN_USERID=100000;

    int DEFULT_DB_STRATEGY=100;



    int getDbStrategy();

    void setDbStrategy(int dbStrategy);

    String getDbName();

    void setDbName(String dbName);

    MongoTemplate getDatastore();

    void setDatastore(MongoTemplate datastore);


    String getDBNamePrefix();


    String getModuleId();

    void setModuleId(String moduleId);

    MongoTemplate initRepository(String moduleId, String uri, String database, int dbStrategy);

    /**
     * 分表检查创建索引
     */
    void initCreateIndex();

   /*default MongoDatabase initRepository(String uri,String database,int dbStrategy){


   }*/

    default String getCollectionName(long userId){
        long remainder=0;
        if(userId>MIN_USERID) {
            remainder=userId% getDbStrategy();
        }
        return getDBNamePrefix()+remainder;
    }
    default String getCollectionName(int userId){
        long remainder=0;
        if(userId>MIN_USERID) {
            remainder=userId% getDbStrategy();
        }
        return getDBNamePrefix()+remainder;
    }


    default MongoCollection<Document> getCollection(MongoDatabase database, long userId){
        String collectionName = getCollectionName(userId);
        return database.getCollection(collectionName);
    }





    default MongoCollection<Document> getCollection(MongoDatabase database, int userId){
        String collectionName = getCollectionName(userId);
        return database.getCollection(collectionName);
    }
    default String getCollectionName(String userId){
        return getCollectionName(Long.parseLong(userId));
    }



    default MongoCollection<Document> getCollection(MongoDatabase database, String userId){
       return getCollection(database,Long.parseLong(userId));
    }





     String PROP_DB_STRATEGY="dbStrategy";

     String PROP_DB_URI="uri";

     String PROP_DATABASE="database";

     String PROP_SPLIT=".";


     String PROP_PREFIX="mongoconfig";

     String DB_STRATEGY_NAME="db_strategy";

     class  DefultMongoRepositoryStrategy{


         public static int getDbStrategy(MongoClient mongoClient, String dbname, int defultStrategy, int porpStrategy){
             MongoCollection<Document> collection = mongoClient.getDatabase(DB_STRATEGY_NAME).getCollection(DB_STRATEGY_NAME);
             Document query=new Document("dbname",dbname);
             Document document = collection.find(query).first();
             if(0<porpStrategy){
                 /**
                  * 配置了,用配置的值
                  */
                  defultStrategy=porpStrategy;
             }
             if (null==document){
                 /**
                  * 数据库没有记录
                  */
                 Document values=new Document("dbname",dbname);
                 values.append(DB_STRATEGY_NAME,defultStrategy);
                 collection.insertOne(values);
                 return defultStrategy;

             }
             Integer integer = document.getInteger(DB_STRATEGY_NAME);
             if(null==integer){
                 Document values=new Document(DB_STRATEGY_NAME,defultStrategy);
                 collection.updateOne(query,new Document("$set",values));
                 return defultStrategy;
             }
             if(defultStrategy==integer.intValue()){
                 /**
                  * 数据库记录的和配置的一样
                  */
                 return integer.intValue();
             }else {
                 /**
                  * 不一样的查询一下数据库是否有写入值,
                  * 数据库如果有记录了就用数据库里面的值
                  * 没有值的更新为配置里面的值
                  */
                 if(isExistData(mongoClient.getDatabase(dbname))){
                     System.out.println("分表数据配置错误： 数据库已有数据更新失败 "+dbname);
                     return integer.intValue();
                 }else {
                     /**
                      * 没有数据更新为配置中的分表策略
                      */
                     Document values=new Document(DB_STRATEGY_NAME,defultStrategy);
                     collection.updateOne(query,new Document("$set",values));
                     return defultStrategy;
                 }
             }
         }

         public static boolean isExistData(MongoDatabase database){
             MongoIterable<String> collectionNames = database.listCollectionNames();
             for (String name : collectionNames) {
                 if(database.getCollection(name).countDocuments()>0){
                     return true;
                 }
             }
             return false;
         }

         /*public static int getDbStrategy(MongoDatabase database, int defultStrategy, int porpStrategy){
             if(0==porpStrategy){
                 return defultStrategy;
             }
             MongoIterable<String> collectionNames = database.listCollectionNames();
             for (String name : collectionNames) {
                 if(database.getCollection(name).countDocuments()>0){
                     return defultStrategy;
                 }
             }

             return defultStrategy;

         }*/

     }

}
