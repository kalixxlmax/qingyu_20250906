package com.basic.mongodb;

import com.mongodb.client.ClientSession;
import com.basic.mongodb.springdata.BaseMongoRepository;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class TransactionServiceDemo extends BaseMongoRepository<Document, ObjectId> {






    public void transactionWithSessionTest(){

        ClientSession clientSession = startSession();
        try {

            clientSession.startTransaction();



            Document d1 = new Document();
            d1.append("name", "事务").append("test", "事务");
            Document d2 = new Document();
            d2.append("name", "事务").append("test", "事务");


            getDatastore().withSession(clientSession).insert(d1,"test");

            getDatastore().withSession(clientSession).insert(d2,"test1");
            System.out.println(1/0);
            /**
             *提交事务
             */
            clientSession.commitTransaction();
        } catch (Exception e) {
            logger.error("异常=> 回滚事务   {}",e.getMessage());
            clientSession.abortTransaction();

        }
    }

    public void transactionWithSessionTest1(){

        ClientSession clientSession = startSession();
        try {

          /* clientSession.withTransaction(() -> {
               Document d1 = new Document();
               d1.append("name", "事务").append("test", "事务");
               Document d2 = new Document();
               d2.append("name", "事务").append("test", "事务");


               TransactionServiceDemo.this.getDatastore().withSession(clientSession).insert(d1, "test");

               TransactionServiceDemo.this.getDatastore().withSession(clientSession).insert(d2, "test1");
               System.out.println(1 / 0);
               return true;
           });*/





        } catch (Exception e) {
            logger.error("异常=> 回滚事务   {}",e.getMessage());

        }
    }


    @Transactional(rollbackFor = Exception.class)
    public void transactionTest1(){

        try {
            Document d1 = new Document();
            d1.append("name", "事务").append("test", "事务");
            Document d2 = new Document();
            d2.append("name", "事务").append("test", "事务");


            mongoTemplate.getCollection("test") .insertOne(d1);

            mongoTemplate.getCollection("test1").insertOne(d2);
            System.out.println(1/0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Transactional(rollbackFor=Exception.class)
    public void transactionTest2(){

        Document d1 = new Document();
        d1.append("name", "事务1").append("test", "事务1");
        Document d2 = new Document();
        d2.append("name", "事务1").append("test", "事务1");


        mongoTemplate.getCollection("test") .insertOne(d1);

        mongoTemplate.getCollection("test1").insertOne(d2);
        //System.out.println(1/0);
    }


}
