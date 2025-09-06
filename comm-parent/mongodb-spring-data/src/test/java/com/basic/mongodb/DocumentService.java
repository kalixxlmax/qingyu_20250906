package com.basic.mongodb;

import com.mongodb.*;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

@Service
public class DocumentService {

    @Autowired
    private MongoClient mongoClient;

    @Transactional(rollbackFor = Exception.class )
    public void insert(){
        ClientSession clientSession =mongoClient.startSession();
        // try {
        MongoCollection<Document> c1 = mongoClient.getDatabase("test").getCollection("test1");
        MongoCollection<Document> c2 = mongoClient.getDatabase("test").getCollection("test2");

        Document d1 = new Document();
        d1.append("name", "test2").append("age", "18");

        c1 .insertOne(clientSession,d1);

        Document d2 = new Document();
        d2.append("name", "test3").append("age", "19");
        System.out.println(1/0);
        c2.insertOne(clientSession,d2);
       /* }catch (Exception e){
            e.printStackTrace();
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
        }*/

    }

    public void insert2(){
/*
        ClientSessionOptions sessionOptions = ClientSessionOptions.builder()
                .causallyConsistent(true)
                .build();*/
/*
    Prereq: Create collections. CRUD operations in transactions must be on existing collections.
 */

        /* Step 1: Start a client session. */
        ClientSession clientSession =mongoClient.startSession();

        /* Step 2: Optional. Define options to use for the transaction. */

        TransactionOptions txnOptions = TransactionOptions.builder()
                .readPreference(ReadPreference.primary())
                .readConcern(ReadConcern.LOCAL)
                .writeConcern(WriteConcern.MAJORITY)
                .build();


    }


    //多表事务
    public void insert3(){

        ClientSession clientSession =mongoClient.startSession();

        MongoCollection<Document> c1 = mongoClient.getDatabase("test").getCollection("test1");
        MongoCollection<Document> c2 = mongoClient.getDatabase("test").getCollection("test2");
        try {
            Document d1 = new Document();
            d1.append("name", "test2").append("test", "1");
            Document d2 = new Document();
            d2.append("name", "test3").append("test", "2");

            clientSession.startTransaction();
            c1 .insertOne(clientSession,d1);
            //System.out.println(1/0);
            c2.insertOne(clientSession,d2);
            clientSession.commitTransaction();
        }catch (Exception e){
            e.printStackTrace();
            clientSession.abortTransaction();
        }finally {
            clientSession.close();
        }

    }
    //多数据库事务
    public void insert4(){

        ClientSession clientSession =mongoClient.startSession();

        MongoCollection<Document> c1 = mongoClient.getDatabase("test").getCollection("test");
        MongoCollection<Document> c2 = mongoClient.getDatabase("test1").getCollection("test");
        try {
            Document d1 = new Document();
            d1.append("name", "test2").append("test", "db1");
            Document d2 = new Document();
            d2.append("name", "test3").append("test", "db2");

            clientSession.startTransaction();
            c1 .insertOne(clientSession,d1);
            System.out.println(1/0);
            c2.insertOne(clientSession,d2);
            clientSession.commitTransaction();
        }catch (Exception e){
            e.printStackTrace();
            clientSession.abortTransaction();
        }finally {
            clientSession.close();
        }

    }


    public void update(){

        MongoCollection<Document> mongoCollection = mongoClient.getDatabase("test").getCollection("test");


        BasicDBObject q = new BasicDBObject("name", "121");
        //Document query=new Document("path", path);

        mongoCollection.find(q);


        //mongoCollection.updateOne(q, new BasicDBObject("$inc", new BasicDBObject("citations", 1)));


    }

}
