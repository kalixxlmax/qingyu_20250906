package com.basic.mongodb;


import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.test.context.junit4.SpringRunner;


@Slf4j

/*@RunWith(SpringRunner.class)
@SpringBootTest(classes= MongodbDemoTest.class)
@EnableAutoConfiguration(exclude = {MongoAutoConfiguration.class})
@EnableMongoRepositories
@SpringBootApplication*/
public class MongodbDemoTest {

    public static void main(String[] args) {


        try {
            System.setProperty("tomcat.util.http.parser.HttpParser.requestTargetAllow","|{}");
            System.setProperty("es.set.netty.runtime.available.processors", "false");
            System.setProperty("rocketmq.client.logLevel", "WARN");
            System.setProperty("rocketmq.broker.diskSpaceWarningLevelRatio","95");
            SpringApplication.run(MongodbDemoTest.class, args);
            //log.info("启动成功  当前版本编译时间  =====》 "+ appConfig.getBuildTime());
        } catch (Exception e) {
            System.out.println("启动报错=== "+e.getMessage());

        }

    }




    @Autowired
    private TransactionServiceDemo transactionServiceDemo;

    @Test
    public void transactionTest1(){
        log.info("事务测试开始 =============>");

        try {
            transactionServiceDemo.transactionTest1();
            transactionServiceDemo.transactionTest2();
        } catch (Exception e) {
            e.printStackTrace();
        }
        while (true){
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }


    @Test
    public void transactionWithSessionTest(){
        log.info("事务测试开始 =============>");

        try {
            //transactionServiceDemo.transactionWithSessionTest();

            transactionServiceDemo.transactionWithSessionTest1();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }





}
