package com.basic.delayjob;

import com.basic.delayjob.model.DelayJob;
import com.basic.delayjob.producer.DelayJobDefaultExample;
import com.basic.delayjob.producer.PushExecDelayJobService;
import com.basic.utils.DateUtil;
import com.basic.utils.SnowflakeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;


@EnableAutoConfiguration(exclude = {MongoAutoConfiguration.class})
@Configuration(proxyBeanMethods = false)
@SpringBootApplication()
public class DelayJobApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(DelayJobApplication.class, args);
    }


    @Lazy
    @Autowired
    private DelayJobDefaultExample delayJobDefaultExample;

    @Override
    public void run(String... args) throws Exception {
        long seconds = DateUtil.currentTimeSeconds();

        delayJobDefaultExample.publishDelayJob();



    }
}
