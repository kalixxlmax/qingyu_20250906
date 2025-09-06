package com.basic.redisson;


import com.basic.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@EnableConfigurationProperties(RedissonConfig.class)
@AutoConfigureAfter(RedissonConfig.class)
public class RedissonConfiguration {


    @Autowired
    private RedissonConfig redissonConfig;

    @Bean(destroyMethod = "shutdown")
    public RedissonClient initRedissonClient() {

        log.info(" initRedissonClient mode {} address {} database {}",
                redissonConfig.getMode(),redissonConfig.getAddress(),redissonConfig.getDatabase());
    	RedissonClient redissonClient=null;
    	try {
    		Config config = new Config();
            config.setCodec(new JsonJacksonCodec());
            log.info("redisson {} start ",redissonConfig.getMode());
            /**
             * 单机模式
             */
            if(RedisMode.Single.getMode().equals(redissonConfig.getMode())) {

                SingleServerConfig serverConfig = config.useSingleServer()
                        .setAddress(redissonConfig.getAddress())
                        .setDatabase(redissonConfig.getDatabase());
                serverConfig.setKeepAlive(true);
                serverConfig.setPingConnectionInterval(redissonConfig.getPingConnectionInterval());
                serverConfig.setTimeout(redissonConfig.getTimeout());
                serverConfig.setConnectTimeout(redissonConfig.getConnectTimeout());
                serverConfig.setConnectionMinimumIdleSize(redissonConfig.getConnectionMinimumIdleSize());

                serverConfig.setConnectionPoolSize(redissonConfig.getConnectionPoolSize());

                if(!StringUtil.isEmpty(redissonConfig.getPassword())) {
                    serverConfig.setPassword(redissonConfig.getPassword());
                }

           }else if (RedisMode.CLUSTER.getMode().equals(redissonConfig.getMode())){
                /**
                 * 集群模式
                 */
                log.info("redisson Cluster start ");
                String[] nodes =redissonConfig.getAddress().split(",");
                ClusterServersConfig serverConfig = config.useClusterServers();
                serverConfig.addNodeAddress(nodes);
                serverConfig.setKeepAlive(true);
                serverConfig.setPingConnectionInterval(redissonConfig.getPingConnectionInterval());
                serverConfig.setTimeout(redissonConfig.getTimeout());
                serverConfig.setConnectTimeout(redissonConfig.getConnectTimeout());

                if(!StringUtil.isEmpty(redissonConfig.getPassword())) {
                    serverConfig.setPassword(redissonConfig.getPassword());
                }
            }else if (RedisMode.Sentinel.getMode().equals(redissonConfig.getMode())){
                /**
                 * 哨兵模式
                 */
                log.info("redisson Sentinel start ");
                String[] nodes =redissonConfig.getAddress().split(",");
                SentinelServersConfig serverConfig = config.useSentinelServers();

                serverConfig.setMasterName(redissonConfig.getMastername());
                serverConfig.addSentinelAddress(nodes);
                serverConfig.setKeepAlive(true);
                serverConfig.setPingConnectionInterval(redissonConfig.getPingConnectionInterval());
                serverConfig.setTimeout(redissonConfig.getTimeout());
                serverConfig.setConnectTimeout(redissonConfig.getConnectTimeout());

                if(!StringUtil.isEmpty(redissonConfig.getPassword())) {
                    serverConfig.setPassword(redissonConfig.getPassword());
                }
            }else if (RedisMode.MASTER_SLAVE.getMode().equals(redissonConfig.getMode())){
                /**
                 * 主从模式
                 */
                log.info("redisson Master-slave start ");
                String[] nodes =redissonConfig.getAddress().split(",");
                MasterSlaveServersConfig serverConfig = config.useMasterSlaveServers();

                serverConfig.setMasterAddress(redissonConfig.getMasterAddress());
                serverConfig.addSlaveAddress(nodes);
                serverConfig.setKeepAlive(true);
                serverConfig.setPingConnectionInterval(redissonConfig.getPingConnectionInterval());
                serverConfig.setTimeout(redissonConfig.getTimeout());
                serverConfig.setConnectTimeout(redissonConfig.getConnectTimeout());

                if(!StringUtil.isEmpty(redissonConfig.getPassword())) {
                    serverConfig.setPassword(redissonConfig.getPassword());
                }
            }else if (RedisMode.Replicated.getMode().equals(redissonConfig.getMode())){
                /**
                 * Replicated 模式
                 */
                log.info("redisson Replicated start ");
                String[] nodes =redissonConfig.getAddress().split(",");
                ReplicatedServersConfig serverConfig = config.useReplicatedServers();
                serverConfig.addNodeAddress(nodes);
                serverConfig.setKeepAlive(true);
                serverConfig.setPingConnectionInterval(redissonConfig.getPingConnectionInterval());
                serverConfig.setTimeout(redissonConfig.getTimeout());
                serverConfig.setConnectTimeout(redissonConfig.getConnectTimeout());

                if(!StringUtil.isEmpty(redissonConfig.getPassword())) {
                    serverConfig.setPassword(redissonConfig.getPassword());
                }
            }

            /*else if (RedisMode.Proxy.getMode().equals(redissonConfig.getMode())){
                *//**
                 * Proxy 代理 模式,暂不支持
                 *//*
                log.info("redisson Proxy start ");
                String[] nodes =redissonConfig.getAddress().split(",");
                ReplicatedServersConfig serverConfig = config.use();
                serverConfig.addNodeAddress(nodes);
                serverConfig.setKeepAlive(true);
                serverConfig.setPingConnectionInterval(redissonConfig.getPingConnectionInterval());
                serverConfig.setTimeout(redissonConfig.getTimeout());
                serverConfig.setConnectTimeout(redissonConfig.getConnectTimeout());

                if(!StringUtil.isEmpty(redissonConfig.getPassword())) {
                    serverConfig.setPassword(redissonConfig.getPassword());
                }
            }*/
             redissonClient= Redisson.create(config);

            log.info("redisson create end ");
		} catch (Exception e) {
			log.error(e.getMessage(),e);
		}
        
        return redissonClient; 
        
    }


}
