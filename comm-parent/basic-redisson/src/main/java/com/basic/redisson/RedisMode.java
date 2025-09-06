package com.basic.redisson;

/**
 * RedisMode <br>
 *
 * @author: lidaye <br>
 * @date: 2021/11/3  <br>
 */
public enum RedisMode {

    /**
     * Redis 链接模式
     */

    /**
     * 单机
     */
    Single("single"),

    /**
     * 集群
     */
    CLUSTER("cluster"),
    /**
     * Replicated
     */
    Replicated("replicated"),
    /**
     * Sentinel 哨兵模式
     */
    Sentinel("sentinel"),
    /**
     * Master-slave 主从复制
     */
    MASTER_SLAVE("master-slave"),
    /**
     * Proxy 代理模式
     */
    Proxy("proxy");



    private String mode;


    RedisMode(String mode){
        this.mode=mode;
    }

    public String getMode() {
        return mode;
    }
}
