package com.basic.im.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Setter
@Getter
@Configuration
@ConfigurationProperties(prefix = "xmppconfig")
public  class XMPPConfig {
    private String host;
    private int port;
    private String serverName;
    private String username;
    private String password;

    /**
     * 数据库链接  127.0.0.1:27017,127.0.0.2:28018
     */
    private String dbUri;
    private String dbName;
    private String dbUsername;
    private String dbPassword;

    public void setXmppConfig(XMPPConfig xmppConfig){
        this.host = xmppConfig.getHost();
        this.port = xmppConfig.getPort();
        this.serverName = xmppConfig.getServerName();
        this.username = xmppConfig.getUsername();
        this.password = xmppConfig.getPassword();
        this.dbUri = xmppConfig.getDbUri();
        this.dbName = xmppConfig.getDbName();
        this.dbUsername = xmppConfig.getDbUsername();
        this.dbPassword = xmppConfig.getDbPassword();
    }
}