package com.basic.im.admin.dao;


import com.basic.im.entity.ClientConfig;
import com.basic.im.repository.IMongoDAO;

public interface ClientConfigDao extends IMongoDAO<ClientConfig, Long> {

    void addClientConfig(ClientConfig clientConfig);

    ClientConfig getClientConfig(long id);

}
