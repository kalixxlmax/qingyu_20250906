package com.basic.im.security.dao;

import com.basic.im.entity.Config;
import com.basic.im.repository.IMongoDAO;

/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2020/3/4
 **/
public interface ConfigsDao extends IMongoDAO<Config, Long> {

    Config getConfig();

    void addConfig(Config config);
}
