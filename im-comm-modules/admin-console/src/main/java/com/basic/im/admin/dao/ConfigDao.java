package com.basic.im.admin.dao;


import com.basic.im.entity.Config;
import com.basic.im.entity.SystemApiConfig;
import com.basic.im.repository.IMongoDAO;

public interface ConfigDao extends IMongoDAO<Config, Long> {

    Config getConfig();

    void addConfig(Config config);

    void updateConfig(int isOpenPrivacyPosition);

    void setSystemApiConfig(SystemApiConfig systemApiConfig);

    SystemApiConfig querySystemApiConfig();
}
