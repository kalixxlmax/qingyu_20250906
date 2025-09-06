package com.basic.im.admin.dao;

import com.basic.im.entity.PayConfig;
import com.basic.im.repository.IMongoDAO;

public interface PayConfigDao extends IMongoDAO<PayConfig,Long> {

    PayConfig getPayConfig();

    void addPayConfig(PayConfig payConfig);
}
