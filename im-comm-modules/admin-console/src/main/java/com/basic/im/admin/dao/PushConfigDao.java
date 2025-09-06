package com.basic.im.admin.dao;

import com.basic.im.entity.PushConfig;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * @ClassName PushConfigDao
 * @Author xie yuan yuang
 * @date 2020.08.03 12:23
 * @Description
 */
public interface PushConfigDao extends IMongoDAO<PushConfig, Integer> {

    List<PushConfig> getPushConfigList();

    PushConfig addPushConfig(PushConfig pushConfig);

    PushConfig getPushConfigModelDetail(int id);

    boolean deletePushConfig(int id);
}
