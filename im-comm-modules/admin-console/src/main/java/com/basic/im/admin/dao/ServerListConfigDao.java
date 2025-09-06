package com.basic.im.admin.dao;

import com.basic.common.model.PageResult;
import com.basic.im.admin.entity.ServerListConfig;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

import java.util.Map;

public interface ServerListConfigDao extends IMongoDAO<ServerListConfig, ObjectId> {

    void addServerList(ServerListConfig serverListConfig);

    PageResult<ServerListConfig> getServerList(ObjectId id, int pageIndex, int pageSize);

    PageResult<ServerListConfig> getServerListByArea(String area);

    void updateServer(ObjectId id, Map<String, Object> map);

    void deleteServer(ObjectId id);
}
