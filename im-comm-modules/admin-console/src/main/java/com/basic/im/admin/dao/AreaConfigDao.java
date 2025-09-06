package com.basic.im.admin.dao;

import com.basic.common.model.PageResult;
import com.basic.im.admin.entity.AreaConfig;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

import java.util.Map;

public interface AreaConfigDao extends IMongoDAO<AreaConfig, ObjectId> {

    void addAreaConfig(AreaConfig areaConfig);

    PageResult<AreaConfig> getAreaConfigList(String area, int pageIndex, int pageSize);

    void updateAreaConfig(ObjectId id, Map<String,Object> map);

    void deleteAreaConfig(ObjectId id);
}
